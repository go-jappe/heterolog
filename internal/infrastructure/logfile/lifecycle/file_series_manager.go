package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	ErrNilFileSeriesManager = errors.New("appendlog file series manager is nil")
	ErrNilConfig            = errors.New("appendlog lifecycle config is nil")
	ErrInvalidFileName      = errors.New("appendlog lifecycle file name is invalid")
	ErrInvalidLatestLog     = errors.New("appendlog lifecycle latest log cannot be resolved")
	ErrEmptyCompressedPath  = processing.ErrEmptyCompressedPath
	// ErrFileProcessingPanickedは、受理済みファイルの処理中にpanicが発生したことを表します。
	// 手動復旧ではファイルごとの失敗に記録し、通常稼働では未完了処理の再試行へ引き継ぎます。
	ErrFileProcessingPanicked = errors.New("log file processing panicked")
	// ErrInvalidUsageCacheConfigurationは、filesystem使用量の概算を再観測する間隔などの
	// 設定が不正なため、lifecycleを構築しなかったことを表します。検証エラーもwrapします。
	ErrInvalidUsageCacheConfiguration = errors.New("appendlog lifecycle usage cache configuration is invalid")
	// ErrInvalidDefaultLogPurgeConfigurationは、default系列の保持世代数・保持日数・容量上限が
	// 下限を満たさないため、lifecycleを構築しなかったことを表します。検証エラーもwrapします。
	ErrInvalidDefaultLogPurgeConfiguration = errors.New("appendlog lifecycle default log purge configuration is invalid")
	// ErrInvalidRoleLogPurgeConfigurationは、有効なerror/debug系列の保持設定が下限を
	// 満たさないため、lifecycleを構築しなかったことを表します。検証エラーもwrapします。
	ErrInvalidRoleLogPurgeConfiguration = errors.New("appendlog lifecycle role log purge configuration is invalid")
)

const defaultLogFileExtension = "log"

type FileSeriesManagerOptions struct {
	Config appendlog.ConfigProvider
	// FileNameは呼び出し元で確定した対象ログ群の固定パターンです。
	// Dir・Target・Extensionは必須で、Configからの補完は行いません。
	// Dirのリンクは構築時に解決し、以後は実体パスを使用します。
	// Prefixは接頭辞なしの場合に空を許可します。Date・Indexの指定は不要です。
	FileName logpattern.FileNamePattern
	Uploader uploader.Uploader // nil の場合は object storage upload を行わない。
	// CheckpointStoreは各fileの宛先別送信・退避完了を永続化します。
	// Uploaderを指定する場合は必須で、Uploaderは宛先別の送信結果を扱う必要があります。
	// Storeの終了責務はmanagerが持ち、最終Closeで解放します。
	CheckpointStore checkpointstore.Store
	// Clockが返す時刻とタイムゾーンを、ファイル名の日付・保持期限の判定にそのまま使います。
	Clock  clock.Clock
	Tracer observer.Tracer
	Meter  observer.Meter
	// Reporterは受理済みファイル処理の復旧結果の通知先です。構築時に固定します。
	// RunOnceの結果は戻り値で返し、この通知先には自動で渡しません。
	Reporter RunReporter
}

// FileSeriesManagerは、一つのappendlog file名patternに一致するfile系列について、
// 対象fileの列挙、圧縮、object storage upload、保持日数・世代数によるpurgeを順に
// 実行します。directory全体のMaxDirSize purgeはDirectoryLifecycleへ委譲します。
// 通常logのdefault/error/debugだけでなく、failure archiveのfile系列にも使用します。
type FileSeriesManager struct {
	// manualRecoveryは停止中専用のfactoryが公開前に固定します。
	// 最新fileも対象に含め、保持期限purgeと非同期再試行を無効にします。
	manualRecovery bool
	Config         appendlog.ConfigProvider
	Clock          clock.Clock
	// fileNameはFileSeriesManagerが担当するfile系列の固定patternです。factoryはこの値から
	// CheckpointStoreのSource IDも生成するため、構築後に変更できないよう非公開で保持します。
	fileName logpattern.FileNamePattern
	// fileNameResolutionErrは構築時の保存先解決失敗です。constructorにはerrorの
	// 戻り値がないため、検証・実行入口で返し、未解決の保存先では処理しません。
	fileNameResolutionErr error

	store appendlog.FileStore
	// lastAgePurgeAtは、このFileSeriesManagerが担当する一つのfile名patternで保持日数purgeを
	// 最後に開始した時刻です。RunOnceが一時的に作るDirectoryLifecycleではなく、
	// process内で継続して使われるFileSeriesManagerが日次状態を所有します。読み書きは同じ
	// directoryのlifecycle処理を直列化するDirectoryGuardの取得中だけ行います。
	lastAgePurgeAt time.Time
	// compressFileは圧縮のcommit結果を返します。通常はstoreへ委譲し、testでは
	// rename前失敗とrename後cleanup失敗を安定して再現するため差し替えます。
	compressFile func(context.Context, string) (string, error)
	// removeSourceAfterCompressionは、既存の圧縮ファイルを検証した後に元ログを削除します。
	// 削除待ちへの名前変更・同期・削除はappendlogへ委譲します。試験では途中の失敗を
	// 再現するために差し替えます。
	removeSourceAfterCompression func(string, string) error
	// initMuはvalidateによるdefault値とdirectory共有部品の初期化を直列化します。
	initMu sync.Mutex
	// forceDoneは明示的な強制終了だけを受理済み周期へ通知します。
	forceDone chan struct{}
	// objectStorageUploadは、このfile系列のuploader、checkpointとprocess内のupload進捗を
	// 一つの所有単位として保持します。uploadとcheckpointの両方が未設定の場合はnilです。
	// 子componentのnil receiver対応methodは、このnilをupload無効のno-opとして扱います。
	objectStorageUpload *objectStorageUploadManager
	// compressedPeerCacheMuはverifiedCompressedPeersを保護します。
	compressedPeerCacheMu sync.Mutex
	// verifiedCompressedPeersはsourceと圧縮済みfileが共存する回復経路で成功した
	// decode・内容比較の検証を、圧縮済みfileと比較したsourceのfingerprint単位で保持し、
	// 同じ大きなfileの反復decodeを避けます。どちらかのfileが変われば再検証します。
	// 失敗は保持せず、通常fileの不正なpeerは処理経路で削除して次周期にsourceから再生成します。
	// 圧縮fileの削除時に破棄し、通常走査でも不存在の記録を回収します。
	// 元ログの退避・削除だけが失敗した場合、圧縮file自体は不変なので記録を保持します。
	verifiedCompressedPeers map[string]compressedPeerFingerprint

	// directoryGuardはwriterのactive pathと、同じ正規化済みdirectoryのlifecycle排他を共有する。
	directoryGuard *appendlog.DirectoryGuard
	// directoryTokenのownerは、このFileSeriesManager instanceです。
	// managerが圧縮・upload処理へ確保したretained pathだけを識別します。
	directoryToken *appendlog.DirectoryGuardToken
	// logFileProcessingRecoveryは、未完了のファイル処理をRunOnceの終了やrunnerの再起動をまたいで管理します。
	// DirectoryLifecycleの終了時に完了を待ちます。managerを直接使う場合は、最後のRunOnce後にCloseします。
	logFileProcessingRecovery         *logfile.LogFileProcessingRecovery[logFileProcessingRecoveryKey]
	logFileProcessingRecoveryReportMu sync.RWMutex
	logFileProcessingRecoveryReport   func(RunReport, error)
	closeOnce                         sync.Once
	closeErr                          error
	closed                            bool
}

type compressedPeerFingerprint struct {
	size            int64
	modTimeUnixNano int64
	// sourceは、圧縮済みfileと内容を比較したsourceの状態です。
	source compressionSourceFingerprint
}

// compressionSourceFingerprintは、検証時のsourceの状態です。existsがfalseなら
// sourceが退避済みで比較対象がなく、圧縮済みfileの完全性だけを検証した記録です。
type compressionSourceFingerprint struct {
	exists          bool
	size            int64
	modTimeUnixNano int64
}

// RunReportは、appendlogの一回のlifecycle実行、または独立したcompressed log file
// upload failure archive保守周期の結果を呼び出し側へ渡します。
type RunReport struct {
	// DirectoryPathは、この実行が対象にした正規化済みの物理directoryです。
	// 同じHandlerが複数Runnerの結果を受け取る場合に、発生元を識別するために使います。
	DirectoryPath string
	ScannedFiles  int
	// RecoveryErrorsは手動復旧で完了できなかったfileを識別します。
	// directory共通の準備失敗は複数fileへ記録される場合があります。
	RecoveryErrors []FileRecoveryError
	// CheckpointWarningは、矛盾した保存結果をpurge対象外へ同期保全し、
	// 既存の受理済み途中経過を保持して継続した通知です。
	// DirectoryPathとこのfieldだけを設定した別のReportRunへ、err=nilで渡します。
	// 通常のRunOnceやcheckpoint保存再試行の失敗とは区別します。
	CheckpointWarning error
	// SpoolWarningは、再送・purge対象外に保全されたspoolファイルの管理者確認です。
	// DirectoryPathとこのfieldだけを設定した別のReportRunへ、err=nilで渡します。
	SpoolWarning error

	Compressed []CompressionReport
	// ObjectStorageUploadsは、この実行で送信・退避が完了したfileと宛先を記録します。
	// checkpointへの反映が失敗した場合も送信・退避の完了自体はここへ記録し、
	// errorを返してlocal fileを次周期へ残します。
	ObjectStorageUploads []ObjectStorageUploadReport
	// UploadedFileRemovalsは、必要な全object storage宛先で完了済みになった圧縮済みfileを
	// local filesystemから削除した結果です。Errがnilなら削除済み、または既に不存在です。
	// 検査・削除の失敗だけをここへ記録し、同期・Closeの失敗は実行全体のerrorへ返します。
	UploadedFileRemovals []fsstore.FileRemovalResult
	// RecoveredCompressionSourceDeletePendingPathsは、以前の圧縮処理がprocess停止や
	// filesystem操作失敗で残したsource削除待ちpathのうち、今回の実行でcleanupを
	// 完了できたpathです。空の場合は残骸を発見していないか、回復に成功していません。
	RecoveredCompressionSourceDeletePendingPaths []string
	// Purgeは通常のappendlog RunOnceで実行した保持日数・世代数・directory容量purgeの結果です。
	Purge PurgeReport
	// UploadFailureArchivePurgeは、compressed log fileのobject storage upload失敗を
	// 保存するarchiveの独立した保守周期の結果です。通常のRunOnceやPurgeには合算せず、
	// 成功・失敗とも専用のReportRun通知で渡します。nilはそのarchive purgeイベントでは
	// ないことを表し、空の結果や全分類のpurge無効でもイベント時はnon-nilです。
	UploadFailureArchivePurge *compressedlogarchive.PurgeReport
}

// FileRecoveryErrorは手動復旧の一回の試行で同期的に返されたfileごとの失敗です。
type FileRecoveryError struct {
	Path string
	Err  error
}

type CompressionReport struct {
	SourcePath     string
	CompressedPath string
}

// ObjectStorageUploadReportは、一つのfileについて今回object storageへの送信または失敗時の退避が完了した結果です。
// DestinationIDsは今回送信・退避が完了した宛先です。完了記録だけの再試行は含めません。
type ObjectStorageUploadReport struct {
	Path           string
	DestinationIDs []objectstorage.DestinationID
}

type PurgeReport struct {
	ByAge        PurgeStepReport
	ByGeneration PurgeStepReport
	ByDirSize    PurgeStepReport
}

type PurgeStepReport struct {
	Ran            bool
	RemovalResults []fsstore.FileRemovalResult
}

// RemovalOutcomeCountsは、このpurge stepで存在しないことを確認できたfile数と、
// 存在しないことを確定できなかったfile数を返します。
func (r PurgeStepReport) RemovalOutcomeCounts() (removed int, failed int) {
	for _, result := range r.RemovalResults {
		if result.Err != nil {
			failed++
			continue
		}
		removed++
	}
	return removed, failed
}

// deferredFileProcessingErrorは、一つの世代の圧縮・送信を後続の処理へ持ち越したことを表します。
// generationとpathは保護して持ち越した世代とfile、errはその原因です。
type deferredFileProcessingError struct {
	generation fileGeneration
	path       string
	err        error
}

func (e *deferredFileProcessingError) Error() string {
	return fmt.Sprintf(
		"appendlog file processing deferred: period=%q index=%d path=%q: %v",
		e.generation.Period, e.generation.Index, e.path, e.err,
	)
}

func (e *deferredFileProcessingError) Unwrap() error { return e.err }

// fileSeriesRunResultは一つのfile系列を処理した結果と、後続のdirectory-size purgeに必要な
// 保護情報をまとめます。pendingPathsはこのRunOnce内だけで使い、受理済み操作は
// logFileProcessingRecoveryが周期をまたいで保護します。directoryPurgeSafeがfalseの場合、呼び出し元は
// directory全体の削除候補を安全に確定できないか、同じ周期のfile系列purgeに失敗して
// いるためMaxDirSizeを実行しません。
type fileSeriesRunResult struct {
	report             RunReport
	pendingPaths       map[string]struct{}
	directoryPurgeSafe bool
}

// fileSeriesPurgeResultはage/generationのreportと、同じ周期でdirectory-size purgeへ
// 進めるかを保持します。file列挙または削除に失敗した場合はdirectoryPurgeSafeをfalseにし、
// 同じpathを後続stepで重ねて削除しません。
type fileSeriesPurgeResult struct {
	report             PurgeReport
	directoryPurgeSafe bool
}

// NewFileSeriesManagerは指定されたfile系列を担当する部品を作ります。
// 入力の検証はNewDirectoryLifecycleまたは実行入口で行います。
// 保存先のリンクを解決して固定しますが、ディレクトリ作成やファイルへの書き込みは行いません。
func NewFileSeriesManager(opts FileSeriesManagerOptions) *FileSeriesManager {
	tracer := observer.EnsureTracer(opts.Tracer)
	meter := observer.EnsureMeter(opts.Meter)
	clk := opts.Clock
	if clk == nil {
		clk = clock.NewUTCClock()
	}
	fileSeriesManager := &FileSeriesManager{
		Config:   opts.Config,
		Clock:    clk,
		fileName: opts.FileName,
		objectStorageUpload: newObjectStorageUploadManager(
			opts.Uploader,
			opts.CheckpointStore,
		),
		store: appendlog.FileStore{
			Config: opts.Config,
			Tracer: tracer,
			Meter:  meter,
		},
		verifiedCompressedPeers:   make(map[string]compressedPeerFingerprint),
		directoryToken:            appendlog.NewDirectoryGuardToken(),
		logFileProcessingRecovery: &logfile.LogFileProcessingRecovery[logFileProcessingRecoveryKey]{},
	}
	if reporter := opts.Reporter; reporter != nil {
		fileSeriesManager.logFileProcessingRecoveryReport = func(report RunReport, err error) {
			reporter.ReportRun(context.Background(), report, err)
		}
	}
	if opts.FileName.Dir != "" {
		resolved, err := pathidentity.ResolveDirectory(opts.FileName.Dir)
		fileSeriesManager.fileNameResolutionErr = err
		if err == nil {
			fileSeriesManager.fileName.Dir = resolved
			fileSeriesManager.directoryGuard = appendlog.DirectoryGuardForPath(resolved)
		}
	}
	fileSeriesManager.compressFile = fileSeriesManager.compressWithStore
	fileSeriesManager.removeSourceAfterCompression = appendlog.RemoveSourceAfterCompression
	return fileSeriesManager
}

// FileNamePatternは、このFileSeriesManagerが担当するfile系列の固定patternを値コピーで返します。
// 戻り値を変更してもFileSeriesManagerや、同じpatternから生成されたcheckpoint Source IDには影響しません。
func (m *FileSeriesManager) FileNamePattern() logpattern.FileNamePattern {
	return m.fileName
}

func (m *FileSeriesManager) compressWithStore(ctx context.Context, path string) (string, error) {
	return m.store.CompressOldFileAndRemoveSource(ctx, path, nil)
}

// RunOnce は、このFileSeriesManagerだけを持つDirectoryLifecycleを1周実行する。
// MaxDirSizeを含むdirectory単位の処理をFileSeriesManagerへ重複実装せず、
// 複数file系列構成と同じ経路を使います。
func (m *FileSeriesManager) RunOnce(ctx context.Context) (RunReport, error) {
	directoryLifecycle, err := NewDirectoryLifecycle(m)
	if err != nil {
		return RunReport{}, err
	}
	return directoryLifecycle.RunOnce(ctx)
}

// runFileSeriesCompressIfNeededThenUploadAndPurgeは、呼び出し元がdirectory排他を取得した状態で、
// このFileSeriesManagerが担当する一つのfile系列を処理します。未圧縮かつ圧縮が有効なfileは先に圧縮し、
// 生成された圧縮fileをobject storageへuploadします。圧縮済みまたは圧縮不要のfileは
// 再圧縮せず、そのfile自体をuploadします。upload処理後に保存期間・世代数によるpurgeを
// 行います。directory-size purgeは全file系列の処理後にDirectoryLifecycleが一度だけ実行します。
func (m *FileSeriesManager) runFileSeriesCompressIfNeededThenUploadAndPurge(ctx context.Context) (fileSeriesRunResult, error) {
	result := fileSeriesRunResult{
		pendingPaths:       make(map[string]struct{}),
		directoryPurgeSafe: true,
	}
	var errs []error
	recoveredPendingSourcePaths, pendingSourceCleanupErr :=
		m.cleanupCompressionSourceDeletePendingPaths(ctx, &result.report)
	result.report.RecoveredCompressionSourceDeletePendingPaths = append(
		result.report.RecoveredCompressionSourceDeletePendingPaths,
		recoveredPendingSourcePaths...,
	)
	if pendingSourceCleanupErr != nil {
		// pending sourceは通常のmanaged file走査に含まれません。存在を解消できない周期は、
		// 物理directory使用量を過小評価したままsize purgeへ進まないようにします。
		result.directoryPurgeSafe = false
		errs = append(errs, pendingSourceCleanupErr)
	}

	scan, err := m.scanLifecycleTargetFiles()
	if err != nil {
		result.directoryPurgeSafe = false
		errs = append(errs, err)
		return result, errors.Join(errs...)
	}
	files, observedPaths := scan.files, scan.sourceGroups.Files.Paths
	result.report.ScannedFiles = len(files)
	hasInspectionErrors := false
	for _, sourceGroup := range scan.sourceGroups.Sources {
		if sourceGroup.Err != nil {
			hasInspectionErrors = true
			result.directoryPurgeSafe = false
			break
		}
	}
	compressedPathsBySource, err := m.resolveLifecycleCompressedPaths(files, scan.sourceGroups)
	if err != nil {
		// 圧縮先を確定できない場合はcheckpoint準備・圧縮・upload・purgeへ進みません。
		result.directoryPurgeSafe = false
		return result, errors.Join(append(errs, err)...)
	}
	uploadState, err := m.prepareObjectStorageUploadRun(ctx, files, observedPaths)
	if err != nil && (m.manualRecovery || !checkpointstore.IsPendingSaveOnly(err)) {
		// checkpointを読み込めない状態では、どのfileが全宛先で完了済みかを
		// 確定できません。age/generation/sizeのどのpurgeにも進めません。
		result.directoryPurgeSafe = false
		errs = append(errs, err)
		for _, path := range files {
			m.recordRecoveryError(&result.report, path, err)
		}
		return result, errors.Join(errs...)
	}
	if err != nil {
		errs = append(errs, err)
	}
	for _, path := range files {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			result.directoryPurgeSafe = false
			return result, errors.Join(errs...)
		}
		source, _ := fsstore.CompressionSourcePath(path, m.fileName.Extension)
		sourceKey := pathidentity.ComparisonKey(source)
		compressedPath := compressedPathsBySource[sourceKey]
		if pathidentity.Contains(result.pendingPaths, path) || pathidentity.Contains(result.pendingPaths, sourceKey) {
			// 同じ周期の前段で未完了として保護したpathは重ねて処理しません。
			continue
		}
		sourceGroup, _ := scan.sourceGroups.Source(source)
		if inspectionErr := sourceGroup.Err; inspectionErr != nil {
			result.pendingPaths[sourceKey] = struct{}{}
			result.pendingPaths[pathidentity.ComparisonKey(path)] = struct{}{}
			if compressedPath != "" {
				result.pendingPaths[pathidentity.ComparisonKey(compressedPath)] = struct{}{}
			}
			errs = append(errs, inspectionErr)
			m.recordRecoveryError(&result.report, path, inspectionErr)
			continue
		}
		if generation, valid := m.parseFileGeneration(path); valid &&
			(m.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(generation, !m.hasUncompressedExtension(path))) ||
				m.logFileProcessingRecovery.Has(logFileProcessingRecoveryKey{File: fileCheckpointKey(generation, true), Compression: true}) ||
				m.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey)) {
			result.pendingPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
			continue
		}

		uncompressed := m.hasUncompressedExtension(path)
		// 元ログと今回選んだ圧縮fileだけを処理します。別の圧縮fileはこの周期の
		// upload・purgeの対象から外し、次周期に残します。
		if !uncompressed && pathidentity.ComparisonKey(path) != pathidentity.ComparisonKey(compressedPath) {
			result.pendingPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
			continue
		}

		var (
			uploadPath               string
			lifecycleTargetProtected bool
			fileProcessingErr        error
		)
		compressionSource := m.Config.Lifecycle().EnableFileCompress() && uncompressed
		// 圧縮fileだけが残る場合も、元ログのキーを使って圧縮・回復処理との競合を防ぎます。
		// 正規化キーは検索・保護登録に使い、実際のfile操作には元の表記のpathを渡します。
		retainedPaths := []string{path, sourceKey}
		if compressionSource {
			retainedPaths = append(retainedPaths, compressedPath)
		}
		// latest除外済みの旧generationについて、sourceと圧縮先を同じ短いlock内で
		// 確認・retainします。まだ存在しない圧縮先も公開前から保護します。
		// 圧縮・検証・source retirementはwriterのactive切替に使うstates lockの外で
		// 実行し、同じdirectoryの二重処理はcallerのlifecycle lockで防ぎます。
		lifecycleTargetProtected = !m.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(
			m.directoryToken, nil, retainedPaths,
		)
		if lifecycleTargetProtected {
			// 圧縮先だけが別ownerに保護されている場合も、対応sourceをこの周期の
			// purgeへ渡さず、同じgenerationの処理をまとめて次周期へ延期します。
			for _, retainedPath := range retainedPaths {
				result.pendingPaths[pathidentity.AbsoluteOrCleanPath(retainedPath)] = struct{}{}
			}
			if m.manualRecovery {
				err := fmt.Errorf("offline recovery file is still protected by another owner: %s", path)
				errs = append(errs, err)
				m.recordRecoveryError(&result.report, path, err)
			}
		}
		if !lifecycleTargetProtected {
			// 圧縮からupload完了まで保護を引き継ぎ、途中return・panicでも解除します。
			m.runWithRetainedPaths(retainedPaths, func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						if m.manualRecovery {
							// errorのpanic値は原因として保持し、呼び出し元がerrors.Is/Asで判定できるようにします。
							if recoveredErr, ok := recovered.(error); ok {
								fileProcessingErr = fmt.Errorf("%w: offline file %q: %w", ErrFileProcessingPanicked, path, recoveredErr)
							} else {
								fileProcessingErr = fmt.Errorf("%w: offline file %q: %v", ErrFileProcessingPanicked, path, recovered)
							}
							return
						}
						// Supervisorの再起動が終了要求で中止されても、受理済みの一件は
						// 保護解除前に再開処理へ渡します。panic自体は従来どおり報告します。
						m.schedulePanickedFileOperation(path, compressedPath, uploadPath, compressionSource, recovered)
						panic(recovered)
					}
				}()
				uploadPath = path
				if compressionSource {
					compression, err := processing.ProcessCompression(ctx, processing.CompressionRequest{
						SourcePath: path, CompressedPath: compressedPath,
						ExistingFileObserved:     slices.Contains(observedPaths, compressedPath),
						RemoveCorruptPeer:        true,
						PreserveMismatchedSource: true,
					}, processing.CompressionOperations{
						VerifyExisting: m.existingCompressedFileIsValid,
						Compress:       m.compressFile,
						RemoveSource:   m.removeSourceAfterCompression,
						RemoveInvalid:  m.removeInvalidCompressedPeer,
					})
					if compression.ExistingFileVerified {
						// 既存peerの元ログを片付け、送信は同じ走査の圧縮候補で進めます。
						if err != nil {
							result.directoryPurgeSafe = false
							if compression.ReadyForUpload {
								m.scheduleCompressionSourceCleanup(path, err)
								errs = append(errs, err)
								m.recordRecoveryError(&result.report, path, err)
							} else {
								result.pendingPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
								result.pendingPaths[pathidentity.AbsoluteOrCleanPath(compressedPath)] = struct{}{}
								m.scheduleCompressionOperationRecovery(path, compressedPath, err)
								fileProcessingErr = err
							}
						}
						return
					}
					if !compression.ReadyForUpload {
						result.pendingPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
						if compression.FailureStage == processing.CompressionFailureExistingFile {
							if compressedPath != "" {
								result.pendingPaths[pathidentity.AbsoluteOrCleanPath(compressedPath)] = struct{}{}
							}
							if compression.FilesMayHaveChanged && errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
								err = fmt.Errorf("invalid compressed peer was removed; source will be retried in the next lifecycle cycle: %w", err)
							}
							if compression.PreservedSourcePath != "" {
								// 完全なpeerは残し、内容の異なるsourceは削除せず管理者の確認へ残します。
								err = fmt.Errorf("source differing from its compressed peer was preserved; the compressed peer is processed in the next lifecycle cycle: %w", err)
							}
							fileProcessingErr = err
							return
						}
						recoveryPath := compression.CompressedPath
						if recoveryPath == "" {
							recoveryPath = compressedPath
						}
						m.scheduleCompressionOperationRecovery(path, recoveryPath, err)
						if compression.CompressedPath != "" {
							result.pendingPaths[pathidentity.AbsoluteOrCleanPath(compression.CompressedPath)] = struct{}{}
							result.directoryPurgeSafe = false
						}
						fileProcessingErr = fmt.Errorf("compress appendlog file %s: %w", path, err)
						return
					}
					result.report.Compressed = append(result.report.Compressed, CompressionReport{
						SourcePath: path, CompressedPath: compression.CompressedPath,
					})
					if err != nil {
						// 成果物は確定済みです。削除待ちの復旧だけを別に登録します。
						m.scheduleCompressionSourceCleanup(path, err)
						result.directoryPurgeSafe = false
						errs = append(errs, err)
						m.recordRecoveryError(&result.report, path, err)
					}
					uploadPath = compression.CompressedPath
				}

				// remote uploaderもstates lockの外で呼びます。source/圧縮先の保護は
				// このiterationを抜けるまで保持し、upload前に解除する隙間を作りません。
				generation, generationOK := m.parseFileGeneration(uploadPath)
				if uploadState != nil && !generationOK {
					result.pendingPaths[pathidentity.AbsoluteOrCleanPath(uploadPath)] = struct{}{}
					errs = append(errs, fmt.Errorf(
						"%w: resolve object storage upload generation for %s",
						ErrInvalidFileName,
						uploadPath,
					))
					return
				}

				compressedUpload := !m.hasUncompressedExtension(uploadPath)
				compressedLogFile := compressedUpload &&
					(m.objectStorageUpload.uploadEnabled() ||
						m.objectStorageUpload.tracksFiles())
				uploadResult, uploadErr := m.objectStorageUpload.uploadIfPending(
					ctx,
					uploadPath,
					generation,
					uploadState,
					compressedLogFile,
				)
				switch uploadResult.state {
				case uploadAttemptResumeUpload, uploadAttemptResumeCheckpoint:
					m.scheduleLogFileProcessingRecovery(uploadPath, generation, compressedLogFile, uploadResult.state == uploadAttemptResumeUpload, uploadErr)
				}
				if uploadErr != nil {
					result.pendingPaths[pathidentity.AbsoluteOrCleanPath(uploadPath)] = struct{}{}
					errs = append(errs, uploadErr)
					m.recordRecoveryError(&result.report, uploadPath, uploadErr)
				}
				if uploadResult.state != uploadAttemptComplete && (uploadState == nil || len(uploadState.destinations) > 0) {
					// active宛先がある未完了fileだけをupload待ちとして保護します。
					// 全inactiveのfileは通常retentionへ戻します。
					result.pendingPaths[pathidentity.AbsoluteOrCleanPath(uploadPath)] = struct{}{}
					if m.manualRecovery && uploadErr == nil {
						err := fmt.Errorf("%w: %s", ErrOfflineRecoveryAwaitsEarlierGeneration, uploadPath)
						errs = append(errs, err)
						m.recordRecoveryError(&result.report, uploadPath, err)
					}
				}
				if len(uploadResult.uploadedDestinationIDs) > 0 {
					result.report.ObjectStorageUploads = append(result.report.ObjectStorageUploads, ObjectStorageUploadReport{
						Path:           uploadPath,
						DestinationIDs: uploadResult.uploadedDestinationIDs,
					})
				}
				// 今回activeな全宛先で完了し、少なくとも一つへ送信・退避したfileを削除します。
				// checkpointの保存だけが失敗しても、受理済みのmemory状態を使って先へ進めます。
				// active宛先が空なら、過去に全成功していてもupload完了を理由に削除しません。
				// 宛先の登録前から存在した対象外のfileも、送信していないため通常のpurgeへ残します。
				if uploadErr == nil || (!m.manualRecovery && checkpointstore.IsPendingSaveOnly(uploadErr)) {
					completedPath := uploadResult.localCompressedPathToRemove
					if completedPath != "" {
						var destinations []objectstorage.DestinationID
						if uploadState != nil {
							destinations = uploadState.destinations
						}
						completion, directorySyncErr := processing.RemoveCompletedFile(ctx, processing.CompletionRequest{
							Path: completedPath, Compressed: compressedLogFile, Complete: uploadResult.delivered,
							DestinationIDs: destinations,
						}, m.removeCompressedFileAndSyncDirectory)
						if !completion.Attempted {
							if directorySyncErr != nil {
								m.recordRecoveryError(&result.report, completedPath, directorySyncErr)
								m.scheduleLogFileProcessingRecovery(completedPath, generation, true, false, directorySyncErr)
								result.pendingPaths[pathidentity.AbsoluteOrCleanPath(completedPath)] = struct{}{}
								errs = append(errs, directorySyncErr)
							}
							return
						}
						removal := completion.Removal
						result.report.UploadedFileRemovals = append(
							result.report.UploadedFileRemovals,
							removal,
						)
						if removal.Err != nil {
							m.recordRecoveryError(&result.report, completedPath, removal.Err)
							errs = append(errs, fmt.Errorf(
								"remove uploaded compressed appendlog file %s: %w",
								completedPath,
								removal.Err,
							))
							m.scheduleLogFileProcessingRecovery(completedPath, generation, true, false, removal.Err)
							result.pendingPaths[pathidentity.AbsoluteOrCleanPath(completedPath)] = struct{}{}
							return
						}
						if directorySyncErr != nil {
							m.recordRecoveryError(&result.report, completedPath, directorySyncErr)
							// 不在確認後のdirectory同期も、受理済みの後片付けとして待ちます。
							m.scheduleLogFileProcessingRecovery(completedPath, generation, true, false, directorySyncErr)
							errs = append(errs, directorySyncErr)
						}
						m.objectStorageUpload.forgetUploadedPath(completedPath)
						completedPathKey := pathidentity.ComparisonKey(completedPath)
						for pendingPath := range result.pendingPaths {
							if pathidentity.ComparisonKey(pendingPath) == completedPathKey {
								delete(result.pendingPaths, pendingPath)
							}
						}
					}
				}
			})
		}
		if fileProcessingErr != nil {
			// 失敗したfileを保護し、後続の圧縮は続けます。
			// uploadは宛先ごとに先行する未完了世代が解消するまで待ちます。
			if generation, ok := m.parseFileGeneration(path); ok {
				fileProcessingErr = &deferredFileProcessingError{
					generation: generation, path: path, err: fileProcessingErr,
				}
			} else {
				fileProcessingErr = fmt.Errorf(
					"appendlog file generation deferred at unresolved path %q: %w",
					path,
					fileProcessingErr,
				)
			}
			errs = append(errs, fileProcessingErr)
			m.recordRecoveryError(&result.report, path, fileProcessingErr)
			continue
		}
	}
	if m.manualRecovery || hasInspectionErrors {
		// 検査不能な名前が残る周期は従来どおりpurgeを行いません。
		// 個別エラーは上で通知済みなので、同じ一覧を再検査して重ねて報告しません。
		return result, errors.Join(errs...)
	}
	purgeResult, err := m.purgeFileSeriesByAgeAndGeneration(ctx, result.pendingPaths)
	result.report.Purge = purgeResult.report
	// 前段のpending source cleanupやscanで一度unsafeになった状態を、後段の
	// age/generation purge成功で上書きしません。directory-size purgeへ進めるのは、
	// lifecycle全段階が一貫してsafeだった場合だけです。
	result.directoryPurgeSafe = result.directoryPurgeSafe && purgeResult.directoryPurgeSafe
	if err != nil {
		errs = append(errs, err)
	}

	return result, errors.Join(errs...)
}

// runWithRetainedPathsはretain済みのsource・圧縮先について、states lockを保持せず
// 圧縮・uploadを実行します。正常終了・早期return・panicのいずれでも保護を解除します。
// callerはdirectoryのlifecycle lockを保持し、このmanager tokenの処理を直列化します。
// panicはrecoverせず、RunnerのSupervisorへそのまま伝播させます。
func (m *FileSeriesManager) runWithRetainedPaths(paths []string, operation func()) {
	defer m.directoryGuard.UnprotectPaths(m.directoryToken, paths...)
	operation()
}

func (m *FileSeriesManager) validate() error {
	if m == nil {
		return ErrNilFileSeriesManager
	}
	m.initMu.Lock()
	defer m.initMu.Unlock()
	if m.forceDone == nil {
		m.forceDone = make(chan struct{})
	}
	if m.Config == nil {
		return ErrNilConfig
	}
	if m.logFileProcessingRecovery == nil {
		m.logFileProcessingRecovery = &logfile.LogFileProcessingRecovery[logFileProcessingRecoveryKey]{}
	}
	// constructorでguardを取得済みでも、必須のpatternが欠けていれば
	// upload/checkpointの準備やfilesystem操作へ進ませません。
	fileName, err := m.fileNamePattern()
	if err != nil {
		return err
	}
	if m.Clock == nil {
		m.Clock = clock.NewUTCClock()
	}
	// store.ConfigはconstructorでFileSeriesManager.Configと同じ参照に初期化します。
	// 直接組み立てたFileSeriesManagerだけをここで補完し、並行するRunOnceの圧縮中に
	// 設定を書き換えません。
	if m.store.Config == nil {
		m.store.Config = m.Config
	}
	if m.compressFile == nil {
		m.compressFile = m.compressWithStore
	}
	if m.removeSourceAfterCompression == nil {
		m.removeSourceAfterCompression = appendlog.RemoveSourceAfterCompression
	}
	m.compressedPeerCacheMu.Lock()
	if m.verifiedCompressedPeers == nil {
		m.verifiedCompressedPeers = make(map[string]compressedPeerFingerprint)
	}
	m.compressedPeerCacheMu.Unlock()
	if err := m.objectStorageUpload.validate(); err != nil {
		return err
	}
	if m.directoryToken == nil {
		m.directoryToken = appendlog.NewDirectoryGuardToken()
	}
	if m.directoryGuard == nil {
		// constructorを通さず組み立てた場合も、初回だけ実体パスを確定します。
		resolved, err := pathidentity.ResolveDirectory(fileName.Dir)
		if err != nil {
			return fmt.Errorf("%w: resolve file directory: %w", ErrInvalidFileName, err)
		}
		m.fileName.Dir = resolved
		m.directoryGuard = appendlog.DirectoryGuardForPath(resolved)
	}
	return nil
}
