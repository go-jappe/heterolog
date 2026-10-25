package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	ErrNilDirectoryLifecycle                    = errors.New("appendlog directory lifecycle is nil")
	ErrDirectoryLifecycleHasNoFileSeriesManager = errors.New(
		"appendlog directory lifecycle has no file series manager",
	)
	ErrFileSeriesManagerDirectoryMismatch = errors.New(
		"appendlog file series managers use different directories",
	)
	ErrDuplicateLifecyclePattern        = errors.New("appendlog lifecycle file pattern is duplicated")
	ErrDirectoryLifecycleConfigMismatch = errors.New(
		"appendlog directory lifecycle configuration differs between file series managers",
	)
)

// runnerが扱うlifecycleとは　logデータの圧縮・upload・purge をスコープとします。
// FileLoggerが作成するファイルシステムに保存されるログのディレクトリ毎に作成されます。
// そのほかarchiveで扱う .jsonlの２つディレクトリ(retryable  permanent)それぞれディレクトリごとに作成されます。

// DirectoryLifecycleは、同じ物理directoryを使用するfile系列の処理を
// 順番に実行し、その後でdirectory全体のMaxDirSize purgeを一度だけ実行します。
// file系列ごとのRunnerやownerを作らないため、特定系列の生成順には依存しません。
type DirectoryLifecycle struct {
	// fileSeriesManagersは、同じdirectoryに属するfile名patternごとのlifecycle処理です。
	// 各FileSeriesManagerはcompress、upload、age、generationを担当し、MaxDirSizeは担当しません。
	fileSeriesManagers []*FileSeriesManager
	// checkpointSaveRetryEnabledは手動再試行のfactoryだけが設定します。
	// 保存待ちの完了記録をLoadしてファイル削除へ進まないよう、RecoverOnce開始前に確認します。
	checkpointSaveRetryEnabled bool

	// initMuはfile系列構成のvalidationと、directory共通依存の初期化を直列化します。
	initMu sync.Mutex
	// configは全file系列で一致を確認したdirectory-size/compress設定の参照元です。
	config appendlog.ConfigProvider

	// directoryGuardはwriterのactive pathと同じdirectoryのlifecycle排他を共有します。
	directoryGuard *appendlog.DirectoryGuard

	// objectStorageCompressedLogFileRouteOwnersは、このlifecycle内のfile series用にfactoryが
	// 取得したcompressed log file upload failure store登録です。Supervisorが周期loopを再起動しても同じ
	// lifecycleが使われるため保持し続け、外側の最終shutdownからだけ解放します。
	// 構築後の一覧は変更せず、Close後も通知完了の待機と強制終了の伝達に使います。
	objectStorageCompressedLogFileUploadFailureRouteOwners []ObjectStorageUploadFailureRouteOwner
	closeCompressedLogRoutesMu                             sync.Mutex
	closeCompressedLogRoutesErr                            error
	closeCompressedLogRoutesDone                           bool
}

// NewDirectoryLifecycleは、同じdirectoryを管理するFileSeriesManager群を一つの実行単位へまとめます。
// 異なるdirectory、重複するfile名pattern、異なるdirectory共通設定の混在は拒否します。
// 検証に失敗した場合、渡されたmanagerの終了責務は呼び出し元に残ります。
func NewDirectoryLifecycle(fileSeriesManagers ...*FileSeriesManager) (*DirectoryLifecycle, error) {
	lifecycle := &DirectoryLifecycle{
		fileSeriesManagers: append([]*FileSeriesManager(nil), fileSeriesManagers...),
	}
	if err := lifecycle.validate(); err != nil {
		return nil, err
	}
	return lifecycle, nil
}

// CloseObjectStorageCompressedLogFileUploadFailureRoutesは、各系列のログファイルcheckpoint Storeと
// factoryが取得したupload failure store登録を解放します。RunまたはSupervisorの一時終了からは呼ばず、外側のownerが
// runner goroutineの終了を待った後に呼びます。全ownerの終了を一度だけ行い、
// 繰り返し呼んでも保存済みの終了結果を返します。
func (d *DirectoryLifecycle) CloseObjectStorageCompressedLogFileUploadFailureRoutes() error {
	if d == nil {
		return ErrNilDirectoryLifecycle
	}
	d.closeCompressedLogRoutesMu.Lock()
	defer d.closeCompressedLogRoutesMu.Unlock()
	if d.closeCompressedLogRoutesDone {
		return d.closeCompressedLogRoutesErr
	}
	d.closeCompressedLogRoutesErr = errors.Join(d.closeCompressedLogRoutesErr, d.CloseLogFileProcessingRecovery())
	for _, manager := range d.fileSeriesManagers {
		d.closeCompressedLogRoutesErr = errors.Join(d.closeCompressedLogRoutesErr, manager.Close())
	}
	for _, owner := range d.objectStorageCompressedLogFileUploadFailureRouteOwners {
		if owner == nil {
			continue
		}
		d.closeCompressedLogRoutesErr = errors.Join(d.closeCompressedLogRoutesErr, owner.Close())
	}
	d.closeCompressedLogRoutesDone = true
	return d.closeCompressedLogRoutesErr
}

// CloseLogFileProcessingRecoveryは各managerの受理済み再開処理の完了を待ちます。
// 所有者は通常runnerと受理済み呼び出しの完了後に実行します。
// durable退避先の登録とcheckpoint Storeは、最後のCloseまで利用可能に保ちます。
func (d *DirectoryLifecycle) CloseLogFileProcessingRecovery() error {
	if d == nil {
		return ErrNilDirectoryLifecycle
	}
	var errs []error
	for _, manager := range d.fileSeriesManagers {
		errs = append(errs, manager.CloseLogFileProcessingRecovery())
	}
	return errors.Join(errs...)
}

// ForceCloseは全系列へ中止を通知します。routeの解放は進行中の処理が戻るまで待ちます。
func (d *DirectoryLifecycle) ForceClose() error {
	if d == nil {
		return ErrNilDirectoryLifecycle
	}
	var errs []error
	for _, manager := range d.fileSeriesManagers {
		errs = append(errs, manager.ForceClose())
	}
	for _, owner := range d.objectStorageCompressedLogFileUploadFailureRouteOwners {
		if owner != nil {
			errs = append(errs, owner.RequestForceClose())
		}
	}
	return errors.Join(errs...)
}

// NotificationsDoneは、復旧・checkpoint・圧縮ファイルの退避先が受理した通知の完了を返します。
// 所有者は最後のClose後に待ち、後段dispatcherの受付と内部ログの出力先をその後で閉じます。
func (d *DirectoryLifecycle) NotificationsDone() <-chan struct{} {
	done := make(chan struct{})
	var upstream []<-chan struct{}
	if d != nil {
		for _, owner := range d.objectStorageCompressedLogFileUploadFailureRouteOwners {
			if owner != nil {
				upstream = append(upstream, owner.NotificationsDone())
			}
		}
		for _, manager := range d.fileSeriesManagers {
			if manager == nil {
				continue
			}
			upstream = append(upstream, manager.NotificationsDone())
		}
	}
	go func() {
		for _, notifications := range upstream {
			<-notifications
		}
		close(done)
	}()
	return done
}

// ownObjectStorageCompressedLogFileUploadFailureRoutesは、factoryの構築が全て成功した後にroute ownershipを
// lifecycleへ移します。構築途中の失敗時はfactory自身が取得済み登録をrollbackします。
func (d *DirectoryLifecycle) ownObjectStorageCompressedLogFileUploadFailureRoutes(
	owners ...ObjectStorageUploadFailureRouteOwner) {
	for _, owner := range owners {
		if owner != nil {
			d.objectStorageCompressedLogFileUploadFailureRouteOwners = append(
				d.objectStorageCompressedLogFileUploadFailureRouteOwners,
				owner,
			)
		}
	}
}

// RunOnceは全file系列のcompress/upload/age/generationを実行した後、MaxDirSizeを一度だけ判定します。
// file系列の列挙に失敗して保護対象を確定できない場合、directory-size purgeは安全のため実行しません。
func (d *DirectoryLifecycle) RunOnce(ctx context.Context) (RunReport, error) {
	var report RunReport
	if err := d.validate(); err != nil {
		return report, err
	}
	// 通常のrunner停止は親で切り離しますが、明示的な強制終了は周期にも伝えます。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, manager := range d.fileSeriesManagers {
		manager.initMu.Lock()
		forceDone := manager.forceDone
		manager.initMu.Unlock()
		select {
		case <-forceDone:
			cancel()
		default:
			go func() {
				select {
				case <-forceDone:
					cancel()
				case <-ctx.Done():
				}
			}()
		}
	}
	directoryPath, err := d.DirectoryPath()
	if err != nil {
		return report, err
	}
	report.DirectoryPath = directoryPath
	if err := ctx.Err(); err != nil {
		return report, err
	}

	err = d.directoryGuard.RunWithLifecycleLockContext(ctx, func() error {
		var runErr error
		runReport, runErr := d.runFileCompressionUploadAndPurgeWithinDirectory(ctx)
		runReport.DirectoryPath = directoryPath
		report = runReport
		return runErr
	})
	return report, err
}

// ReconcileObjectStorageUploadDestinationsは起動前にfileの走査、成功記録の回収、現在の宛先の確認を行います。
// 新しい宛先の登録時に存在するfileを除外対象として永続化します。upload・圧縮・purgeは行いません。
func (d *DirectoryLifecycle) ReconcileObjectStorageUploadDestinations(
	ctx context.Context,
) (RunReport, error) {
	var report RunReport
	if err := d.validate(); err != nil {
		return report, err
	}
	directoryPath, err := d.DirectoryPath()
	if err != nil {
		return report, err
	}
	report.DirectoryPath = directoryPath
	if err := ctx.Err(); err != nil {
		return report, err
	}

	err = d.directoryGuard.RunWithLifecycleLockContext(ctx, func() error {
		var errs []error
		for _, fileSeriesManager := range d.fileSeriesManagers {
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			scan, err := fileSeriesManager.scanLifecycleTargetFiles()
			if err != nil {
				errs = append(errs, err)
				continue
			}
			report.ScannedFiles += len(scan.files)
			for _, sourceGroup := range scan.sourceGroups.Sources {
				if sourceGroup.Err != nil {
					errs = append(errs, sourceGroup.Err)
				}
			}
			_, err = fileSeriesManager.prepareObjectStorageUploadRun(ctx, scan.files, scan.sourceGroups.Files.Paths)
			if err != nil {
				errs = append(errs, err)
				continue
			}
		}
		return errors.Join(errs...)
	})
	return report, err
}

// DirectoryPathは、このlifecycleが管理する正規化済みの物理directoryを返します。
func (d *DirectoryLifecycle) DirectoryPath() (string, error) {
	if err := d.validate(); err != nil {
		return "", err
	}
	pattern, err := d.fileSeriesManagers[0].fileNamePattern()
	if err != nil {
		return "", err
	}
	return pathidentity.AbsoluteOrCleanPath(pattern.Dir), nil
}

// runFileCompressionUploadAndPurgeWithinDirectoryは、RunOnceがDirectoryGuard.RunWithLifecycleLockを
// 取得し、同じ物理directoryを対象とする別のcleanup・圧縮・runner処理との同時実行を
// 防いでいる間に呼び出します。各file系列の対象fileは、圧縮が必要なら先に圧縮し、その圧縮結果を
// object storageへuploadします。圧縮済みまたは圧縮不要のfileは、そのfile自体をuploadします。
// 各file系列のupload処理後に保存期間/世代数purgeを行い、全file系列の処理後に物理directory全体の
// MaxDirSize purgeを一度だけ実行します。directory自体を圧縮する処理ではありません。
func (d *DirectoryLifecycle) runFileCompressionUploadAndPurgeWithinDirectory(ctx context.Context) (RunReport, error) {
	var report RunReport
	var errs []error
	pendingPaths := make(map[string]struct{})
	directoryPurgeSafe := true

	for _, fileSeriesManager := range d.fileSeriesManagers {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			return report, errors.Join(errs...)
		}

		fileSeriesResult, err := fileSeriesManager.runFileSeriesCompressIfNeededThenUploadAndPurge(ctx)
		mergeRunReport(&report, fileSeriesResult.report)
		for path := range fileSeriesResult.pendingPaths {
			pendingPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
		}
		if !fileSeriesResult.directoryPurgeSafe {
			directoryPurgeSafe = false
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
		return report, errors.Join(errs...)
	}
	if d.fileSeriesManagers[0].manualRecovery || d.config.Purge().MaxDirSize() <= 0 || !directoryPurgeSafe {
		return report, errors.Join(errs...)
	}

	directorySizeReport, err := d.runDirectorySizePurge(ctx, pendingPaths)
	report.Purge.ByDirSize = directorySizeReport
	if err != nil {
		errs = append(errs, err)
	}
	return report, errors.Join(errs...)
}

func (d *DirectoryLifecycle) validate() error {
	if d == nil {
		return ErrNilDirectoryLifecycle
	}

	d.initMu.Lock()
	defer d.initMu.Unlock()

	if len(d.fileSeriesManagers) == 0 {
		return ErrDirectoryLifecycleHasNoFileSeriesManager
	}

	patterns := make(map[string]struct{}, len(d.fileSeriesManagers))
	var (
		directory         string
		maxDirSize        uint64
		compressExtension string
	)
	for index, fileSeriesManager := range d.fileSeriesManagers {
		if fileSeriesManager == nil {
			return ErrNilFileSeriesManager
		}
		if err := fileSeriesManager.validate(); err != nil {
			return err
		}
		pattern, err := fileSeriesManager.fileNamePattern()
		if err != nil {
			return err
		}

		fileSeriesDirectory := pathidentity.AbsoluteOrCleanPath(pattern.Dir)
		fileSeriesMaxDirSize := fileSeriesManager.Config.Purge().MaxDirSize()
		fileSeriesCompressExtension := fileSeriesManager.store.GetCompressFormat().Extension()
		if index == 0 {
			directory = fileSeriesDirectory
			maxDirSize = fileSeriesMaxDirSize
			compressExtension = fileSeriesCompressExtension
			// 構成の検証は毎回行いますが、共有依存は初回だけ確定します。
			// 並行するRunOnce/ReconcileはinitMu外でこれらを参照するため、
			// 同じ値でも再代入すると実行中の参照と競合します。
			if d.config == nil {
				d.config = fileSeriesManager.Config
			}
			if d.directoryGuard == nil {
				d.directoryGuard = fileSeriesManager.directoryGuard
			}
		} else {
			if fileSeriesManager.manualRecovery != d.fileSeriesManagers[0].manualRecovery {
				return ErrDirectoryLifecycleConfigMismatch
			}
			if pathidentity.ComparisonKey(fileSeriesDirectory) != pathidentity.ComparisonKey(directory) {
				return fmt.Errorf(
					"%w: %q and %q",
					ErrFileSeriesManagerDirectoryMismatch,
					directory,
					fileSeriesDirectory,
				)
			}
			if fileSeriesMaxDirSize != maxDirSize || fileSeriesCompressExtension != compressExtension {
				return ErrDirectoryLifecycleConfigMismatch
			}
		}

		// 実file名のprefix+targetと拡張子で系列を比較します。Windowsではcaseだけ
		// 異なる系列も同じfileを書き得るため拒否し、元のpattern自体は変更しません。
		patternIdentity := pathidentity.ComparisonKey(filepath.Join(
			pattern.Dir,
			pattern.Prefix+pattern.Target+"."+pattern.Extension,
		))
		if _, duplicated := patterns[patternIdentity]; duplicated {
			return fmt.Errorf(
				"%w: directory=%q prefix=%q target=%q extension=%q",
				ErrDuplicateLifecyclePattern,
				directory,
				pattern.Prefix,
				pattern.Target,
				pattern.Extension,
			)
		}
		patterns[patternIdentity] = struct{}{}
	}
	return nil
}

// runDirectorySizePurgeは、Guardの保護対象に系列ごとの削除禁止パスを加え、容量による削除を行います。
// 追加の削除禁止パスはdirectorySizePurgeInputから受け取り、prepareProtectionから返します。
// RunPurgeWithProtectionが、その追加保護を今回のPurgeだけに設定します。
// 呼び出し元は、このdirectoryのlifecycle lockを保持してください。
func (d *DirectoryLifecycle) runDirectorySizePurge(
	ctx context.Context,
	pendingPaths map[string]struct{},
) (PurgeStepReport, error) {
	fileInfo, lifecycleProtectedPaths, err := d.directorySizePurgeInput(pendingPaths)
	if err != nil {
		return PurgeStepReport{}, err
	}

	maxDirectoryBytes := d.config.Purge().MaxDirSize()
	if !fsstore.ShouldPurgeByDirectorySize(fileInfo, maxDirectoryBytes) {
		return PurgeStepReport{}, nil
	}
	var (
		ran            bool
		removalResults []fsstore.FileRemovalResult
	)
	d.directoryGuard.RunPurgeWithProtection(fileInfo, func(map[string]struct{}) map[string]struct{} {
		return lifecycleProtectedPaths
	}, func(purge *appendlog.Purge) {
		_, span := d.fileSeriesManagers[0].store.Tracer.StartSpan(
			ctx,
			appendlog.SpanFilePurgeByDirSize,
		)
		defer span.End()
		// scope開始時のdirectory全体の観測値で容量を判定し、activeや
		// 保持中のfileを使用量から落としません。
		purgeResult := purge.PurgeByDirectorySize(maxDirectoryBytes)
		for _, manager := range d.fileSeriesManagers {
			manager.forgetRemovedCompressedPeers(purgeResult.RemovalResults...)
		}
		ran = purgeResult.Ran
		removalResults = purgeResult.RemovalResults
	})
	report := PurgeStepReport{Ran: ran, RemovalResults: removalResults}
	purgeErr := fsstore.JoinFileRemovalFailures(removalResults)
	if purgeErr == nil {
		return report, nil
	}
	return report, fmt.Errorf("%w: %w", ErrPurgeByDirectorySize, purgeErr)
}

// directorySizePurgeInputは、容量計算用の全log file一覧と、追加の削除禁止パスを返します。
// 各file系列のlatest、未圧縮、未upload、今回の処理失敗pathに加え、現在のfile系列では
// checkpoint完了を証明できない旧file系列・未知patternのfileを削除禁止パスに加えます。
// ファイル一覧からは取り除かず、呼び出し元がprepareProtectionから返して今回のPurgeでの削除を禁止します。
// Guardに登録された保護対象は、RunPurgeWithProtectionが別途Purgeへ設定します。
// error/debug分離を無効にした後も同じdirectoryに残る旧fileは、この旧file系列に該当します。
// protected pathも容量には含めるため、MaxDirSizeは物理directoryの実使用量に対して判定されます。
// 削除可能なfileだけで超過を解消できなければ、運用者が旧fileを整理するまで超過は残ります。
// 分離error fileが別directoryにある場合、そのdirectory自体が現在の実行対象から外れるため、
// この容量計算にも含めません。
func (d *DirectoryLifecycle) directorySizePurgeInput(
	pendingPaths map[string]struct{},
) ([]fsstore.FileInfo, map[string]struct{}, error) {
	type fileSeriesPattern struct {
		fileSeriesManager *FileSeriesManager
		fileName          logpattern.FileNamePattern
	}

	fileSeries := make([]fileSeriesPattern, 0, len(d.fileSeriesManagers))
	protectedPaths := make(map[string]struct{}, len(d.fileSeriesManagers)*2+len(pendingPaths))
	for path := range pendingPaths {
		protectedPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
	}

	var base logpattern.FileNamePattern
	for index, fileSeriesManager := range d.fileSeriesManagers {
		pattern, err := fileSeriesManager.fileNamePattern()
		if err != nil {
			return nil, nil, err
		}
		if index == 0 {
			base = pattern
		}
		fileSeries = append(fileSeries, fileSeriesPattern{
			fileSeriesManager: fileSeriesManager,
			fileName:          pattern,
		})
	}

	matches, err := logpattern.GlobFileNames(
		base,
		logpattern.GlobAllFiles,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("glob appendlog directory for size purge: %w", err)
	}
	pendingCompressionSources, err := logpattern.FindCompressionSourceDeletePendingPaths(
		base,
		logpattern.GlobAllFiles,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"glob appendlog compression source delete-pending files for size purge: %w",
			err,
		)
	}
	// pending sourceはgeneration purgeへ混ぜませんが、物理directoryの使用量には
	// 含めます。現在のconfigから外れた旧file系列にはcleanup ownerが存在しないため、
	// directory-wide scanで列挙し、削除候補から保護したまま容量だけを計上します。
	for _, path := range pendingCompressionSources {
		protectedPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
	}
	matches = append(matches, pendingCompressionSources...)

	// directory snapshotの後で全file系列のlatestを解決します。snapshot中にactiveだったfileが
	// rotation後にinactiveになった場合はclose済みで、新activeはsnapshot外となります。
	for _, series := range fileSeries {
		latest, err := series.fileSeriesManager.latestFileNames(series.fileName)
		if err != nil {
			return nil, nil, err
		}
		for path := range latest {
			protectedPaths[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
		}
	}

	for _, path := range matches {
		path = pathidentity.AbsoluteOrCleanPath(path)
		matchedFileSeries := false
		for _, series := range fileSeries {
			if !matchesLifecyclePattern(path, series.fileName) {
				continue
			}
			matchedFileSeries = true
			if pathidentity.Contains(protectedPaths, path) {
				break
			}
			if series.fileSeriesManager.shouldProtectFromLifecyclePurge(path) {
				protectedPaths[path] = struct{}{}
			}
			break
		}
		if !matchedFileSeries {
			// GlobAllFilesは、分離無効化後に残るerror_/debug_ fileや同じ拡張子の
			// 未知fileもdirectory容量へ含めます。現在のFileSeriesManagerがcheckpoint完了を
			// 証明できないfileは削除候補にせず、分離を再び有効にするか、運用者が
			// 明示的に処理するまで保持します。
			protectedPaths[path] = struct{}{}
		}
	}
	return fsstore.GetFilesInfo(matches), protectedPaths, nil
}

func matchesLifecyclePattern(path string, pattern logpattern.FileNamePattern) bool {
	_, canonical := logpattern.ParseCanonicalGenerationFilePath(path, pattern)
	return canonical
}

func mergeRunReport(destination *RunReport, source RunReport) {
	destination.RecoveryErrors = append(destination.RecoveryErrors, source.RecoveryErrors...)
	destination.ScannedFiles += source.ScannedFiles
	destination.Compressed = append(destination.Compressed, source.Compressed...)
	destination.ObjectStorageUploads = append(destination.ObjectStorageUploads, source.ObjectStorageUploads...)
	destination.UploadedFileRemovals = append(
		destination.UploadedFileRemovals,
		source.UploadedFileRemovals...,
	)
	destination.RecoveredCompressionSourceDeletePendingPaths = append(
		destination.RecoveredCompressionSourceDeletePendingPaths,
		source.RecoveredCompressionSourceDeletePendingPaths...,
	)
	mergePurgeStepReport(&destination.Purge.ByAge, source.Purge.ByAge)
	mergePurgeStepReport(&destination.Purge.ByGeneration, source.Purge.ByGeneration)
	mergePurgeStepReport(&destination.Purge.ByDirSize, source.Purge.ByDirSize)
}

func mergePurgeStepReport(destination *PurgeStepReport, source PurgeStepReport) {
	destination.Ran = destination.Ran || source.Ran
	destination.RemovalResults = append(destination.RemovalResults, source.RemovalResults...)
}
