package file

import (
	"sync/atomic"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorysize"
)

// inlinePurgePlanは、一回のinline cleanupを開始する際に確定した基準日、
// 日次age purgeの要否、directory全体の走査権を取得した結果をまとめます。
// 全判定完了後は値としてbackground処理へ渡し、変更しません。
type inlinePurgePlan struct {
	cleanupTriggeredAt        time.Time
	agePurgeReferenceDate     time.Time
	dailyAgePurgeDue          bool
	directorySizeScanAcquired bool
	seriesUploadRetryDue      bool
	// deferredRawRecoveryPathは、rotation archive taskが直前に圧縮を試みたsourceです。
	// post-rotation観測ではこの一pathだけをretainして再圧縮せず、同じ失敗通知の
	// 自己再入を防ぎます。他のrestart rawは通常どおり回復します。
	deferredRawRecoveryPath string
}

// cleanupRequiredは、このplanでfilesystem走査を起動する理由があるかを返します。
// upload retryだけの場合もtarget-only走査が必要ですが、age・generation purgeは
// fileSeriesPurgeRequiredで別に判定します。
func (p inlinePurgePlan) cleanupRequired() bool {
	return p.fileSeriesPurgeRequired() || p.seriesUploadRetryDue
}

// fileSeriesPurgeRequiredは、日次age走査、または既に取得したdirectory全体の
// snapshotを再利用してfile系列のage・generation purgeを行うかを返します。
// upload retryと全体観測が同時なら全体snapshotを使用し、upload retryだけの
// target-only走査ではpurgeを実行しません。
func (p inlinePurgePlan) fileSeriesPurgeRequired() bool {
	return p.directorySizeScanAcquired ||
		(!p.seriesUploadRetryDue && p.dailyAgePurgeDue)
}

// inlinePurgeRuntimeは、FileLoggerがinline modeでpurgeを起動・判断するための
// 状態だけを所有します。runner・disable modeではこの実体を作りません。
//
// configはpurgeとfilesystem観測の両方が使う設定です。lastAgePurgeDateは
// 保持日数purgeをcalendar dayごとに一度へ抑制するためのprocess-localな日付です。
// directory sizeのbyte数Trackerはwrite後を含むcleanup要求で、filesystem観測を
// 開始するかの概算判断にだけ使用します。mode非依存のFileLogger処理が
// inlinePurgeの存在を確認してから呼び出すため、
// この型のmethodはnil receiverを受け付けません。
type inlinePurgeRuntime struct {
	// configは、実際のpurgeとfilesystem観測の開始判定で共有する唯一の
	// config providerです。
	config appendlog.FilePurgeConfigProvider
	// lastAgePurgeDateは、このFileLoggerが担当するfile系列で保持日数purgeを最後に開始した
	// calendar dayの0時です。値のtimezoneはClockの戻り値と一致します。
	// 読み書きはcleanupRunningの取得中だけ行います。
	lastAgePurgeDate time.Time

	// cleanupRunningは、cleanupの必要性判定からbackground処理完了までtrueです。
	// trueの間に到着したcleanup要求は待機・pending化せず破棄します。
	cleanupRunning atomic.Bool
	// seriesUploadRetryは、このFileLoggerが担当するphysical file seriesの
	// upload再試行時刻だけを管理します。directory共有Trackerとは独立です。
	seriesUploadRetry fileSeriesUploadRetrySchedule
	// rawArchiveRecoveryBootstrapPendingは、このphysical file seriesについて
	// process起動後の全体走査をまだ完了していないことを表します。directory Trackerは
	// 別roleと共有するため、別roleがbaselineを確定済みでも、このseries自身の最初の
	// 自然なwriteでは一度だけ全体走査を強制します。
	rawArchiveRecoveryBootstrapPending atomic.Bool

	directorySizeTracker *directorysize.Tracker

	fileScanner purgeFileScanner
}

// initInlinePurgeRuntimeは、FileLogger自身がlifecycleを担当する場合だけruntimeを
// 構築します。同じdirectoryのlogger間で共有するbyte Trackerは、正規化path単位の
// registryからinline modeでだけ取得します。
func (l *FileLogger) initInlinePurgeRuntime() {
	if l == nil || !l.inlineLifecycleEnabled() {
		return
	}
	storeConfig := appendlog.WithResolvedLogFilePurgeDirectories(
		l.config.Fsstore(),
		l.directoryPaths[DefaultLogger],
		l.directoryPaths[ErrorLogger],
		l.directoryPaths[DebugLogger],
	)
	purgeConfig := storeConfig.Purge()
	switch l.logger {
	case ErrorLogger:
		purgeConfig = storeConfig.ErrorPurge()
	case DebugLogger:
		purgeConfig = storeConfig.DebugPurge()
	}
	runtime := newInlinePurgeRuntime(
		purgeConfig,
		appendlog.DirectorySizeTrackerForPath(l.Fsstore.FileName.Dir),
	)
	if l.config.Fsstore().Lifecycle().EnableFileCompress() {
		runtime.rawArchiveRecoveryBootstrapPending.Store(true)
	}
	// directory Trackerは同じdirectoryの別seriesと共有されるため、別seriesが
	// 先にbaselineを確定しても、upload有効seriesは自身の最初のwriteで
	// restart前から残る圧縮fileをtarget-only発見します。
	if l.ArchiveUploader != nil {
		runtime.seriesUploadRetry.scheduleImmediateRetry()
	}
	l.inlinePurge = runtime
}

func newInlinePurgeRuntime(
	config appendlog.FilePurgeConfigProvider,
	directorySizeTracker *directorysize.Tracker,
) *inlinePurgeRuntime {
	return &inlinePurgeRuntime{
		config:               config,
		directorySizeTracker: directorySizeTracker,
		fileScanner:          filesystemPurgeFileScanner{},
	}
}

// invalidateDirectorySizeEstimateは、観測外のfile単位lifecycle mutation後に
// directory使用量cacheを無効化し、次のcleanup判定で全体観測を要求します。
func (r *inlinePurgeRuntime) invalidateDirectorySizeEstimate() {
	if r == nil || r.directorySizeTracker == nil {
		return
	}
	r.directorySizeTracker.Invalidate()
}
