package fsarchivehandler

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorysize"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// archiveUploaderOverrideは、このpackageの試験用設定だけが実装できる構築時の依存指定です。
// 本番の設定providerは実装しないため、factoryが送信設定から組み立てたuploaderと、
// checkpoint設定から作るStoreを使います。試験でも構築後に差し替えず、宛先の登録より前に確定します。
type archiveUploaderOverride interface {
	// archiveUploaderOverrideは、指定時のuploaderと、checkpoint設定から作ったStoreを包む関数を返します。
	// Storeは本番と同じ保存先・source・宛先の登録で作り、障害注入などのために包むだけです。
	// 起動時の世代番号の決定も同じcheckpointを読みます。
	archiveUploaderOverride(failurearchive.FailureCategory) (
		uploader.Uploader,
		func(checkpointstore.Store) checkpointstore.Store,
		bool,
	)
}

// initArchiveDirectoryRuntimesはretryable/permanent writerのGuard登録を準備し、
// inline modeの場合だけ各directoryの概算Trackerも取得します。runner側Managerは
// FileName.Dirから同じGuardを取得するため、明示的なinstance注入は不要です。
func (f *FsArchiveHandler) initArchiveDirectoryRuntimes() error {
	f.retryableDirectory = archiveDirectoryRuntime{
		guard: appendlog.DirectoryGuardForPath(f.Fsstore.FileNameRetryable.Dir),
		token: appendlog.NewDirectoryGuardToken(),
	}
	f.permanentDirectory = archiveDirectoryRuntime{
		guard: appendlog.DirectoryGuardForPath(f.Fsstore.FileNamePermanent.Dir),
		token: appendlog.NewDirectoryGuardToken(),
	}
	if f.inlineLifecycleEnabled() {
		checkpointCfg := f.Config.ArchiveCheckpoint()
		override, _ := f.Config.(archiveUploaderOverride)
		for _, series := range []struct {
			category failurearchive.FailureCategory
			fileName logpattern.FileNamePattern
			state    *archiveDirectoryRuntime
			uploader *uploader.Uploader
		}{
			{failurearchive.FailureCategoryRetryable, f.Fsstore.FileNameRetryable, &f.retryableDirectory, &f.retryableArchiveUploader},
			{failurearchive.FailureCategoryPermanent, f.Fsstore.FileNamePermanent, &f.permanentDirectory, &f.permanentArchiveUploader},
		} {
			var wrapStore func(checkpointstore.Store) checkpointstore.Store
			if override != nil {
				if archiveUploader, wrap, overridden := override.archiveUploaderOverride(series.category); overridden {
					*series.uploader = archiveUploader
					wrapStore = wrap
				}
			}
			store, err := newCompressedArchiveUploadCheckpointStore(
				checkpointCfg,
				series.category,
				series.fileName,
				*series.uploader,
				f.compressedArchiveUploadCheckpointRetryErrorHandler(series.fileName.Dir),
			)
			if err != nil {
				return &FailureArchiveCheckpointError{Category: series.category, Err: err}
			}
			if wrapStore != nil {
				store = wrapStore(store)
			}
			series.state.uploadCheckpoints = store
		}
		f.retryableDirectory.directorySizeTracker = appendlog.DirectorySizeTrackerForPath(
			f.Fsstore.FileNameRetryable.Dir,
		)
		f.permanentDirectory.directorySizeTracker = appendlog.DirectorySizeTrackerForPath(
			f.Fsstore.FileNamePermanent.Dir,
		)
		// local compressed file自体がpending状態です。process再起動後も
		// directory Trackerの初回全体走査に依存せず再発見できるよう、
		// uploaderが有効な系列はtarget-only bootstrapを即時期限で予約します。
		if f.archiveUploader(failurearchive.FailureCategoryRetryable) != nil {
			f.retryableDirectory.uploadRetry.armImmediate()
		}
		if f.archiveUploader(failurearchive.FailureCategoryPermanent) != nil {
			f.permanentDirectory.uploadRetry.armImmediate()
		}
	}
	return nil
}

// 保存再試行の通知にもfactoryで設定されたOnErrorを使用します。checkpoint Storeが
// 別goroutineへ通知するため、callbackからPipeline.Shutdownへ再入できます。
func (f *FsArchiveHandler) compressedArchiveUploadCheckpointRetryErrorHandler(directory string) func(error) {
	if f.OnError == nil {
		return nil
	}
	return func(err error) {
		f.notifyCompressedArchiveUploadCheckpointError(context.Background(), directory, err)
	}
}

func (f *FsArchiveHandler) directoryForFailureCategory(failureCategory failurearchive.FailureCategory) *archiveDirectoryRuntime {
	if f == nil {
		return nil
	}
	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		return &f.retryableDirectory
	case failurearchive.FailureCategoryPermanent:
		return &f.permanentDirectory
	default:
		return nil
	}
}

func (f *FsArchiveHandler) directorySizeTracker(
	failureCategory failurearchive.FailureCategory,
) *directorysize.Tracker {
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil {
		return nil
	}
	return state.directorySizeTracker
}

// protectRotatedFileBeforeTransitionは、active登録をnew pathへ切り替える前にold raw pathと
// 生成され得るcompressed pathをretainします。これにより、runnerまたはinline purgeが
// active切替の隙間でold fileを処理・削除することを防ぎます。
// archivePathProtectionは、一回のrotation taskまたはpending-upload retryが
// DirectoryGuardへ登録した所有権です。tokenを処理単位に分けることで、同じpathを
// 別処理が保護していても、一方の完了が他方の保護を解除しません。
type archivePathProtection struct {
	token *appendlog.DirectoryGuardToken
	paths []string
	// guardPathsは実処理対象pathsに加え、同世代の処理を排他するraw共通キーを含みます。
	// 空の場合はpaths自身が保護一覧です。
	guardPaths []string
}

func (f *FsArchiveHandler) protectRotatedFileBeforeTransition(
	failureCategory failurearchive.FailureCategory,
	oldFilePath string,
) archivePathProtection {
	if oldFilePath == "" {
		return archivePathProtection{}
	}
	protection := archivePathProtection{
		paths: f.archiveProtectedPaths(oldFilePath),
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return protection
	}
	// writer tokenと分離し、一つのrotation taskが終了しても同じfailure categoryの別taskや
	// pending-upload retryが所有する同一pathの保護を解除しないようにします。
	protection.token = appendlog.NewDirectoryGuardToken()
	state.guard.ProtectPaths(protection.token, protection.paths...)
	return protection
}

func (f *FsArchiveHandler) releaseArchivePathProtection(
	failureCategory failurearchive.FailureCategory,
	protection archivePathProtection,
) {
	paths := protection.guardPaths
	if len(paths) == 0 {
		paths = protection.paths
	}
	if protection.token == nil || len(paths) == 0 {
		return
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return
	}
	state.guard.UnprotectPaths(protection.token, paths...)
}

func (f *FsArchiveHandler) unprotectArchivePaths(
	failureCategory failurearchive.FailureCategory,
	paths []string,
) {
	if len(paths) == 0 {
		return
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return
	}
	state.guard.UnprotectPaths(state.token, paths...)
}

func (f *FsArchiveHandler) protectedArchivePaths(
	failureCategory failurearchive.FailureCategory,
) map[string]struct{} {
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return nil
	}

	protected := make(map[string]struct{})
	state.guard.WithProtectedPaths(func(paths map[string]struct{}) {
		protected = make(map[string]struct{}, len(paths))
		for path := range paths {
			protected[path] = struct{}{}
		}
	})
	return protected
}

// archiveFileInfosExcludingProtectedは、同じdirectoryのwriterがopenしているactive pathと、
// inline compress/upload中またはrunner処理中のretained pathをpurge候補から除外します。
func (f *FsArchiveHandler) archiveFileInfosExcludingProtected(
	files []fsstore.FileInfo,
	failureCategory failurearchive.FailureCategory,
) []fsstore.FileInfo {
	if f == nil || len(files) == 0 {
		return files
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return files
	}

	var candidates []fsstore.FileInfo
	state.guard.WithProtectedPaths(func(protected map[string]struct{}) {
		candidates = fsstore.ExcludeFileInfosByPaths(files, protected)
	})
	return candidates
}

// removeDirectoryGuardTokensはactive fileのclose試行後にwriter所有権を解除します。
// runner modeの最終scanはこの呼出しより前に行うため、shutdown時点のactive fileを
// 新たなarchive対象にはしません。
func (f *FsArchiveHandler) removeDirectoryGuardTokens() {
	if f == nil {
		return
	}
	if f.retryableDirectory.guard != nil {
		f.retryableDirectory.guard.RemoveToken(f.retryableDirectory.token)
	}
	if f.permanentDirectory.guard != nil {
		f.permanentDirectory.guard.RemoveToken(f.permanentDirectory.token)
	}
}

func (f *FsArchiveHandler) removeDirectoryGuardToken(failureCategory failurearchive.FailureCategory) {
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return
	}
	state.guard.RemoveToken(state.token)
}
