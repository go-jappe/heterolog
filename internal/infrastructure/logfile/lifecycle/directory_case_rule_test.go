package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	spools3 "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/uploader/s3"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// このfileの試験は、保存先directoryの大小文字の規則（pathidentity.DirectoryNameCase）に従う
// lifecycleの比較を、規則を差し替えて両方の規則で確認します。Windowsでは
// path_identity_windows_test.goとtarget_case_restart_windows_test.goが実際のfilesystemで
// 同じ要求を確認し、ここではLinux・macOSを含む全OSで同じ判定を確認します。
// Linuxで実際に大小文字を区別しないfilesystemの上では、CIのtest (ubuntu-latest, case-insensitive TMPDIR)が
// ext4のcasefoldの一時directoryで全試験を実行します。規則の差し替えはprocess全体に適用されるため、
// t.Parallelを使いません。
// 大小文字だけが異なる別名のfileやdirectoryは作成せず、名前の比較だけを別名で行います。

func directoryCaseRuleName(sensitive bool) string {
	if sensitive {
		return "case-sensitive"
	}
	return "case-insensitive"
}

// pathの大小文字だけが異なる別名を、規則に従って同じfileとして扱うかを確認します。
// 送信の完了は世代のcheckpointで共有するため規則に依存しませんが、path単位の送信済みcacheと
// 保護の判定は、区別しない規則でだけ別名を同じfileとして扱います。
func TestObjectStorageUploadManagerPathAliasesFollowDirectoryCaseRule(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		t.Run(directoryCaseRuleName(sensitive), func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			testutil.UseDirectoryCaseRuleForTest(t, root, sensitive)
			path := filepath.Join(root, "App_20260911_1.log.gz")
			alias := filepath.Join(root, "APP_20260911_1.LOG.GZ")
			recorder := &objectStorageSingleUploadRecorder{}
			manager := newObjectStorageUploadManager(newTestDestinationUploader(t, recorder), newTestFileCheckpointStore(t))
			state, err := manager.prepareRun(ctx)
			require.NoError(t, err)
			generation := fileGeneration{Period: "20260911", Index: 1}
			result, err := manager.uploadIfPending(ctx, path, generation, state, true)
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{"primary"}, result.uploadedDestinationIDs)
			result, err = manager.uploadIfPending(ctx, alias, generation, state, true)
			require.NoError(t, err)
			require.Empty(t, result.uploadedDestinationIDs, "the generation completion is shared under either rule")
			require.Equal(t, []string{path}, recorder.pathsSnapshot())

			cached := func(candidate string) bool {
				return slices.Contains(manager.uploadedPathDestinationIDs(candidate), objectstorage.DestinationID("primary"))
			}
			manager.recordUploadedPathDestinations(path, []objectstorage.DestinationID{"primary"})
			require.Equal(t, !sensitive, cached(alias))
			// 現存pathの一覧に別名だけがあっても、区別しない規則では同じfileのcacheを残します。
			fileManager := &FileSeriesManager{objectStorageUpload: manager}
			fileManager.prunePathCaches([]string{alias})
			require.Equal(t, !sensitive, cached(path))
			manager.recordUploadedPathDestinations(path, []objectstorage.DestinationID{"primary"})
			manager.forgetUploadedPath(alias)
			require.Equal(t, sensitive, cached(path))
			require.False(t, manager.uploadPending(generation, true, true), "forgetting a path cache must preserve checkpoint completion")
		})
	}
}

// 別の所有者が大小文字だけ異なる別名で保護したfileは、区別しない規則では同じ実体として保護を取得しません。
func TestDirectoryGuardOtherOwnerAliasFollowsDirectoryCaseRule(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		t.Run(directoryCaseRuleName(sensitive), func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			testutil.UseDirectoryCaseRuleForTest(t, root, sensitive)
			path := filepath.Join(root, "App_20260911_1.log.gz")
			alias := filepath.Join(root, "APP_20260911_1.LOG.GZ")
			guard := appendlog.DirectoryGuardForPath(root)
			otherOwner := appendlog.NewDirectoryGuardToken()
			guard.ProtectPaths(otherOwner, alias)
			t.Cleanup(func() { guard.UnprotectPaths(otherOwner, alias) })
			owner := appendlog.NewDirectoryGuardToken()
			acquired := guard.ProtectPathsIfNoOtherOwnerConflict(owner, nil, []string{path})
			if acquired {
				t.Cleanup(func() { guard.UnprotectPaths(owner, path) })
			}
			require.Equal(t, sensitive, acquired)
		})
	}
}

// 大小文字だけが異なるprefixのfile系列は、区別しない規則では同じdirectoryの同じ系列です。
func TestDirectoryLifecycleCaseAliasedFileSeriesFollowDirectoryCaseRule(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		t.Run(directoryCaseRuleName(sensitive), func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			testutil.UseDirectoryCaseRuleForTest(t, root, sensitive)
			cfg := testFileSeriesManagerConfig(root, "app", 0, 0, 0, false)
			directory := filepath.Join(root, "app", "logs")
			managers := make([]*FileSeriesManager, 0, 2)
			for _, prefix := range []string{"Err_", "err_"} {
				managers = append(managers, NewFileSeriesManager(FileSeriesManagerOptions{
					Config:   cfg,
					FileName: logpattern.FileNamePattern{Dir: directory, Prefix: prefix, Target: "app", Extension: "log"},
				}))
			}
			lifecycle, err := NewDirectoryLifecycle(managers...)
			if !sensitive {
				require.ErrorIs(t, err, ErrDuplicateLifecyclePattern)
				return
			}
			require.NoError(t, err)
			closeDirectoryLifecycleOnCleanup(t, lifecycle)
		})
	}
}

// target名の表記を変えて再起動しても、区別しない規則のdirectoryでは旧表記の圧縮fileを同じ系列として
// 送信・削除し、旧表記の最新rawは送信・圧縮しません。過去の期間の最新rawを置き、現在の期間からの
// 推測だけでは保護できない状態にします。区別する規則では旧表記のfileは別系列で、操作しません。
func TestFileSeriesManagerOldTargetSpellingFollowsDirectoryCaseRule(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		t.Run(directoryCaseRuleName(sensitive), func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			testutil.UseDirectoryCaseRuleForTest(t, root, sensitive)
			directory := filepath.Join(root, "app", "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			period := logDateDaysBefore(1)
			compressedPath := filepath.Join(directory, "App_"+period+"_1.log.gz")
			rawPath := filepath.Join(directory, "App_"+period+"_2.log")
			writeGzipFile(t, compressedPath, []byte("archive written with the old target spelling"))
			originalRaw := []byte("latest raw must not be uploaded, compressed, or purged")
			require.NoError(t, os.WriteFile(rawPath, originalRaw, 0o600))
			remote := newDestinationUploaderRecorder("primary")
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: "app", Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, "app", 10, 0, 0, true), Uploader: remote,
			})
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)

			report, err := manager.RunOnce(ctx)
			require.NoError(t, err)
			require.Empty(t, report.Compressed)
			if sensitive {
				require.Empty(t, remote.pathsFor("primary"))
				require.Empty(t, report.UploadedFileRemovals)
				require.FileExists(t, compressedPath)
			} else {
				require.Equal(t, []string{compressedPath}, remote.pathsFor("primary"))
				require.Len(t, report.UploadedFileRemovals, 1)
				require.NoFileExists(t, compressedPath)
			}
			got, err := os.ReadFile(rawPath)
			require.NoError(t, err)
			require.Equal(t, originalRaw, got)
			require.NoFileExists(t, rawPath+".gz")
		})
	}
}

// 旧表記のtarget名で保存された圧縮ログspoolのfileは、区別しない規則では同じ系列の送信経路へ
// 一致し、再送されます。区別する規則では一致する経路がないため、送信せずerrorにします。
func TestCompressedLogSpoolRouteOldTargetSpellingFollowsDirectoryCaseRule(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		t.Run(directoryCaseRuleName(sensitive), func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			testutil.UseDirectoryCaseRuleForTest(t, root, sensitive)
			sourceDirectory := filepath.Join(root, "logs")
			require.NoError(t, os.MkdirAll(sourceDirectory, 0o750))
			source := filepath.Join(sourceDirectory, "App_"+logDateDaysBefore(1)+"_1.log.gz")
			writeGzipFile(t, source, []byte("spooled with the old target spelling"))
			spoolRoot, err := objectstoragespool.NewRoot(filepath.Join(root, "spool"), "app", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			store, err := objectstoragespool.NewStore(spoolRoot)
			require.NoError(t, err)
			_, err = store.SaveRetryableFailure(ctx, source)
			require.NoError(t, err)
			remote := newDestinationUploaderRecorder("primary")
			replayUploader, err := spools3.New(remote)
			require.NoError(t, err)
			worker, err := compressedlogworker.NewWorker(store, compressedlogworker.RouteRegistry{{
				FileName: logpattern.FileNamePattern{Dir: sourceDirectory, Target: "app", Extension: "log"},
				Uploader: replayUploader,
			}}, time.Minute)
			require.NoError(t, err)

			report, err := worker.RunOnce(ctx)
			if sensitive {
				require.ErrorIs(t, err, compressedlogworker.ErrNoMatchingRoute)
				require.Empty(t, remote.pathsFor("primary"))
				return
			}
			require.NoError(t, err)
			require.True(t, report.File.UploadSucceeded)
			require.Equal(t, pkgspool.StateDone, report.File.State)
			require.Len(t, remote.pathsFor("primary"), 1)
			require.Equal(t, filepath.Base(source), filepath.Base(remote.pathsFor("primary")[0]))
		})
	}
}
