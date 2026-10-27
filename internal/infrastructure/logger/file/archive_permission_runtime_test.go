package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFileArchiveHandoffPermissionFailureRetainsSourceAndRetries(t *testing.T) {
	for _, storeType := range []failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive} {
		t.Run(string(storeType), func(t *testing.T) {
			base := t.TempDir()
			cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, pkglogger.LoggerContractSync, time.Hour)
			cfg.Default.CompressedLogFileUploadFailureStore.DurableType = []failurestore.DurableType{storeType}
			spoolRoot, err := objectstoragespool.NewRoot(base, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			spoolRoot, err = spoolRoot.ForDestination("s3-current")
			require.NoError(t, err)
			destination := spoolRoot.ActiveDir()
			temporaryDirectory := filepath.Join(spoolRoot.Path(), ".staging")
			if storeType == failurestore.DurableTypeFileArchive {
				cfg.Default.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig = objectstorage.CompressedLogFileUploadFailureArchiveConfig{
					RootPath:       base,
					LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: time.Hour,
				}
				archiveRoot, err := compressedlogarchive.NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
				require.NoError(t, err)
				archiveRoot, err = archiveRoot.ForDestination("s3-current")
				require.NoError(t, err)
				destination = archiveRoot.RetryableDir()
				temporaryDirectory = destination
			}
			owner, permissionErrors := newArchivePermissionTestOwner()
			raw := &archiveUploadDestinationStub{
				ids: []objectstorage.DestinationID{"s3-current"}, err: errors.New("remote upload unavailable"),
			}
			fileName := archiveUploaderTestFileName(t.TempDir())
			managed, err := newLogFileUploaderWithFailureStorage(
				objectstorage.NewUploadTargetConfigAdapter(cfg.Default),
				fileName, raw, owner,
				archiveUploaderTestSpoolRuntimes(t, cfg),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, closeArchiveUploader(managed)) })
			source := filepath.Join(fileName.Dir, "app_20260913_1.log.gz")
			payload := []byte("compressed source retained until handoff succeeds")
			require.NoError(t, os.WriteFile(source, payload, 0o600))

			// 初期化は成功しています。ここで外部の変更により、ownerがACLを変更することは
			// 妨げずに、次の一時書き込みを妨げます。
			restore := denyArchiveRuntimeDirectoryAccess(t, temporaryDirectory, true)
			for attempt := 0; attempt < 2; attempt++ {
				require.False(t, owner.tryUploadRotatedFileUsingWithNotifications(context.Background(), managed, source, runNotificationImmediately))
				awaitArchivePermissionError(t, permissionErrors)
				contents, err := os.ReadFile(source)
				require.NoError(t, err)
				require.Equal(t, payload, contents)
				require.NoFileExists(t, filepath.Join(destination, filepath.Base(source)))
			}

			// 外部のactorがaccessを戻した後は、同じuploaderが次の呼び出しで成功します。
			// storeやloggerを作り直す必要はありません。
			restore()
			require.True(t, owner.tryUploadRotatedFileUsingWithNotifications(context.Background(), managed, source, runNotificationImmediately))
			contents, err := os.ReadFile(filepath.Join(destination, filepath.Base(source)))
			require.NoError(t, err)
			require.Equal(t, payload, contents)
			// 保存先の権限復旧後は退避だけを再開し、直接uploadは繰り返しません。
			require.Equal(t, 1, raw.callCount())
		})
	}
}

func TestCompressedLogReplayPermissionFailureNotifiesAndContinues(t *testing.T) {
	base := t.TempDir()
	owner, permissionErrors := newArchivePermissionTestOwner()
	uploaded := make(chan string, 1)
	raw := &archivePermissionRuntimeUploader{
		archiveUploadDestinationStub: archiveUploadDestinationStub{ids: []objectstorage.DestinationID{"s3-current"}},
		uploaded:                     uploaded,
	}
	cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, pkglogger.LoggerContractSync, time.Millisecond)
	spoolRuntime := archiveUploaderTestSpoolRuntimes(t, cfg)
	managed, err := newLogFileUploaderWithFailureStorage(
		objectstorage.NewUploadTargetConfigAdapter(cfg.Default),
		archiveUploaderTestFileName(t.TempDir()), raw, owner,
		spoolRuntime,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeArchiveUploader(managed)) })
	root, err := objectstoragespool.NewRoot(base, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	root, err = root.ForDestination("s3-current")
	require.NoError(t, err)
	restore := denyArchiveRuntimeDirectoryAccess(t, root.ProcessingDir(), false)
	// queueに置くfileを追加する前に、実際に失敗した周期を待ちます。これにより、
	// 権限の変更より前に始まった列挙がそのfileを取得できないようにします。
	awaitArchivePermissionError(t, permissionErrors)
	const fileName = "app_20260913_1.log.gz"
	payload := []byte("queued data must survive an unavailable processing directory")
	source := filepath.Join(root.ActiveDir(), fileName)
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	// fileを置く前の周期の通知が残っていても使わず、fileを置いた後に始まった周期が失敗したことを
	// 待ってから、その周期がfileを処理対象として取得していないことを確認します。
	awaitArchivePermissionErrorFromCycleStartedAfter(t, permissionErrors, time.Now())
	require.Zero(t, raw.callCount())
	contents, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, payload, contents)

	restore()
	path := receiveForTest(t, uploaded, "the same spool runtime did not replay on a later tick after access was restored")
	require.Equal(t, filepath.Join(root.ProcessingDir(), fileName), path)
	// routeをCloseすると、その登録が解放されます。続いて別に所有されているroot runtimeを
	// Closeすると、確認の前に、受理したreplayの遷移を最後まで処理させます。
	require.NoError(t, closeArchiveUploader(managed))
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "spoolRuntime Close", spoolRuntime))
	contents, err = os.ReadFile(filepath.Join(root.DoneDir(), fileName))
	require.NoError(t, err)
	require.Equal(t, payload, contents)
	require.NoFileExists(t, source)
	require.Equal(t, 1, raw.callCount())
}

// archivePermissionFailureは、OnErrorへ通知された権限のerrorと、その内部ログの発生時刻です。
// 周期保守の失敗は、周期の終わりに報告した時点の時刻を内部ログに持ちます。
type archivePermissionFailure struct {
	err        error
	occurredAt time.Time
}

func newArchivePermissionTestOwner() (*FileLogger, <-chan archivePermissionFailure) {
	errorsSeen := make(chan archivePermissionFailure, 8)
	owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	owner.OnError = func(_ context.Context, err error, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		if errors.Is(err, os.ErrPermission) {
			select {
			case errorsSeen <- archivePermissionFailure{err: err, occurredAt: entry.OccurredAt}:
			default:
			}
		}
	}
	return owner, errorsSeen
}

func awaitArchivePermissionError(t *testing.T, errorsSeen <-chan archivePermissionFailure) {
	t.Helper()
	failure := receiveForTest(t, errorsSeen, "OnError did not receive the filesystem permission failure")
	require.ErrorIs(t, failure.err, os.ErrPermission)
}

// awaitArchivePermissionErrorFromCycleStartedAfterは、after以降に始まった再送の周期が権限の
// errorで失敗したことを待ちます。再送の周期は一つずつ順に実行され、失敗は周期の終わりに時刻を
// 付けて報告されます。after以降に報告された失敗の一件目はafterより前に始まった周期の場合が
// あるため、二件目を待ちます。それより前に報告された通知は読み飛ばします。
func awaitArchivePermissionErrorFromCycleStartedAfter(t *testing.T, errorsSeen <-chan archivePermissionFailure, after time.Time) {
	t.Helper()
	timer := time.NewTimer(hangTimeoutForTest)
	defer timer.Stop()
	for reportedAfter := 0; reportedAfter < 2; {
		select {
		case failure := <-errorsSeen:
			require.ErrorIs(t, failure.err, os.ErrPermission)
			if failure.occurredAt.After(after) {
				reportedAfter++
			}
		case <-timer.C:
			t.Fatalf("no replay cycle that started after %s reported the filesystem permission failure within %s", after, hangTimeoutForTest)
		}
	}
}

type archivePermissionRuntimeUploader struct {
	archiveUploadDestinationStub
	uploaded chan<- string
}

func (u *archivePermissionRuntimeUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *archivePermissionRuntimeUploader) UploadDestinations(
	ctx context.Context, input uploader.UploadRequest, destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	report := u.archiveUploadDestinationStub.UploadDestinations(ctx, input, destinationIDs)
	select {
	case u.uploaded <- input.FilePath():
	case <-ctx.Done():
	}
	return report
}
