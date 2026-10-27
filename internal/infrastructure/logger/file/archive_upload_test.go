package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestNewLogFileUploader_MasterDisabledSkipsUpload(t *testing.T) {
	created, err := newLogFileUploader(objectstorage.NewLogFileUploadConfigAdapter(objectstorage.LogFileUploadConfig{
		Enabled: false,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		},
	}), DefaultLogger)

	require.NoError(t, err)
	require.Nil(t, created)
}

func TestNewLogFileUploader_ClassificationDisabledSkipsUpload(t *testing.T) {
	created, err := newLogFileUploader(objectstorage.NewLogFileUploadConfigAdapter(objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: false,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		},
		Error: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		},
	}), DefaultLogger)

	require.NoError(t, err)
	require.Nil(t, created)
}

func TestNewLogFileUploaderWithFailureStorageWithoutDurableStoreReturnsBaseUploader(t *testing.T) {
	for _, tt := range []struct {
		name  string
		types []failurestore.DurableType
	}{
		{name: "unspecified"},
		{name: "empty", types: []failurestore.DurableType{}},
		{name: "none", types: []failurestore.DurableType{failurestore.DurableTypeNone}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			baseUploader := &archiveUploadDestinationStub{}
			actual, err := newLogFileUploaderWithFailureStorage(
				objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
					CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
						DurableType: tt.types,
					},
				}),
				logpattern.FileNamePattern{},
				baseUploader,
				nil,
			)
			require.NoError(t, err)
			require.Same(t, baseUploader, actual)
		})
	}
}

func TestNewLogFileUploaderWithFailureStorageUsesLoggerContractCompressedLogRoot(t *testing.T) {
	tests := []struct {
		name     string
		contract pkglogger.LoggerContractKind
	}{
		{name: "logger", contract: pkglogger.LoggerContractSync},
		{name: "async logger", contract: pkglogger.LoggerContractAsyncBestEffort},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, tt.contract, time.Hour)
			raw := &archiveUploadDestinationStub{
				ids: []objectstorage.DestinationID{"s3-current"},
				err: errors.New("upload unavailable"),
			}
			fileName := archiveUploaderTestFileName(t.TempDir())
			managed, err := newLogFileUploaderWithFailureStorage(
				objectstorage.NewUploadTargetConfigAdapter(cfg.Default),
				fileName,
				raw,
				nil,
				archiveUploaderTestSpoolRuntimes(t, cfg),
			)
			require.NoError(t, err)
			require.NotNil(t, managed)
			t.Cleanup(func() { require.NoError(t, closeArchiveUploader(managed)) })

			active := filepath.Join(
				base,
				"heterolog",
				"spool",
				"objectstorage",
				"s3",
				tt.contract.String(),
				"s3-current",
				"active",
			)
			info, statErr := os.Stat(active)
			require.NoError(t, statErr)
			require.True(t, info.IsDir())
		})
	}
}

func TestNewLogFileUploaderWithFailureStorageDurablyAcceptsOnlyCompressedCanonicalArchive(t *testing.T) {
	base := t.TempDir()
	fileDir := t.TempDir()
	fileName := archiveUploaderTestFileName(fileDir)
	raw := &archiveUploadDestinationStub{
		ids: []objectstorage.DestinationID{"s3-current"},
		err: errors.New("upload unavailable"),
	}
	cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, pkglogger.LoggerContractSync, time.Hour)
	managed, err := newLogFileUploaderWithFailureStorage(
		objectstorage.NewUploadTargetConfigAdapter(cfg.Default),
		fileName,
		raw,
		nil,
		archiveUploaderTestSpoolRuntimes(t, cfg),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeArchiveUploader(managed)) })

	compressedPath := filepath.Join(fileDir, "app_20260902_1.log.gz")
	require.NoError(t, os.WriteFile(compressedPath, []byte("valuable compressed log"), 0o600))
	require.NoError(t, managed.Upload(context.Background(), uploadertest.Request(t, compressedPath)))
	require.Equal(t, 1, raw.callCount(), "direct uploader must run once per compressed log file")
	rawPath := filepath.Join(fileDir, "app_20260902_2.log")
	require.NoError(t, os.WriteFile(rawPath, []byte("uncompressed log"), 0o600))
	require.Error(t, managed.Upload(context.Background(), uploadertest.Request(t, rawPath)))
	require.Equal(t, 2, raw.callCount(), "raw path must also make exactly one direct attempt")
	require.FileExists(t, rawPath)
	// inspection中に共有runnerがfileをprocessingへ移さないよう、このroute ownerを閉じます。
	require.NoError(t, closeArchiveUploader(managed))
	require.ErrorIs(t, managed.Upload(context.Background(), uploadertest.Request(t, rawPath)), compressedlogfailure.ErrUploaderClosed)
	require.Equal(t, 2, raw.callCount(), "closed uploader must reject new direct attempts")
	// durable handoff後は既存のarchive cleanupが元fileを削除できます。
	require.NoError(t, os.Remove(compressedPath))

	root, err := objectstoragespool.NewRoot(
		base,
		"heterolog",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	require.NoError(t, err)
	store, err := objectstoragespool.NewDestinationStores(root)
	require.NoError(t, err)
	files, err := store.ListFiles(context.Background(), pkgspool.StateActive)
	require.NoError(t, err)
	require.Len(t, files, 1, "raw upload failure must not create another spool file")
	require.Equal(t, filepath.Base(compressedPath), files[0].FileName)
	require.FileExists(t, files[0].Path)

}

func TestCanonicalCompressedArchiveRequiresOwnedDirectory(t *testing.T) {
	directory := t.TempDir()
	fileName := archiveUploaderTestFileName(directory)
	shouldStore := logpattern.CompressedFilePathMatcher(fileName)

	require.True(t, shouldStore(filepath.Join(directory, "app_20260902_1.log.gz")))
	require.False(t, shouldStore(filepath.Join(t.TempDir(), "app_20260902_1.log.gz")))
	require.False(t, shouldStore(filepath.Join(directory, "app_20260902_1.log")))
	require.False(t, shouldStore(filepath.Join(directory, "app_20260902_1.log.gz.tmp")))
}

func TestArchiveCompressedLogMaintenanceErrorIsDispatchedToOnError(t *testing.T) {
	logger := &FileLogger{
		logger: DefaultLogger,
		Clock:  clock.NewUTCClock(),
		Tracer: observer.NopTracer{},
		Meter:  observer.NopMeter{},
	}
	dispatched := make(chan error, 1)
	logger.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		dispatched <- err
	}
	dispatcher := notification.NewDispatcher()
	wantErr := errors.New("cannot scan durable compressed log file spool")
	logger.notifyArchiveCompressedLogMaintenanceError(
		context.Background(),
		"compressed log file upload failure spool maintenance failed",
		wantErr,
		dispatcher,
	)
	dispatcher.CloseAfterDrain()

	gotErr := receiveForTest(t, dispatched, "compressed log file maintenance error was not dispatched")
	require.ErrorIs(t, gotErr, wantErr)
	receiveForTest(t, dispatcher.Done(), "compressed log file maintenance notification dispatcher did not stop")
}

type archiveUploadTestSavedTargetDetails string

func (archiveUploadTestSavedTargetDetails) Kind() failurestore.DurableType { return "third-test-store" }
func (r archiveUploadTestSavedTargetDetails) String() string               { return string(r) }

func TestManagedFileArchiveUploaderNotifiesSavedTargetsWithoutKnowingTheirKind(t *testing.T) {
	logger := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	var received []error
	logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		received = append(received, err)
	}
	uploadErr := errors.New("upload unavailable")
	managed := &managedFileArchiveUploader{owner: logger}
	notifications := &backgroundTaskNotificationQueue{}
	managed.notifyUploadFailureInternalLogs(context.Background(), []compressedlogfailure.Notification{{
		SourcePath:      "app_20260929_1.log.gz",
		DestinationID:   "destination-a",
		FailureCategory: uploader.FailureCategoryRetryable,
		UploadErr:       uploadErr,
		SavedTargets: []compressedlogfailure.SavedTargetDetails{
			archiveUploadTestSavedTargetDetails("third-test-store: /other/file.log.gz"),
		},
	}}, notifications.enqueue)
	notifications.runDeferredNotifications()
	require.Len(t, received, 1)
	require.ErrorIs(t, received[0], uploadErr)
	requireFileLoggerInternalError(t, received[0], DefaultLogger, OperationArchiveUpload)
	var saved *SavedUploadFailureError
	require.ErrorAs(t, received[0], &saved)
	require.Equal(t, []compressedlogfailure.SavedTargetDetails{
		archiveUploadTestSavedTargetDetails("third-test-store: /other/file.log.gz"),
	}, saved.SavedTargets)
	require.Equal(t, uploader.FailureCategoryRetryable, saved.FailureCategory)
	require.Equal(t, objectstorage.DestinationID("destination-a"), saved.DestinationID)
}

// 内部エラーの文言は、OnErrorを通じて管理者が読む通知です。退避先の詳細はkindを
// 解釈せずString()の表記で示し、管理者に確認を求める文言を含めます（文言の契約）。
func TestArchiveUploadNotificationErrorsFormatOperatorDetails(t *testing.T) {
	saved := &SavedUploadFailureError{
		SavedTargets: []compressedlogfailure.SavedTargetDetails{
			archiveUploadTestSavedTargetDetails("third-test-store: /other/file.log.gz"),
		},
		FailureCategory: uploader.FailureCategoryRetryable,
		DestinationID:   "destination-a",
		Err:             errors.New("upload unavailable"),
	}
	require.Equal(t,
		`object storage upload failed; compressed log file was saved to durable stores [third-test-store: /other/file.log.gz] (upload failure category "retryable", destination "destination-a"): upload unavailable`,
		saved.Error())
	conflict := &PreservedCompressedLogConflictError{
		OriginalPath:  "/archive/retryable/app_20260929_1.log.gz",
		RetainedPath:  "/archive/retryable/.compressed-log-conflict-preserved",
		DestinationID: "destination-a",
	}
	require.Equal(t,
		`preserved conflicting compressed log file "/archive/retryable/app_20260929_1.log.gz" as "/archive/retryable/.compressed-log-conflict-preserved" (destination "destination-a"); operator review required`,
		conflict.Error())
}

func TestManagedFileArchiveUploaderDoesNotNotifyFailedSaveAsAccepted(t *testing.T) {
	logger := &FileLogger{
		logger: DefaultLogger,
		Clock:  clock.NewUTCClock(),
		Tracer: observer.NopTracer{},
		Meter:  observer.NopMeter{},
	}
	dispatched := 0
	logger.OnError = func(
		context.Context,
		error,
		*logging.LogEntry,
		pkgobserver.Tracer,
		pkgobserver.Meter,
	) {
		dispatched++
	}
	uploadFailureNotifications := []compressedlogfailure.Notification{{
		SourcePath: "app_20260902_1.log.gz",
		UploadErr:  errors.New("remote upload failed"),
		SaveErr:    errors.New("durable save failed"),
	}}
	managed := &managedFileArchiveUploader{owner: logger}
	notifications := &backgroundTaskNotificationQueue{}
	managed.notifyUploadFailureInternalLogs(context.Background(), uploadFailureNotifications, notifications.enqueue)
	notifications.runDeferredNotifications()

	require.Zero(t, dispatched)
}

func TestManagedFileArchiveUploaderNotifiesExistingFileCheckError(t *testing.T) {
	logger := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	var received []error
	logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		received = append(received, err)
	}
	injected := errors.New("cannot inspect existing spool")
	uploadFailureNotifications := []compressedlogfailure.Notification{{
		SourcePath: "app_20260929_1.log.gz", DestinationID: "destination-a", ExistingFileCheckErr: injected,
	}}
	managed := &managedFileArchiveUploader{owner: logger}
	notifications := &backgroundTaskNotificationQueue{}
	managed.notifyUploadFailureInternalLogs(context.Background(), uploadFailureNotifications, notifications.enqueue)
	notifications.runDeferredNotifications()
	require.Len(t, received, 1)
	require.ErrorIs(t, received[0], injected)
	var saved *SavedUploadFailureError
	require.False(t, errors.As(received[0], &saved), "an existing-file check error is not a saved upload failure")
}

func TestManagedFileArchiveUploaderNotifiesPreservedConflictEvenWhenSaveFails(t *testing.T) {
	logger := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	var received []error
	logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		received = append(received, err)
	}
	uploadFailureNotifications := []compressedlogfailure.Notification{{
		SourcePath:    "app_20260929_1.log.gz",
		DestinationID: "destination-a",
		SaveErr:       errors.New("save failed after preserving conflict"),
		ConflictRenames: []infrafsstore.ConflictRename{{
			OriginalPath: "/archive/retryable/app_20260929_1.log.gz",
			RetainedPath: "/archive/retryable/.compressed-log-conflict-preserved",
		}},
	}}
	managed := &managedFileArchiveUploader{owner: logger}
	notifications := &backgroundTaskNotificationQueue{}
	managed.notifyUploadFailureInternalLogs(context.Background(), uploadFailureNotifications, notifications.enqueue)
	notifications.runDeferredNotifications()
	require.Len(t, received, 1)
	requireFileLoggerInternalError(t, received[0], DefaultLogger, OperationArchiveUpload)
	var conflict *PreservedCompressedLogConflictError
	require.ErrorAs(t, received[0], &conflict)
	require.Equal(t, PreservedCompressedLogConflictError{
		OriginalPath:  "/archive/retryable/app_20260929_1.log.gz",
		RetainedPath:  "/archive/retryable/.compressed-log-conflict-preserved",
		DestinationID: "destination-a",
	}, *conflict)
}

func TestManagedFileArchiveUploaderClosesNotificationsAfterManagedResources(t *testing.T) {
	owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(t.TempDir(), pkglogger.LoggerContractSync, time.Hour)
	wrapped, err := newLogFileUploaderWithFailureStorage(
		objectstorage.NewUploadTargetConfigAdapter(cfg.Default),
		archiveUploaderTestFileName(t.TempDir()),
		&archiveUploadDestinationStub{ids: []objectstorage.DestinationID{"s3-current"}},
		owner,
		archiveUploaderTestSpoolRuntimes(t, cfg),
	)
	require.NoError(t, err)
	managed := wrapped.(*managedFileArchiveUploader)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "managed Close", managed))
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "managed Close", managed))
	select {
	case <-managed.maintenanceNotifications.Done():
	default:
		t.Fatal("Close returned before its accepted notifications drained")
	}
	request := uploadertest.Request(t, filepath.Join(t.TempDir(), "app_20260902_1.log.gz"))
	require.ErrorIs(t, managed.Upload(context.Background(), request), compressedlogfailure.ErrUploaderClosed)
}

func TestCloseArchiveUploaderReleasesLastContractRootOwner(t *testing.T) {
	base := t.TempDir()
	fileName := archiveUploaderTestFileName(t.TempDir())
	raw := &archiveUploadDestinationStub{ids: []objectstorage.DestinationID{"s3-current"}}
	newManaged := func(interval time.Duration) uploader.Uploader {
		t.Helper()
		cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, pkglogger.LoggerContractSync, interval)
		runtime := archiveUploaderTestSpoolRuntimes(t, cfg)
		managed, err := newLogFileUploaderWithFailureStorage(
			objectstorage.NewUploadTargetConfigAdapter(cfg.Default),
			fileName,
			raw,
			nil,
			runtime,
		)
		require.NoError(t, err)
		require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "runtime Close", runtime))
		return managed
	}

	first := newManaged(time.Hour)
	require.NoError(t, closeArchiveUploader(first))
	// 最後のownerが停止済みなら、同じrootを別intervalで新規取得できます。
	second := newManaged(2 * time.Hour)
	require.NoError(t, closeArchiveUploader(second))
}

func archiveUploaderCompressedLogFileUploadFailureStoreConfig(
	base string,
	contract pkglogger.LoggerContractKind,
	runnerInterval time.Duration,
) objectstorage.LogFileUploadConfig {
	return objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
				DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
			},
		},
		CompressedLogFileUploadSpool: objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
			SpoolPath:                  base,
			Target:                     "heterolog",
			LoggerContract:             contract.String(),
			RunnerInterval:             runnerInterval,
			LifecycleInterval:          time.Hour,
			CircuitBreakerOpenCooldown: 5 * time.Minute,
		}),
	}
}

func archiveUploaderTestSpoolRuntimes(t *testing.T, cfg objectstorage.LogFileUploadConfig) compressedlog.RootRuntimes {
	t.Helper()
	runtimes, err := compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(cfg.CompressedLogFileUploadSpool, []objectstorage.UploadTarget{objectstorage.UploadTargetS3})...)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "runtimes Close", func() closerForTest { return runtimes }, true)
	return runtimes
}

func archiveUploaderTestFileName(dir string) logpattern.FileNamePattern {
	return logpattern.FileNamePattern{
		Dir:       dir,
		Target:    "app",
		Extension: LogFileExtension,
	}
}

type archiveUploadDestinationStub struct {
	mu              sync.Mutex
	ids             []objectstorage.DestinationID
	err             error
	failureCategory uploader.FailureCategory
	calls           int
}

func (u *archiveUploadDestinationStub) DestinationIDs() []objectstorage.DestinationID {
	return append([]objectstorage.DestinationID(nil), u.ids...)
}

func (u *archiveUploadDestinationStub) DestinationService(id objectstorage.DestinationID) (objectstorage.UploadTarget, bool) {
	// このfixtureはS3の送信先を模します。
	for _, registered := range u.ids {
		if registered == id {
			return objectstorage.UploadTargetS3, true
		}
	}
	return "", false
}

func (u *archiveUploadDestinationStub) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *archiveUploadDestinationStub) UploadDestinations(
	_ context.Context,
	_ uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	result := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
	for index, destinationID := range destinationIDs {
		result.Results[index] = uploader.DestinationResult{
			DestinationID: destinationID,
			Err:           u.err,
		}
		if u.err != nil {
			result.Results[index].FailureCategory = u.failureCategory
		}
	}
	return result
}

func (u *archiveUploadDestinationStub) callCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}
