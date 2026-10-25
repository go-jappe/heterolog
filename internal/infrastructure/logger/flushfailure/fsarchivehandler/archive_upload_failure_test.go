package fsarchivehandler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// category別prefixから異なる宛先IDを作り、通常ログと同じ退避処理へ接続することを
// factory・checkpoint・元ファイル削除まで含めて確認します。
func TestFailureArchiveInlineUploadFailureIsArchivedAndSourceRemoved(t *testing.T) {
	requests := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	errorsReported := make(chan error, 4)
	pipeline := newTestArchiveHandler(t, time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		configureFailureArchiveManagedUploadTest(t, cfg, server.URL, false)
	}, InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		errorsReported <- err
	}})
	handler := pipeline.handler
	var destinations []objectstorage.DestinationID
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		managed := handler.archiveUploader(category).(uploader.DestinationUploader)
		ids := managed.DestinationIDs()
		require.Len(t, ids, 1)
		destinations = append(destinations, ids[0])
		fileName := handler.Fsstore.FileNameRetryable
		if category == failurearchive.FailureCategoryPermanent {
			fileName = handler.Fsstore.FileNamePermanent
		}
		path := filepath.Join(fileName.Dir, "test-app_20261008_1.jsonl.gz")
		payload := []byte("compressed backend failure records: " + string(category))
		require.NoError(t, os.WriteFile(path, payload, 0o600))
		outcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		require.False(t, outcome.retryRequired)
		require.NoFileExists(t, path)
		root := failureArchiveManagedUploadRoot(t, handler, ids[0])
		archived, err := os.ReadFile(filepath.Join(root.RetryableDir(), filepath.Base(path)))
		require.NoError(t, err)
		require.Equal(t, payload, archived)
		select {
		case requestPath := <-requests:
			require.Equal(t, "/failure-archive/backend/"+string(category)+"/"+filepath.Base(path), requestPath)
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("object storage upload was not attempted")
		}
	}
	require.NotEqual(t, destinations[0], destinations[1], "same basename in different backend failure categories must have separate destinations")
	select {
	case err := <-errorsReported:
		// 退避先への保存の通知は、送信失敗の原因を保持します。
		var statusErr *s3uploader.HTTPStatusError
		require.ErrorAs(t, err, &statusErr)
		require.Equal(t, http.StatusServiceUnavailable, statusErr.Code)
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("durably accepted object storage failure was not sent to OnError")
	}
	require.NoError(t, shutdownFailureArchiveForTest(pipeline))
	managed := handler.archiveUploader(failurearchive.FailureCategoryRetryable)
	require.ErrorIs(t, managed.Upload(context.Background(), uploadertest.Request(t, filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20261008_1.jsonl.gz"))), compressedlogfailure.ErrUploaderClosed)
}

// 二つの退避先のうち片方が失敗しても、checkpointの途中経過から残りだけを
// 再試行します。通常のログ書き込みを待たず、保存完了後に元ファイルを削除します。
func TestFailureArchiveInlineRetriesRemainingFailureTargetWithoutUploadingAgain(t *testing.T) {
	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(uploadStarted)
			<-releaseUpload
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpload) }) }
	defer release()
	pipeline := newTestArchiveHandler(t, time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		configureFailureArchiveManagedUploadTest(t, cfg, server.URL, true)
	})
	handler := pipeline.handler
	// 実HTTP serverへの通信があるため仮想時間では進められません。再開間隔だけを短くし、
	// 保存先を戻す前の再試行は失敗として次の間隔へ回します。
	setArchiveLogFileProcessingRecoveryIntervalForTest(t, handler, 10*time.Millisecond)
	category := failurearchive.FailureCategoryRetryable
	fileName := handler.Fsstore.FileNameRetryable
	path := filepath.Join(fileName.Dir, "test-app_20261008_1.jsonl.gz")
	require.NoError(t, os.WriteFile(path, []byte("compressed backend records"), 0o600))
	ids := handler.archiveUploader(category).(uploader.DestinationUploader).DestinationIDs()
	root := failureArchiveManagedUploadRoot(t, handler, ids[0])
	finished := make(chan archiveFileOperationResult, 1)
	go func() {
		finished <- handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
	}()
	select {
	case <-uploadStarted:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("upload did not begin")
	}
	// 送信前の再利用確認が終わった後にarchiveの保存先を塞ぎます。
	previousDirectory := root.RetryableDir() + "-test-backup"
	require.NoError(t, os.Rename(root.RetryableDir(), previousDirectory))
	require.NoError(t, os.WriteFile(root.RetryableDir(), []byte("temporarily blocked"), 0o600))
	release()
	select {
	case outcome := <-finished:
		require.True(t, outcome.retryRequired)
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("initial failed save did not return")
	}
	require.FileExists(t, path)
	key := checkpointstore.CheckpointKey{Generation: "20261008-1", Compressed: true}
	progress, found, err := handler.compressedArchiveUploadCheckpointStore(category).LoadUploadProgress(context.Background(), key, ids[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, progress.SavedTargets, 1)
	require.Equal(t, failurestore.DurableTypeSpool, progress.SavedTargets[0].Kind())
	require.NoError(t, os.Rename(root.RetryableDir(), root.RetryableDir()+"-test-obstruction"))
	require.NoError(t, os.Rename(previousDirectory, root.RetryableDir()))
	// 通常のwriteやcleanupを呼ばず、受理済みの再開処理が保存と元ファイルの削除を
	// 完了するまで待ちます。Closeは追加登録だけを止め、受理済みの再試行は続けます。
	recovered := make(chan error, 1)
	go func() { recovered <- handler.logFileProcessingRecovery.Close() }()
	select {
	case err := <-recovered:
		require.NoError(t, err)
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("accepted recovery did not save the remaining failure target")
	}
	require.NoFileExists(t, path)
	require.EqualValues(t, 1, calls.Load(), "resume must only save the missing archive target")
	require.FileExists(t, filepath.Join(root.RetryableDir(), filepath.Base(path)))
	require.NoError(t, shutdownFailureArchiveForTest(pipeline))
}

func TestFailureArchivePipelineUsesInjectedSpoolWithoutClosingItsOwner(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := newValidArchiveHandlerConfig(t)
			configureFailureArchiveManagedUploadTest(t, &cfg, "http://127.0.0.1:1", true)
			cfg.lifecycleMode = mode
			require.NoError(t, checkpointstore.PrepareDirectory(cfg.archiveCheckpoint.Directory))
			spoolCfg := cfg.objectStorageUpload.CompressedLogFileUploadSpool
			// 一つのmapへ複数backendを渡しても、Pipelineは所有者を選択・解放しません。
			services := []objectstorage.UploadTarget{objectstorage.UploadTargetS3, "another-backend"}
			configs := compressedlog.NewConfigAdapters(spoolCfg, services)
			runtimes, err := compressedlog.NewRootRuntimes(configs...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runtimes.Close()) })
			pipeline, err := NewFailureArchivePipeline(context.Background(), archiveFixedClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}, cfg,
				observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{}, WithCompressedLogSpoolRuntimes(runtimes))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, shutdownFailureArchiveForTest(pipeline)) })
			require.NoError(t, shutdownFailureArchiveForTest(pipeline))
			for _, config := range configs {
				_, err := compressedlog.NewRootRuntimes(config)
				require.ErrorIs(t, err, compressedlog.ErrSpoolRootAlreadyOwned, "pipeline must only close its routes, not the injected root owner")
			}
			require.NoError(t, runtimes.Close())
			reopened, err := compressedlog.NewRootRuntimes(configs...)
			require.NoError(t, err, "caller must be able to release every backend root after pipeline shutdown")
			require.NoError(t, reopened.Close())
		})
	}
}

func TestFailureArchivePipelineRejectsMissingSpoolRuntimesAtUploaderConstruction(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		for _, test := range []struct {
			name    string
			options []FailureArchivePipelineOption
		}{
			{name: "omitted"},
			{name: "nil", options: []FailureArchivePipelineOption{WithCompressedLogSpoolRuntimes(nil)}},
			{name: "empty", options: []FailureArchivePipelineOption{WithCompressedLogSpoolRuntimes(compressedlog.RootRuntimes{})}},
			{name: "nil_entry", options: []FailureArchivePipelineOption{WithCompressedLogSpoolRuntimes(compressedlog.RootRuntimes{objectstorage.UploadTargetS3: nil})}},
		} {
			t.Run(string(mode)+"/"+test.name, func(t *testing.T) {
				cfg := newValidArchiveHandlerConfig(t)
				configureFailureArchiveManagedUploadTest(t, &cfg, "http://127.0.0.1:1", true)
				cfg.lifecycleMode = mode
				require.NoError(t, checkpointstore.PrepareDirectory(cfg.archiveCheckpoint.Directory))
				pipeline, err := NewFailureArchivePipeline(context.Background(), archiveFixedClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}, cfg,
					observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{}, test.options...)
				require.ErrorIs(t, err, compressedlogfailure.ErrInvalidConfig)
				require.ErrorIs(t, err, compressedlogfailure.ErrNoMatchingSpoolRuntime)
				var mismatch *compressedlogfailure.SpoolRuntimeMismatchError
				require.ErrorAs(t, err, &mismatch)
				require.Equal(t, objectstorage.UploadTargetS3, mismatch.Service)
				require.Nil(t, pipeline)
				entries, err := os.ReadDir(cfg.objectStorageUpload.CompressedLogFileUploadSpool.SpoolPath())
				require.NoError(t, err)
				require.Empty(t, entries, "missing runtime must not trigger spool construction")
			})
		}
	}
}

func TestFailureArchivePipelineAcceptsNilRuntimesWhenSpoolIsUnused(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testArchiveConfig)
	}{
		{name: "spool_disabled", mutate: func(cfg *testArchiveConfig) {
			cfg.objectStorageUpload.CompressedLogFileUploadFailureStore.DurableType = []failurestore.DurableType{failurestore.DurableTypeFileArchive}
		}},
		{name: "upload_disabled", mutate: func(cfg *testArchiveConfig) { cfg.objectStorageUpload.Enabled = false }},
		{name: "lifecycle_disabled", mutate: func(cfg *testArchiveConfig) { cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := newValidArchiveHandlerConfig(t)
			configureFailureArchiveManagedUploadTest(t, &cfg, "http://127.0.0.1:1", true)
			test.mutate(&cfg)
			require.NoError(t, checkpointstore.PrepareDirectory(cfg.archiveCheckpoint.Directory))
			pipeline, err := NewFailureArchivePipeline(context.Background(), archiveFixedClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}, cfg,
				observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{}, WithCompressedLogSpoolRuntimes(nil))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, shutdownFailureArchiveForTest(pipeline)) })
			entries, err := os.ReadDir(cfg.objectStorageUpload.CompressedLogFileUploadSpool.SpoolPath())
			require.NoError(t, err)
			require.Empty(t, entries, "unused spool must not be constructed")
		})
	}
}

func configureFailureArchiveManagedUploadTest(t *testing.T, cfg *testArchiveConfig, endpoint string, spoolEnabled bool) {
	t.Helper()
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "managed_failure_archive_test", AutoRecoverMissingCheckpoint: true}
	cfg.objectStorageUpload = testFailureArchiveUploadConfig()
	cfg.objectStorageUpload.S3.Endpoint = endpoint
	cfg.objectStorageUpload.S3.ForcePathStyle = true
	cfg.objectStorageUpload.S3.Timeout = 2 * time.Second
	cfg.objectStorageUpload.CompressedLogFileUploadFailureStore = objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive},
		DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{FileArchiveConfig: objectstorage.CompressedLogFileUploadFailureArchiveConfig{
			RootPath: t.TempDir(), LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: time.Hour,
		}},
	}
	if spoolEnabled {
		cfg.objectStorageUpload.CompressedLogFileUploadFailureStore.DurableType = append(cfg.objectStorageUpload.CompressedLogFileUploadFailureStore.DurableType, failurestore.DurableTypeSpool)
		cfg.objectStorageUpload.CompressedLogFileUploadSpool = objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
			SpoolPath: t.TempDir(), Target: cfg.target, LoggerContract: pkglogger.LoggerContractSync.String(),
			RunnerInterval: time.Hour, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: time.Minute,
		})
	}
}

func failureArchiveManagedUploadRoot(t *testing.T, handler *FsArchiveHandler, destination objectstorage.DestinationID) compressedlogarchive.Root {
	t.Helper()
	cfg := handler.Config.ObjectStorageUpload().CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig
	root, err := compressedlogarchive.NewRoot(cfg.RootPath, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	root, err = root.ForDestination(destination)
	require.NoError(t, err)
	return root
}
