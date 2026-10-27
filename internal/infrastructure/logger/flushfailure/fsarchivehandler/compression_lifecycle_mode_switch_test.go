package fsarchivehandler

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

// codecの試験境界で各commit段階の残存物を作り、旧modeの実処理をpanicさせます。
// 新modeは旧goroutine・Guard登録・メモリ上のcheckpointを引き継がず、永続状態だけで再開します。
func TestFailureArchiveCompressionRecoversAcrossLifecycleModeSwitch(t *testing.T) {
	for _, oldMode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
			for _, stage := range []string{"partial-temporary", "published-with-source", "source-delete-pending"} {
				t.Run(string(oldMode)+"/"+string(category)+"/"+stage, func(t *testing.T) {
					testFailureArchiveCompressionModeSwitch(t, oldMode, category, stage)
				})
			}
		}
	}
}

func testFailureArchiveCompressionModeSwitch(t *testing.T, oldMode pkgfsstore.FileLifecycleMode, category failurearchive.FailureCategory, stage string) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	var uploads atomic.Int32
	var payloadMu sync.Mutex
	var payloads [][]byte
	var objectPaths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payloadMu.Lock()
		payloads = append(payloads, payload)
		objectPaths = append(objectPaths, r.URL.Path)
		payloadMu.Unlock()
		uploads.Add(1)
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	delegate, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	interrupted := &modeSwitchArchiveCodec{CompressionCodec: delegate, stage: stage}
	cfg := newValidArchiveHandlerConfig(t)
	cfg.lifecycleMode = oldMode
	cfg.enableFileCompress = true
	cfg.compressionCodec = interrupted
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "archive-mode-switch", AutoRecoverMissingCheckpoint: false}
	cfg.lifecycleRunnerConfig.Interval = time.Hour
	cfg.objectStorageUpload = objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{Region: "us-east-1", Bucket: "archive", Prefix: "backend-records", Endpoint: server.URL, ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second},
	}
	require.NoError(t, checkpoint.PrepareDirectory(cfg.archiveCheckpoint.Directory))
	reported := make(chan error, 16)
	old := newModeSwitchArchivePipeline(t, cfg, now, InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		select {
		case reported <- err:
		default:
		}
	}})
	fileName, err := old.handler.archiveFileNamePattern(category)
	require.NoError(t, err)
	source := filepath.Join(fileName.Dir, "test-app_20261008_1.jsonl")
	compressed := source + ".gz"
	pending := logpattern.CompressionSourceDeletePendingPath(source)
	const content = "{\"id\":\"retained-failure-record\",\"reason\":\"backend unavailable\"}\n"
	require.NoError(t, os.WriteFile(source, []byte(content), 0o600))
	interrupted.source = source
	interruptionGuard := appendlog.DirectoryGuardForPath(fileName.Dir)
	interruptionToken := appendlog.NewDirectoryGuardToken()
	interruptionPaths := []string{source, compressed, pending}
	interrupted.beforePanic = func() {
		// 中断時点の残存物を、旧taskがpanicから戻る際の追加走査からも保護します。
		// 新processを構築する前に解除し、再起動後へメモリ上の保護を持ち越しません。
		interruptionGuard.ProtectPaths(interruptionToken, interruptionPaths...)
		// 通知を受ける試験goroutineの実行順に依存せず、中断位置で旧modeの再試行を止めます。
		// Pipelineのabortは非同期なので、再試行の停止要求だけは対象へ同期的に渡します。
		_ = old.RequestForceClose()
		_ = old.handler.logFileProcessingRecovery.ForceClose()
		if old.runnerRuntime != nil {
			for _, runner := range old.runnerRuntime.runners {
				_ = runner.ForceClose()
			}
		}
	}
	if oldMode == pkgfsstore.FileLifecycleModeInline {
		// 実際のrotation後の非同期処理へ渡し、panic通知を受けてから強制終了します。
		old.handler.dispatchArchiveRotatedFile(ctx, "active-file", source, category)
		select {
		case err := <-reported:
			var panicErr *ArchiveTaskPanicError
			require.ErrorAs(t, err, &panicErr)
			require.Equal(t, "mode-switch compression interruption", panicErr.Value)
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("inline compression panic was not reported")
		}
	} else {
		// RunOnceのpanicは通常Supervisorへ伝播します。試験では呼び出し境界で捕捉します。
		require.PanicsWithValue(t, "mode-switch compression interruption", func() {
			_, _ = modeSwitchArchiveRunner(t, old, category).RunOnce(ctx)
		})
	}
	require.EqualValues(t, 1, interrupted.compressCalls.Load())
	require.Zero(t, uploads.Load(), "interrupted compression must not reach object storage")
	require.ErrorIs(t, old.ForceClose(), pkgshutdown.ErrForcedClose)
	waitFailureArchiveClosedForTest(t, old)
	interruptionGuard.UnprotectPaths(interruptionToken, interruptionPaths...)
	require.Nil(t, old.archiveRootLease, "old writer, retry workers and root lease must all be retired")
	if stage == "partial-temporary" {
		// 捕捉されたpanicではdeferがtmpを消します。ここだけはプロセス強制中断で
		// deferが実行されなかった場合の残骸も追加して、新ownerでの除去を検証します。
		require.NoError(t, os.WriteFile(compressed+".tmp", []byte("incomplete gzip from stopped process"), 0o600))
	}
	if stage == "source-delete-pending" {
		require.NoFileExists(t, source)
		require.FileExists(t, pending)
	} else {
		require.FileExists(t, source)
	}
	if stage != "partial-temporary" {
		require.FileExists(t, compressed)
	}
	sourceID := checkpoint.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(category), fileName)
	checkpointPath := filepath.Join(cfg.archiveCheckpoint.Directory, sourceID+".log-file-uploads.json")
	require.FileExists(t, checkpointPath, "the source identity is generated by the old-mode factory")

	newMode := pkgfsstore.FileLifecycleModeInline
	if oldMode == newMode {
		newMode = pkgfsstore.FileLifecycleModeRunner
	}
	resumedCodec := &failureArchiveRecoveryCodec{}
	cfg.lifecycleMode, cfg.compressionCodec = newMode, resumedCodec
	resumed := newModeSwitchArchivePipeline(t, cfg, now, InternalLogHandlers{})
	activePath, err := resumed.ActiveArchivePath(category)
	require.NoError(t, err)
	require.NotEqual(t, source, activePath)
	newPattern, err := resumed.handler.archiveFileNamePattern(category)
	require.NoError(t, err)
	require.Equal(t, sourceID, checkpoint.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(category), newPattern))
	for range 3 {
		runModeSwitchArchiveLifecycle(t, resumed, category)
	}
	require.NoError(t, shutdownFailureArchiveForTest(resumed))
	waitFailureArchiveClosedForTest(t, resumed)
	require.NoFileExists(t, source)
	require.NoFileExists(t, compressed)
	require.NoFileExists(t, pending)
	require.NoFileExists(t, compressed+".tmp")
	require.FileExists(t, activePath, "the active writer must remain outside compression and deletion")
	require.EqualValues(t, 1, uploads.Load(), "the recovered file is sent once")
	if stage == "partial-temporary" {
		require.EqualValues(t, 1, resumedCodec.compressCalls.Load())
	} else {
		require.Zero(t, resumedCodec.compressCalls.Load(), "a completed compressed peer must be reused")
	}
	payloadMu.Lock()
	gotPayload := append([]byte(nil), payloads[0]...)
	gotObjectPath := objectPaths[0]
	payloadMu.Unlock()
	require.Equal(t, "/archive/backend-records/"+string(category)+"/"+filepath.Base(compressed), gotObjectPath, "the original backend failure category remains part of the object key")
	reader, err := gzip.NewReader(strings.NewReader(string(gotPayload)))
	require.NoError(t, err)
	recovered, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, content, string(recovered), "recovery must preserve all archived record bytes")

	// 削除直後の中断で同じ圧縮fileが復元された状況を再現し、元のmodeへ戻します。
	// 完了cursorをdiskから読み直すため、既に終わった宛先へ再送せず削除だけを行います。
	writeFailureArchiveRecoveryGzip(t, compressed, content)
	cfg.lifecycleMode = oldMode
	verified := newModeSwitchArchivePipeline(t, cfg, now, InternalLogHandlers{})
	for range 2 {
		runModeSwitchArchiveLifecycle(t, verified, category)
	}
	require.NoError(t, shutdownFailureArchiveForTest(verified))
	waitFailureArchiveClosedForTest(t, verified)
	require.EqualValues(t, 1, uploads.Load(), "persisted completion must survive switching back")
	require.NoFileExists(t, compressed)
	require.FileExists(t, checkpointPath)
}

type modeSwitchArchiveCodec struct {
	pkgfsstore.CompressionCodec
	stage         string
	source        string
	beforePanic   func()
	compressCalls atomic.Int32
}

func (c *modeSwitchArchiveCodec) Compress(source, destination *os.File) error {
	c.compressCalls.Add(1)
	if c.stage == "partial-temporary" {
		if _, err := destination.Write([]byte("unfinished gzip")); err != nil {
			return err
		}
		c.beforePanic()
		panic("mode-switch compression interruption")
	}
	return c.CompressionCodec.Compress(source, destination)
}

func (c *modeSwitchArchiveCodec) VerifyCompressedIntegrity(file *os.File) error {
	if err := c.CompressionCodec.VerifyCompressedIntegrity(file); err != nil {
		return err
	}
	// 圧縮内容を検証した実ファイルから、final公開後またはsource rename後の中断状態を作ります。
	// Windowsでも開いているtmpのrenameに依存しないよう、finalへ同じbytesを保存します。
	bytes, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}
	if err := os.WriteFile(c.source+".gz", bytes, 0o600); err != nil {
		return err
	}
	if c.stage == "source-delete-pending" {
		if err := os.Rename(c.source, logpattern.CompressionSourceDeletePendingPath(c.source)); err != nil {
			return err
		}
	}
	c.beforePanic()
	panic("mode-switch compression interruption")
}

func newModeSwitchArchivePipeline(t *testing.T, cfg testArchiveConfig, now time.Time, handlers InternalLogHandlers) *FailureArchivePipeline {
	t.Helper()
	pipeline, err := NewFailureArchivePipeline(context.Background(), archiveFixedClock{now: now}, cfg, observer.NopTracer{}, observer.NopMeter{}, handlers)
	require.NoError(t, err)
	useTestArchiveAvailableDiskSpace(pipeline)
	t.Cleanup(func() {
		_ = pipeline.ForceClose()
		waitFailureArchiveClosedForTest(t, pipeline)
	})
	return pipeline
}

func modeSwitchArchiveRunner(t *testing.T, pipeline *FailureArchivePipeline, category failurearchive.FailureCategory) *logfilerunner.Runner {
	t.Helper()
	want, err := pipeline.handler.archiveFileNamePattern(category)
	require.NoError(t, err)
	for _, runner := range pipeline.runnerRuntime.runners {
		directory, err := runner.DirectoryPath()
		require.NoError(t, err)
		if directory == want.Dir {
			return runner
		}
	}
	t.Fatalf("runner missing for %s", category)
	return nil
}

func runModeSwitchArchiveLifecycle(t *testing.T, pipeline *FailureArchivePipeline, category failurearchive.FailureCategory) {
	t.Helper()
	if pipeline.handler.runnerLifecycleEnabled() {
		_, err := modeSwitchArchiveRunner(t, pipeline, category).RunOnce(context.Background())
		require.NoError(t, err)
		return
	}
	// inlineは公開APIのログ保存をきっかけに保守します。通常のcache期限もClockで進めます。
	pipeline.handler.Clock = archiveFixedClock{now: pipeline.handler.Clock.Now().Add(testUsageCacheTTL)}
	require.NoError(t, pipeline.HandleFailure(context.Background(), loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{archiveFailedRecord("trigger", pipeline.handler.Clock.Now(), 429, "retryable")},
	}))
}
