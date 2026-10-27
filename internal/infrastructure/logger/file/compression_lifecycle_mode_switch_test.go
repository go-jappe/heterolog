package file

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	checkpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/shutdown"
)

// 実writer・runner・永続checkpointをmodeごとに構築し直します。
// 圧縮のpanic後に旧所有者を終了し、残存ファイルだけから新modeが処理を再開できるか確認します。
func TestLogCompressionRecoversAcrossLifecycleModeSwitch(t *testing.T) {
	for _, oldMode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		for _, stage := range []string{"partial-temporary", "published-with-source", "source-delete-pending"} {
			t.Run(string(oldMode)+"/"+stage, func(t *testing.T) {
				testLogCompressionModeSwitch(t, oldMode, stage)
			})
		}
	}
}

func testLogCompressionModeSwitch(t *testing.T, oldMode pkgfsstore.FileLifecycleMode, stage string) {
	t.Helper()
	ctx := context.Background()
	var requests atomic.Int32
	var payloadMu sync.Mutex
	var uploaded []byte
	var objectPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payloadMu.Lock()
		uploaded, objectPath = body, r.URL.Path
		payloadMu.Unlock()
		requests.Add(1)
		w.Header().Set("ETag", `"mode-switch-test"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	interrupted := &modeSwitchLogCodec{CompressionCodec: codec, stage: stage}
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "mode-switch-log")
	cfg.LifecycleMode = oldMode
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.UsageCacheTTL = 10 * time.Millisecond
	cfg.FsstoreConfig.Lifecycle.MaxFileAge = 36500
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 10
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
	cfg.FsstoreConfig.Duration.CompressWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.CompressErrorThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = time.Hour
	cfg.FsstoreConfig.CompressionCodec = interrupted
	cfg.LifecycleRunnerSettings.Interval = time.Hour
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: t.TempDir(), SourcePrefix: "mode_switch_log", AutoRecoverMissingCheckpoint: false,
	}
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{
				Region: "us-east-1", Bucket: "logs", Prefix: "normal", Endpoint: server.URL,
				ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second,
			},
		},
	}
	// 最初の登録前に存在した世代を除外する仕様が、復旧試験を隠さないようにします。
	// 送信先は元ログが生成される前に登録し、両factoryに同じ永続記録を読み込ませます。
	directory, err := fsstore.GetFileSystemStoreDirectoryPath(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir)
	require.NoError(t, err)
	pattern := logpattern.FileNamePattern{Dir: directory, Target: cfg.FsstoreConfig.Layout.Target, Extension: LogFileExtension}
	sourceID := checkpoint.ArchiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(DefaultLogger), pattern)
	destination, err := s3uploader.ResolveDestinationID(cfg.ObjectStorageUploadConfig.Default.S3)
	require.NoError(t, err)
	store, err := checkpoint.NewFilesystemStore(cfg.ArchiveCheckpointConfig.Directory, sourceID)
	require.NoError(t, err)
	require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{destination}, nil))
	require.NoError(t, store.Close())

	old := newModeSwitchLogRuntime(t, cfg)
	source := fillActiveFileLoggerForRotation(t, old.writer)
	content, err := os.ReadFile(source)
	require.NoError(t, err)
	compressed := source + ".gz"
	pending := logpattern.CompressionSourceDeletePendingPath(source)
	interrupted.source = source
	// 中断点で終了を要求し、panic後のcleanupや再試行が残存状態を先に片付けるのを防ぎます。
	interrupted.beforePanic = func() {
		_ = old.writer.RequestForceClose()
		_ = old.writer.archiveLogFileProcessingRecovery.ForceClose()
		for _, runner := range old.runners {
			_ = runner.DirectoryLifecycle.ForceClose()
		}
	}
	reported := make(chan error, 4)
	old.writer.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		select {
		case reported <- err:
		default:
		}
	}
	// 通常writeで使うrotation処理で世代1を閉じ、世代2をactiveにします。
	// inlineならその非同期処理、runnerなら実RunOnceで同じcodecを呼びます。
	require.NoError(t, rotateFileLoggerForTest(ctx, old.writer))
	latest := old.writer.Fsstore.ActiveFilePath
	require.NotEqual(t, source, latest)
	if oldMode == pkgfsstore.FileLifecycleModeInline {
		err := receiveForTest(t, reported, "inline compression panic was not reported")
		requireRotationArchivePanicReported(t, err, "mode-switch compression interruption")
	} else {
		require.PanicsWithValue(t, "mode-switch compression interruption", func() { _, _ = old.runners[0].RunOnce(ctx) })
	}
	require.EqualValues(t, 1, interrupted.compressCalls.Load())
	require.Zero(t, requests.Load())
	old.forceClose(t)
	if stage == "partial-temporary" {
		// 通常panicのdeferはtmpを削除します。強制process中断でdeferが動かなかった
		// 場合の残骸も追加し、再起動後に完成済みファイルと誤認しないことを確認します。
		require.NoError(t, os.WriteFile(compressed+".tmp", []byte("incomplete gzip"), 0o600))
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

	newMode := pkgfsstore.FileLifecycleModeInline
	if oldMode == newMode {
		newMode = pkgfsstore.FileLifecycleModeRunner
	}
	resumedCodec := &inlineRecoveryCompressionCodec{}
	cfg.LifecycleMode, cfg.FsstoreConfig.CompressionCodec = newMode, resumedCodec
	resumed := newModeSwitchLogRuntime(t, cfg)
	require.Equal(t, sourceID, checkpoint.ArchiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(DefaultLogger), resumed.writer.Fsstore.FileName))
	require.NotEqual(t, source, resumed.writer.Fsstore.ActiveFilePath)
	for range 3 {
		resumed.maintain(t)
	}
	resumed.close(t)
	require.NoFileExists(t, source)
	require.NoFileExists(t, compressed)
	require.NoFileExists(t, pending)
	require.NoFileExists(t, compressed+".tmp")
	require.FileExists(t, latest)
	require.EqualValues(t, 1, requests.Load())
	if stage == "partial-temporary" {
		require.EqualValues(t, 1, resumedCodec.compressCalls.Load())
	} else {
		require.Zero(t, resumedCodec.compressCalls.Load(), "完成済み圧縮ファイルを再圧縮しない")
	}
	payloadMu.Lock()
	gotPayload, gotPath := append([]byte(nil), uploaded...), objectPath
	payloadMu.Unlock()
	require.Equal(t, "/logs/normal/"+filepath.Base(compressed), gotPath)
	reader, err := gzip.NewReader(bytes.NewReader(gotPayload))
	require.NoError(t, err)
	decoded, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, content, decoded, "中断した元ログの内容を欠落なく送信する")

	// 完了後にファイルが再出現しても、元modeで保存済みcursorを読み直し、再送を防ぎます。
	require.NoError(t, os.WriteFile(compressed, gotPayload, 0o600))
	cfg.LifecycleMode = oldMode
	verified := newModeSwitchLogRuntime(t, cfg)
	for range 2 {
		verified.maintain(t)
	}
	verified.close(t)
	require.NoFileExists(t, compressed)
	require.EqualValues(t, 1, requests.Load())
	store, err = checkpoint.NewFilesystemStore(cfg.ArchiveCheckpointConfig.Directory, sourceID)
	require.NoError(t, err)
	key, err := checkpoint.ArchiveCheckpointGenerationKeyForPath(compressed, pattern)
	require.NoError(t, err)
	completed, err := store.Load(ctx, checkpoint.CheckpointKey{Generation: key, Compressed: true})
	require.NoError(t, err)
	require.True(t, completed.DestinationComplete(destination))
	require.NoError(t, store.Close())
}

type modeSwitchLogCodec struct {
	pkgfsstore.CompressionCodec
	stage         string
	source        string
	compressCalls atomic.Int32
	beforePanic   func()
}

func (c *modeSwitchLogCodec) Compress(source, destination *os.File) error {
	c.compressCalls.Add(1)
	if c.stage == "partial-temporary" {
		if _, err := destination.Write([]byte("unfinished gzip")); err != nil {
			return err
		}
		if c.beforePanic != nil {
			c.beforePanic()
		}
		panic("mode-switch compression interruption")
	}
	return c.CompressionCodec.Compress(source, destination)
}

func (c *modeSwitchLogCodec) VerifyCompressedIntegrity(file *os.File) error {
	if err := c.CompressionCodec.VerifyCompressedIntegrity(file); err != nil {
		return err
	}
	// 検証済みtmpから、final公開後／元ログのrename後に中断した配置を再現します。
	// 実際の圧縮を呼んだ旧所有者は、この試験境界のpanicで停止します。
	payload, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}
	if err := os.WriteFile(c.source+".gz", payload, 0o600); err != nil {
		return err
	}
	if c.stage == "source-delete-pending" {
		if err := os.Rename(c.source, logpattern.CompressionSourceDeletePendingPath(c.source)); err != nil {
			return err
		}
	}
	if c.beforePanic != nil {
		c.beforePanic()
	}
	panic("mode-switch compression interruption")
}

type modeSwitchLogRuntime struct {
	writer  *FileLogger
	runners []*logfilerunner.Runner
	closed  bool
}

func newModeSwitchLogRuntime(t *testing.T, cfg pkglogger.FileLoggerConfig) *modeSwitchLogRuntime {
	t.Helper()
	writer, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	period, err := time.Parse(pkgfsstore.FileDateFormat, writer.Fsstore.FileName.Date)
	require.NoError(t, err)
	writer.Clock = clock.NewFixedClock(period.Add(12 * time.Hour))
	runtime := &modeSwitchLogRuntime{writer: writer}
	t.Cleanup(func() { runtime.forceClose(t) })
	if cfg.LifecycleMode == pkgfsstore.FileLifecycleModeRunner {
		runtime.runners, err = logfilerunner.NewLogFileRunners(logfilelifecycle.LogFileOptions{
			Config: pkgfsstore.LogFileLifecycleRunnerConfig{
				FsstoreConfig: cfg.FsstoreConfig, ObjectStorageUploadConfig: cfg.ObjectStorageUploadConfig,
				ArchiveCheckpointConfig: cfg.ArchiveCheckpointConfig, RunnerConfig: cfg.LifecycleRunnerSettings,
			},
			Clock: writer.Clock,
		}, nil)
		require.NoError(t, err)
		require.Len(t, runtime.runners, 1)
	}
	return runtime
}

func (r *modeSwitchLogRuntime) maintain(t *testing.T) {
	t.Helper()
	if len(r.runners) > 0 {
		_, err := r.runners[0].RunOnce(context.Background())
		require.NoError(t, err)
		return
	}
	r.writer.Clock = clock.NewFixedClock(r.writer.Clock.Now().Add(r.writer.config.Fsstore().Purge().UsageCacheTTL()))
	require.NoError(t, r.writer.testInfo(context.Background(), "resume lifecycle"))
	waitBackgroundForTest(t, r.writer)
}

func (r *modeSwitchLogRuntime) forceClose(t *testing.T) {
	t.Helper()
	if r.closed {
		return
	}
	for _, runner := range r.runners {
		_ = runner.DirectoryLifecycle.ForceClose()
		_ = runner.DirectoryLifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
	}
	_ = r.writer.RequestForceClose()
	err := closeBoundedForTest(t, boundedwait.HangTimeout, "r.writer Close", r.writer)
	require.ErrorIs(t, err, shutdown.ErrForcedClose)
	receiveForTest(t, r.writer.ShutdownDone(), "file logger shutdown worker completion")
	require.True(t, r.writer.ShutdownComplete())
	r.closed = true
}

func (r *modeSwitchLogRuntime) close(t *testing.T) {
	t.Helper()
	for _, runner := range r.runners {
		require.NoError(t, runner.DirectoryLifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes())
	}
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "r.writer Close", r.writer))
	receiveForTest(t, r.writer.ShutdownDone(), "file logger shutdown worker completion")
	r.closed = true
}
