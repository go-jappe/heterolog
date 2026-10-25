package file

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// generationReuseObjectStorageは、実際のS3 uploaderが送ったobjectのpathと伸長後の内容を記録します。
type generationReuseObjectStorage struct {
	mu      sync.Mutex
	paths   []string
	content []string
	server  *httptest.Server
}

func newGenerationReuseObjectStorage(t *testing.T) *generationReuseObjectStorage {
	t.Helper()
	storage := &generationReuseObjectStorage{}
	storage.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		decoded := ""
		if reader, err := gzip.NewReader(bytes.NewReader(body)); err == nil {
			plain, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr == nil {
				decoded = string(plain)
			}
		}
		storage.mu.Lock()
		storage.paths = append(storage.paths, r.URL.Path)
		storage.content = append(storage.content, decoded)
		storage.mu.Unlock()
		w.Header().Set("ETag", `"generation-reuse"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(storage.server.Close)
	return storage
}

// uploadedNamesは送信順のobject名です。同じ名前が二度現れればobjectを上書きしています。
func (s *generationReuseObjectStorage) uploadedNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.paths))
	for _, objectPath := range s.paths {
		names = append(names, path.Base(objectPath))
	}
	return names
}

// contentOfは、object名nameで送信された内容を返します。
func (s *generationReuseObjectStorage) contentOf(name string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var contents []string
	for index, objectPath := range s.paths {
		if path.Base(objectPath) == name {
			contents = append(contents, s.content[index])
		}
	}
	return contents
}

func (s *generationReuseObjectStorage) uploaded(marker string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, content := range s.content {
		if strings.Contains(content, marker) {
			return true
		}
	}
	return false
}

func generationReuseConfig(t *testing.T, root, checkpointDirectory, endpoint string, mode pkgfsstore.FileLifecycleMode) pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := newTestFileLoggerConfig(root, "logs", "generation-reuse")
	cfg.LifecycleMode = mode
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	cfg.FsstoreConfig.Lifecycle.UsageCacheTTL = 10 * time.Millisecond
	cfg.FsstoreConfig.Lifecycle.MaxFileAge = 36500
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 10
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
	cfg.FsstoreConfig.Duration.CompressWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.CompressErrorThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = time.Hour
	cfg.LifecycleRunnerSettings.Interval = time.Hour
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{Directory: checkpointDirectory, SourcePrefix: "generation_reuse"}
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{
				Region: "us-east-1", Bucket: "logs", Prefix: "normal", Endpoint: endpoint,
				ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second,
			},
		},
	}
	return cfg
}

func generationReuseLifecycleOptions(cfg pkglogger.FileLoggerConfig, clk clock.Clock) logfilelifecycle.LogFileOptions {
	return logfilelifecycle.LogFileOptions{
		Config: pkgfsstore.LogFileLifecycleRunnerConfig{
			FsstoreConfig: cfg.FsstoreConfig, ObjectStorageUploadConfig: cfg.ObjectStorageUploadConfig,
			ArchiveCheckpointConfig: cfg.ArchiveCheckpointConfig, RunnerConfig: cfg.LifecycleRunnerSettings,
		},
		Clock: clk,
	}
}

// generationReuseRuntimeは、アプリ起動と同じconstructorで作ったwriterと、runner modeのlifecycleです。
type generationReuseRuntime struct {
	writer  *FileLogger
	runners []*logfilerunner.Runner
	closed  bool
}

// startGenerationReuseRuntimeはwriterを構築した後、日付境界をまたがないようwriterの時計を
// 起動時の期間の正午へ固定します。runner modeでは同じ時計のlifecycleも構築します。
func startGenerationReuseRuntime(t *testing.T, cfg pkglogger.FileLoggerConfig) *generationReuseRuntime {
	t.Helper()
	writer, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	writer.Clock = clock.NewFixedClock(generationReuseNoon(t, writer.Fsstore.FileName.Date))
	runtime := &generationReuseRuntime{writer: writer}
	t.Cleanup(func() { runtime.close(t) })
	if cfg.LifecycleMode == pkgfsstore.FileLifecycleModeRunner {
		runtime.runners, err = logfilerunner.NewLogFileRunners(generationReuseLifecycleOptions(cfg, writer.Clock), nil)
		require.NoError(t, err)
		require.Len(t, runtime.runners, 1)
	}
	return runtime
}

func generationReuseNoon(t *testing.T, period string) time.Time {
	t.Helper()
	day, err := time.Parse(pkgfsstore.FileDateFormat, period)
	require.NoError(t, err)
	return day.Add(12 * time.Hour)
}

// settleはinlineならrotation後の圧縮・送信の完了を待ち、runnerなら一周期を実行します。
func (r *generationReuseRuntime) settle(t *testing.T) {
	t.Helper()
	if len(r.runners) > 0 {
		_, err := r.runners[0].RunOnce(context.Background())
		require.NoError(t, err)
		return
	}
	waitBackgroundForTest(t, r.writer)
}

// rotateWithは、active fileを上限まで埋めてからmessageを書き、size rotationを起こします。
func (r *generationReuseRuntime) rotateWith(t *testing.T, message string) {
	t.Helper()
	fillActiveFileLoggerForRotation(t, r.writer)
	require.NoError(t, r.writer.testInfo(context.Background(), message))
	r.settle(t)
}

func (r *generationReuseRuntime) close(t *testing.T) {
	t.Helper()
	if r.closed {
		return
	}
	r.closed = true
	for _, runner := range r.runners {
		require.NoError(t, runner.DirectoryLifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes())
	}
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "r.writer Close", r.writer))
	receiveForTest(t, r.writer.ShutdownDone(), "file logger shutdown worker completion")
}

// recoverGenerationReuseLogsは、停止中のアプリに対する手動復旧CLIと同じ処理で、
// 最新fileを含む全fileを送信・削除します。
func recoverGenerationReuseLogs(t *testing.T, cfg pkglogger.FileLoggerConfig, at time.Time) {
	t.Helper()
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.ArchiveCheckpointConfig.Directory))
	lifecycles, err := logfilelifecycle.NewLogFileRecovery(generationReuseLifecycleOptions(cfg, clock.NewFixedClock(at)))
	require.NoError(t, err)
	for _, lifecycle := range lifecycles {
		_, runErr := lifecycle.RecoverOnce(context.Background())
		closeErr := lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
		receiveForTest(t, lifecycle.NotificationsDone(), "recovery lifecycle notifications")
		require.NoError(t, runErr)
		require.NoError(t, closeErr)
	}
}

func generationReuseDirectoryNames(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func requireUniqueObjectNames(t *testing.T, storage *generationReuseObjectStorage) {
	t.Helper()
	seen := make(map[string]struct{})
	for _, name := range storage.uploadedNames() {
		_, duplicate := seen[name]
		require.False(t, duplicate, "object %s must not be overwritten by another generation's content", name)
		seen[name] = struct{}{}
	}
}

func TestFileLoggerRestartAfterManualRecoveryContinuesAfterRecordedGenerations(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			storage := newGenerationReuseObjectStorage(t)
			cfg := generationReuseConfig(t, t.TempDir(), filepath.Join(t.TempDir(), "checkpoints"), storage.server.URL, mode)

			first := startGenerationReuseRuntime(t, cfg)
			period := first.writer.Fsstore.FileName.Date
			directory := first.writer.Fsstore.FileName.Dir
			require.NoError(t, first.writer.testInfo(ctx, "FIRST-RUN-GEN1"))
			first.rotateWith(t, "FIRST-RUN-GEN2")
			first.close(t)

			// 停止中の手動復旧は最新fileまで送信・削除し、directoryを空にします。
			recoverGenerationReuseLogs(t, cfg, generationReuseNoon(t, period))
			require.Empty(t, generationReuseDirectoryNames(t, directory))
			require.Equal(t, []string{
				"generation-reuse_" + period + "_1.log.gz", "generation-reuse_" + period + "_2.log.gz",
			}, storage.uploadedNames())

			// 同じ期間の再起動では、checkpointに記録済みの世代の後から書き込みます。
			second := startGenerationReuseRuntime(t, cfg)
			if second.writer.Fsstore.FileName.Date != period {
				t.Skip("the wall clock crossed a rotation boundary between the two application starts")
			}
			require.Equal(t, filepath.Join(directory, "generation-reuse_"+period+"_3.log"), second.writer.Fsstore.ActiveFilePath)
			require.NoError(t, second.writer.testInfo(ctx, "SECOND-RUN-GEN3"))
			second.rotateWith(t, "SECOND-RUN-GEN4")
			second.rotateWith(t, "SECOND-RUN-GEN5")
			second.close(t)

			// 新しい内容は記録済みの世代と別の名前で送信され、送信前に削除されません。
			require.Len(t, storage.contentOf("generation-reuse_"+period+"_3.log.gz"), 1)
			require.Contains(t, storage.contentOf("generation-reuse_" + period + "_3.log.gz")[0], "SECOND-RUN-GEN3")
			require.Len(t, storage.contentOf("generation-reuse_"+period+"_4.log.gz"), 1)
			require.Contains(t, storage.contentOf("generation-reuse_" + period + "_4.log.gz")[0], "SECOND-RUN-GEN4")
			for _, name := range []string{"generation-reuse_" + period + "_1.log.gz", "generation-reuse_" + period + "_2.log.gz"} {
				require.Len(t, storage.contentOf(name), 1)
				require.NotContains(t, storage.contentOf(name)[0], "SECOND-RUN")
			}
			requireUniqueObjectNames(t, storage)
			require.Equal(t, []string{"generation-reuse_" + period + "_5.log"}, generationReuseDirectoryNames(t, directory))
		})
	}
}

func TestFileLoggerRestartAfterClockStepBackContinuesRecordedPeriod(t *testing.T) {
	ctx := context.Background()
	storage := newGenerationReuseObjectStorage(t)
	cfg := generationReuseConfig(t, t.TempDir(), filepath.Join(t.TempDir(), "checkpoints"), storage.server.URL, pkgfsstore.FileLifecycleModeInline)

	// 一回目の起動中だけsystem clockが一日進んでいた状態を再現します。
	first := startGenerationReuseRuntime(t, cfg)
	period := first.writer.Fsstore.FileName.Date
	directory := first.writer.Fsstore.FileName.Dir
	ahead := generationReuseNoon(t, period).AddDate(0, 0, 1)
	aheadPeriod := ahead.Format(pkgfsstore.FileDateFormat)
	first.writer.Clock = clock.NewFixedClock(ahead)
	require.NoError(t, first.writer.testInfo(ctx, "FIRST-RUN-GEN1"))
	first.settle(t)
	first.rotateWith(t, "FIRST-RUN-GEN2")
	first.close(t)
	require.True(t, storage.uploaded("FIRST-RUN-GEN1"))

	// 時計を戻して再起動しても、送信済みの期間より前の期間へ戻らず、記録済みの世代の後を使います。
	second := startGenerationReuseRuntime(t, cfg)
	require.Equal(t, filepath.Join(directory, "generation-reuse_"+aheadPeriod+"_2.log"), second.writer.Fsstore.ActiveFilePath)
	second.writer.Clock = clock.NewFixedClock(generationReuseNoon(t, period))
	require.NoError(t, second.writer.testInfo(ctx, "CLOCKBACK-RUN-GEN2"))
	second.rotateWith(t, "CLOCKBACK-RUN-GEN3")
	second.close(t)

	contents := storage.contentOf("generation-reuse_" + aheadPeriod + "_2.log.gz")
	require.Len(t, contents, 1)
	require.Contains(t, contents[0], "FIRST-RUN-GEN2")
	require.Contains(t, contents[0], "CLOCKBACK-RUN-GEN2")
	requireUniqueObjectNames(t, storage)
	for _, name := range generationReuseDirectoryNames(t, directory) {
		require.NotContains(t, name, "_"+period+"_", "the restarted writer must not go back to an older period")
	}
	require.Equal(t, []string{"generation-reuse_" + aheadPeriod + "_3.log"}, generationReuseDirectoryNames(t, directory))
}

func TestLoggerSetRestartAfterManualRecoveryContinuesAfterRecordedGenerations(t *testing.T) {
	ctx := context.Background()
	storage := newGenerationReuseObjectStorage(t)
	checkpointDirectory := filepath.Join(t.TempDir(), "checkpoints")
	cfg := generationReuseConfig(t, t.TempDir(), checkpointDirectory, storage.server.URL, pkgfsstore.FileLifecycleModeInline)
	require.NoError(t, checkpointstore.PrepareDirectory(checkpointDirectory))

	firstSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	firstWriter, err := firstSet.NewFileLogger(ctx)
	require.NoError(t, err)
	period, directory := firstWriter.Fsstore.FileName.Date, firstWriter.Fsstore.FileName.Dir
	firstWriter.Clock = clock.NewFixedClock(generationReuseNoon(t, period))
	require.NoError(t, firstWriter.testInfo(ctx, "FIRST-RUN-GEN1"))
	fillActiveFileLoggerForRotation(t, firstWriter)
	require.NoError(t, firstWriter.testInfo(ctx, "FIRST-RUN-GEN2"))
	waitBackgroundForTest(t, firstWriter)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "firstSet Close", firstSet))

	recoverGenerationReuseLogs(t, cfg, generationReuseNoon(t, period))
	require.Empty(t, generationReuseDirectoryNames(t, directory))

	secondSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	secondWriter, err := secondSet.NewFileLogger(ctx)
	require.NoError(t, err)
	if secondWriter.Fsstore.FileName.Date != period {
		require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "secondSet Close", secondSet))
		t.Skip("the wall clock crossed a rotation boundary between the two application starts")
	}
	secondWriter.Clock = clock.NewFixedClock(generationReuseNoon(t, period))
	require.Equal(t, filepath.Join(directory, "generation-reuse_"+period+"_3.log"), secondWriter.Fsstore.ActiveFilePath)
	require.NoError(t, secondWriter.testInfo(ctx, "SECOND-RUN-GEN3"))
	fillActiveFileLoggerForRotation(t, secondWriter)
	require.NoError(t, secondWriter.testInfo(ctx, "SECOND-RUN-GEN4"))
	waitBackgroundForTest(t, secondWriter)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "secondSet Close", secondSet))

	contents := storage.contentOf("generation-reuse_" + period + "_3.log.gz")
	require.Len(t, contents, 1)
	require.Contains(t, contents[0], "SECOND-RUN-GEN3")
	requireUniqueObjectNames(t, storage)
	require.Equal(t, []string{"generation-reuse_" + period + "_4.log"}, generationReuseDirectoryNames(t, directory))
}

func generationReusePattern(directory, target string) logpattern.FileNamePattern {
	return logpattern.FileNamePattern{Dir: directory, Target: target, Extension: LogFileExtension}
}

func TestDeliveryFileLoggerStartsAfterRecordedGeneration(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := newTestDeliveryFileLoggerConfig(root, "logs", "delivery-generation")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	cfg.LifecycleRunnerSettings.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "delivery_generation",
	}
	directory := filepath.Join(root, "delivery-generation", "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	resolved, err := filepath.EvalSymlinks(directory)
	require.NoError(t, err)
	// runnerが送信済みとして記録した期間が、現在の時計より後にある状態です。
	recordedPeriod := time.Now().UTC().AddDate(0, 0, 10).Format(pkgfsstore.FileDateFormat)
	checkpointConfig := cfg.LifecycleRunnerSettings.ArchiveCheckpointConfig
	require.NoError(t, checkpointstore.PrepareDirectory(checkpointConfig.Directory))
	sourceID := checkpointstore.ArchiveCheckpointSourceID(checkpointConfig.SourcePrefix, string(DefaultLogger), generationReusePattern(resolved, "delivery-generation"))
	store, err := checkpointstore.NewFilesystemStore(checkpointConfig.Directory, sourceID)
	require.NoError(t, err)
	_, err = store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: recordedPeriod + "-4", Compressed: true}, []objectstorage.DestinationID{"archive"})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	writer, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "writer Close", func() closerForTest { return writer }, true)
	require.Equal(t, filepath.Join(resolved, "delivery-generation_"+recordedPeriod+"_5.log"), writer.Fsstore.ActiveFilePath)
}
