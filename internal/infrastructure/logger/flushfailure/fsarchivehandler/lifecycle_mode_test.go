package fsarchivehandler

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFailureArchiveLifecycleModesOwnExactlyOnePostRotationPath(t *testing.T) {
	tests := []struct {
		name               string
		mode               pkgfsstore.FileLifecycleMode
		wantInlineUploader bool
		wantInlineTrackers bool
		wantRunner         bool
	}{
		{
			name:               "inline",
			mode:               pkgfsstore.FileLifecycleModeInline,
			wantInlineUploader: true,
			wantInlineTrackers: true,
		},
		{
			name:       "runner",
			mode:       pkgfsstore.FileLifecycleModeRunner,
			wantRunner: true,
		},
		{
			name: "disable",
			mode: pkgfsstore.FileLifecycleModeDisable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = tt.mode
				cfg.objectStorageUpload = testFailureArchiveUploadConfig()
				cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
					Directory:    filepath.Join(t.TempDir(), "archive-checkpoints"),
					SourcePrefix: "failure_archive_lifecycle_mode_test",
				}
			})

			hasInlineUploaders := handler.handler.retryableArchiveUploader != nil &&
				handler.handler.permanentArchiveUploader != nil
			if hasInlineUploaders != tt.wantInlineUploader {
				t.Fatalf(
					"inline uploaders enabled = %t, want %t",
					hasInlineUploaders,
					tt.wantInlineUploader,
				)
			}
			if got := handler.runnerRuntime != nil; got != tt.wantRunner {
				t.Fatalf("runner runtime enabled = %t, want %t", got, tt.wantRunner)
			}
			for failureCategory, directory := range map[failurearchive.FailureCategory]*archiveDirectoryRuntime{
				failurearchive.FailureCategoryRetryable: &handler.handler.retryableDirectory,
				failurearchive.FailureCategoryPermanent: &handler.handler.permanentDirectory,
			} {
				hasInlineTracker := directory.directorySizeTracker != nil
				if hasInlineTracker != tt.wantInlineTrackers {
					t.Fatalf(
						"%s inline directory tracker enabled=%t, want %t",
						failureCategory,
						hasInlineTracker,
						tt.wantInlineTrackers,
					)
				}
			}
			if tt.wantRunner && len(handler.runnerRuntime.runners) != 2 {
				t.Fatalf(
					"runner count = %d, want retryable and permanent runners",
					len(handler.runnerRuntime.runners),
				)
			}
			if tt.wantRunner {
				for index, runner := range handler.runnerRuntime.runners {
					if _, ok := runner.Reporter.(*failureArchiveLifecycleLogReporter); !ok {
						t.Fatalf(
							"runner[%d] Reporter = %T, want *failureArchiveLifecycleLogReporter",
							index,
							runner.Reporter,
						)
					}
				}
			}
		})
	}
}

func TestFailureArchivePipelineOwnsMatchingWriterRunnerRuntimeAndLease(t *testing.T) {
	handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
	})
	if handler.handler == nil {
		t.Fatal("failure archive writer is nil")
	}
	if handler.runnerRuntime == nil {
		t.Fatal("failure archive runner runtime is nil")
	}
	if handler.archiveRootLease == nil {
		t.Fatal("failure archive root Lease is nil")
	}
	if len(handler.runnerRuntime.runners) != 2 {
		t.Fatalf(
			"runner count = %d, want retryable and permanent runners",
			len(handler.runnerRuntime.runners),
		)
	}

	tests := []struct {
		failureCategory failurearchive.FailureCategory
		terminalDir     string
		writerDir       string
		activePath      string
		hasFile         bool
		directory       *archiveDirectoryRuntime
		runner          *logfilerunner.Runner
	}{
		{
			failureCategory: failurearchive.FailureCategoryRetryable,
			terminalDir:     handler.handler.Config.DirCaseRetryable(),
			writerDir:       handler.handler.Fsstore.FileNameRetryable.Dir,
			activePath:      handler.handler.Fsstore.ActiveFilePathRetryable,
			hasFile:         handler.handler.Fsstore.FileRetryable != nil,
			directory:       &handler.handler.retryableDirectory,
			runner:          handler.runnerRuntime.runners[0],
		},
		{
			failureCategory: failurearchive.FailureCategoryPermanent,
			terminalDir:     handler.handler.Config.DirCasePermanent(),
			writerDir:       handler.handler.Fsstore.FileNamePermanent.Dir,
			activePath:      handler.handler.Fsstore.ActiveFilePathPermanent,
			hasFile:         handler.handler.Fsstore.FilePermanent != nil,
			directory:       &handler.handler.permanentDirectory,
			runner:          handler.runnerRuntime.runners[1],
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.failureCategory), func(t *testing.T) {
			if !tt.hasFile {
				t.Fatal("active failure archive file is nil")
			}
			if tt.directory.guard == nil || tt.directory.token == nil {
				t.Fatal("writer directory guard or token is nil")
			}

			runnerDir, err := tt.runner.DirectoryPath()
			if err != nil {
				t.Fatalf("runner DirectoryPath() error = %v", err)
			}
			configuredDir, err := failurearchive.GetFileArchiveDirectoryPath(
				handler.handler.Config,
				tt.terminalDir,
			)
			if err != nil {
				t.Fatalf("GetFileArchiveDirectoryPath() error = %v", err)
			}
			wantDir := pathidentity.AbsoluteOrCleanPath(configuredDir)
			for source, path := range map[string]string{
				"writer":      tt.writerDir,
				"active file": filepath.Dir(tt.activePath),
				"runner":      runnerDir,
			} {
				if got := pathidentity.AbsoluteOrCleanPath(path); got != wantDir {
					t.Fatalf("%s directory = %q, want %q", source, got, wantDir)
				}
			}
			if shared := appendlog.DirectoryGuardForPath(runnerDir); shared != tt.directory.guard {
				t.Fatalf(
					"runner DirectoryGuard = %p, writer DirectoryGuard = %p",
					shared,
					tt.directory.guard,
				)
			}

			reporter, ok := tt.runner.Reporter.(*failureArchiveLifecycleLogReporter)
			if !ok {
				t.Fatalf(
					"runner Reporter = %T, want *failureArchiveLifecycleLogReporter",
					tt.runner.Reporter,
				)
			}
			if reporter.handler != handler.handler {
				t.Fatal("lifecycle reporter does not reference the pipeline writer")
			}

			conflicting, err := directorylease.Acquire(
				directorylease.ScopeLabel("test.failure_archive_pipeline."+string(tt.failureCategory)),
				runnerDir,
			)
			if conflicting != nil {
				conflicting.Release()
				t.Fatal("runner directory was acquired while archive root Lease is active")
			}
			if !errors.Is(err, directorylease.ErrDirectoryLeaseConflict) {
				t.Fatalf("Acquire() error = %v, want ErrDirectoryLeaseConflict", err)
			}
			var conflict *directorylease.ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("Acquire() error type = %T, want *ConflictError", err)
			}
			if conflict.ExistingLeaseID != handler.archiveRootLease.ID() {
				t.Fatalf(
					"conflicting Lease ID = %d, want archive root Lease %d",
					conflict.ExistingLeaseID,
					handler.archiveRootLease.ID(),
				)
			}
		})
	}
}

func TestFailureArchiveRunnerAndDisableModesSkipInlineCleanup(t *testing.T) {
	tests := []struct {
		name        string
		mode        pkgfsstore.FileLifecycleMode
		wantCleanup bool
	}{
		{
			name:        "inline",
			mode:        pkgfsstore.FileLifecycleModeInline,
			wantCleanup: true,
		},
		{
			name: "runner",
			mode: pkgfsstore.FileLifecycleModeRunner,
		},
		{
			name: "disable",
			mode: pkgfsstore.FileLifecycleModeDisable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = tt.mode
			})

			if err := handler.HandleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
				t.Fatalf("HandleFailure() error = %v", err)
			}
			cleanupRan := !handler.handler.retryableDirectory.lastAgePurgeAt.IsZero() ||
				!handler.handler.permanentDirectory.lastAgePurgeAt.IsZero()
			if cleanupRan != tt.wantCleanup {
				t.Fatalf("inline cleanup ran = %t, want %t", cleanupRan, tt.wantCleanup)
			}
		})
	}
}

// runner・disableでは、rotation後のfileをinline処理へ渡さず、圧縮も送信もtaskも始めません。
// 圧縮とuploadを有効にした設定でも、inline用の処理はwriterのrotationから起動しません。
func TestFailureArchiveNonInlineModesDoNotDispatchArchiveTask(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{
		pkgfsstore.FileLifecycleModeRunner,
		pkgfsstore.FileLifecycleModeDisable,
	} {
		t.Run(string(mode), func(t *testing.T) {
			recorder := newRecordingFailureArchiveUploader(nil, nil)
			pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = mode
				cfg.enableFileCompress = true
				cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
			})
			handler := pipeline.handler
			// runner・disableではinline用のuploaderを構築しないため、uploadを指定しても保持しません。
			if got := handler.archiveUploader(failurearchive.FailureCategoryRetryable); got != nil {
				t.Fatalf("non-inline mode holds inline archive uploader %T", got)
			}

			oldPath := canonicalArchivePathForTest(t, handler, failurearchive.FailureCategoryRetryable, "20260913", 1)
			const contents = "retryable"
			if err := os.WriteFile(oldPath, []byte(contents), 0o600); err != nil {
				t.Fatalf("write rotated archive: %v", err)
			}
			handler.archiveTaskMu.Lock()
			previousTaskGeneration := handler.archiveTaskGeneration
			handler.archiveTaskMu.Unlock()
			handler.dispatchArchiveRotatedFile(
				context.Background(),
				handler.Fsstore.ActiveFilePathRetryable,
				oldPath,
				failurearchive.FailureCategoryRetryable,
			)

			// taskを開始していないため、この後に圧縮やuploadが非同期に始まることもありません。
			handler.archiveTaskMu.Lock()
			taskCount := handler.archiveTaskCount
			taskGeneration := handler.archiveTaskGeneration
			handler.archiveTaskMu.Unlock()
			if taskCount != 0 || taskGeneration != previousTaskGeneration {
				t.Fatalf("archive task count = %d, generation changed = %t; want no inline task", taskCount, taskGeneration != previousTaskGeneration)
			}
			if actual, err := os.ReadFile(oldPath); err != nil || string(actual) != contents {
				t.Fatalf("rotated archive = %q, %v; want unchanged %q", actual, err, contents)
			}
			if _, err := os.Stat(oldPath + pkgfsstore.ExtensionCompressGzip); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("compressed archive stat error = %v, want not exist", err)
			}
			select {
			case input := <-recorder.inputs:
				t.Fatalf("non-inline mode dispatched upload: %+v", input)
			default:
			}
			assertArchivePathProtected(t, handler.retryableDirectory.guard, oldPath, false)
		})
	}
}

func TestFailureArchiveRunnerModeSharesActivePathGuardWithWriter(t *testing.T) {
	handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
	})
	retryablePath := handler.handler.Fsstore.ActiveFilePathRetryable
	permanentPath := handler.handler.Fsstore.ActiveFilePathPermanent

	assertArchivePathProtected(t, handler.handler.retryableDirectory.guard, retryablePath, true)
	assertArchivePathProtected(t, handler.handler.permanentDirectory.guard, permanentPath, true)

	if err := shutdownFailureArchiveForTest(handler); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	assertArchivePathProtected(t, handler.handler.retryableDirectory.guard, retryablePath, false)
	assertArchivePathProtected(t, handler.handler.permanentDirectory.guard, permanentPath, false)
}

// 設定は構築後に変更できないため、lifecycle modeの変更はShutdown後の再構築で行います。
// Shutdownは所有するrunnerを停止してarchive rootのLeaseを解放し、別modeのPipelineを同じrootへ構築できます。
func TestFailureArchiveShutdownStopsOwnedRunnerBeforeRestartWithAnotherMode(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
	})
	runtime := pipeline.runnerRuntime
	if runtime == nil || runtime.done == nil {
		t.Fatal("runner runtime was not started")
	}

	if err := shutdownFailureArchiveForTest(pipeline); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case <-runtime.done:
	default:
		t.Fatal("owned runner remained active after Shutdown")
	}
	if pipeline.archiveRootLease != nil {
		t.Fatal("archive root lease remains after owned runner shutdown")
	}

	restarted := restartTestArchiveHandler(t, pipeline, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	})
	if restarted.runnerRuntime != nil && restarted.runnerRuntime.done != nil {
		t.Fatal("inline restart started a lifecycle runner")
	}
	if !restarted.handler.inlineLifecycleEnabled() {
		t.Fatal("restarted pipeline does not use the inline lifecycle")
	}
}

func TestFailureArchiveRotationCloseErrorUsesNewActiveFile(t *testing.T) {
	handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
		cfg.enableRotate = true
	})
	oldPath := handler.handler.Fsstore.ActiveFilePathRetryable
	oldFile := handler.handler.Fsstore.FileRetryable
	if _, err := oldFile.Write(make([]byte, handler.handler.Config.MaxFileSize())); err != nil {
		t.Fatalf("grow retryable archive to rotation threshold: %v", err)
	}
	if err := oldFile.Close(); err != nil {
		t.Fatalf("pre-close retryable archive: %v", err)
	}

	rotation, err := handler.handler.rotateRetryable(
		context.Background(),
		handler.handler.Clock.Now(),
	)
	if err != nil {
		t.Fatalf("rotateRetryable() error = %v, want successful active transition", err)
	}
	if rotation.PreviousFileCloseErr == nil {
		t.Fatal("PreviousFileCloseErr = nil, want close error from pre-closed file")
	}
	if rotation.OldFilePath != oldPath {
		t.Fatalf("OldFilePath = %q, want %q", rotation.OldFilePath, oldPath)
	}
	newPath := handler.handler.Fsstore.ActiveFilePathRetryable
	if newPath == oldPath {
		t.Fatalf("active retryable path remained %q after successful transition", oldPath)
	}
	if handler.handler.Fsstore.FileRetryable == nil || handler.handler.Fsstore.FileRetryable == oldFile {
		t.Fatal("new active file handle was not installed after previous close error")
	}
	assertArchivePathProtected(t, handler.handler.retryableDirectory.guard, oldPath, false)
	assertArchivePathProtected(t, handler.handler.retryableDirectory.guard, newPath, true)

	const message = "record-written-before-previous-close-notification"
	var delegatedErr error
	notificationObservedWrite := false
	handler.handler.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		delegatedErr = err
		content, readErr := os.ReadFile(newPath)
		notificationObservedWrite = readErr == nil && bytes.Contains(content, []byte(message))
	}
	handler.handler.Fsstore.Mu.Lock()
	archiveErr := handler.handler.archiveLocked(
		context.Background(),
		failureArchiveResultForFailureCategory(failurearchive.FailureCategoryRetryable, message),
	)
	handler.handler.Fsstore.Mu.Unlock()
	if archiveErr != nil {
		t.Fatalf("archive to new active file: %v", archiveErr)
	}
	handler.handler.reportPreviousFileCloseFailures(
		context.Background(),
		[]previousArchiveFileCloseFailure{{
			failureCategory: failurearchive.FailureCategoryRetryable,
			path:            rotation.OldFilePath,
			err:             rotation.PreviousFileCloseErr,
		}},
	)
	if delegatedErr == nil {
		t.Fatal("OnError was not called for previous file close failure")
	}
	if !notificationObservedWrite {
		t.Fatal("OnError ran before the failure record was written to the new active file")
	}
}

func TestFailureArchiveRunnerShutdownDoesNotStartNewInactiveFileWork(t *testing.T) {
	now := time.Date(2026, time.September, 21, 12, 0, 0, 0, time.UTC)
	handler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
		cfg.lifecycleRunnerConfig.Interval = time.Hour
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
	})
	activePath := handler.handler.Fsstore.ActiveFilePathRetryable
	inactivePath := filepath.Join(
		filepath.Dir(activePath),
		"test-app_"+now.AddDate(0, 0, -1).Format("20060102")+"_1.jsonl",
	)
	if err := os.WriteFile(inactivePath, []byte("retryable"), 0o600); err != nil {
		t.Fatalf("write inactive archive: %v", err)
	}

	if err := shutdownFailureArchiveForTest(handler); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if _, err := os.Stat(inactivePath); err != nil {
		t.Fatalf("shutdown changed an inactive file without accepted work: %v", err)
	}
	if _, err := os.Stat(inactivePath + pkgfsstore.ExtensionCompressGzip); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shutdown started new compression: %v", err)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("shutdown-time active archive must remain uncompressed: %v", err)
	}
}

func TestFailureArchiveRunnerShutdownReturnsStoppedRunnerError(t *testing.T) {
	handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
	})
	failure := errors.New("accepted runner failed")
	handler.runnerRuntime.errMu.Lock()
	handler.runnerRuntime.errs = append(handler.runnerRuntime.errs, failure)
	handler.runnerRuntime.errMu.Unlock()
	if err := shutdownFailureArchiveForTest(handler); !errors.Is(err, failure) {
		t.Fatalf("Shutdown() error = %v, want accepted runner failure", err)
	}
	if handler.handler.Fsstore.FileRetryable != nil || handler.handler.Fsstore.FilePermanent != nil {
		t.Fatal("active files were not closed after accepted work drained")
	}
	if err := shutdownFailureArchiveForTest(handler); err != nil {
		t.Fatalf("retry Shutdown() error = %v", err)
	}
	if handler.shutdownState != shutdownStateCompleted {
		t.Fatal("Shutdown did not finish the lifecycle runtime")
	}
	if handler.archiveRootLease != nil {
		t.Fatal("archive root lease remains after accepted work drained")
	}
}

func TestFinishRotatedFileTransitionDoesNotAcquireLifecycleLockWhileFsstoreLocked(
	t *testing.T,
) {
	now := time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	handler := pipeline.handler
	failureCategory := failurearchive.FailureCategoryRetryable
	state := handler.directoryForFailureCategory(failureCategory)
	oldPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "rotated.jsonl")
	protection := archivePathProtection{
		token: appendlog.NewDirectoryGuardToken(),
		paths: handler.archiveProtectedPaths(oldPath),
	}
	state.guard.ProtectPaths(protection.token, protection.paths...)
	for _, path := range protection.paths {
		assertArchivePathProtected(t, state.guard, path, true)
	}

	lifecycleLocked := make(chan struct{})
	releaseLifecycle := make(chan struct{})
	lifecycleReleased := make(chan struct{})
	go func() {
		_ = state.guard.RunWithLifecycleLock(func() error {
			close(lifecycleLocked)
			<-releaseLifecycle
			return nil
		})
		close(lifecycleReleased)
	}()
	<-lifecycleLocked

	transitionFinished := make(chan struct{})
	go func() {
		handler.Fsstore.Mu.Lock()
		defer handler.Fsstore.Mu.Unlock()
		handler.finishRotatedFileTransition(
			context.Background(),
			handler.Fsstore.ActiveFilePathRetryable,
			oldPath,
			failureCategory,
			protection,
			nil,
		)
		close(transitionFinished)
	}()

	select {
	case <-transitionFinished:
		close(releaseLifecycle)
		<-lifecycleReleased
	case <-time.After(failureArchiveHangTimeout):
		close(releaseLifecycle)
		<-lifecycleReleased
		<-transitionFinished
		t.Fatal("finishRotatedFileTransition waited for lifecycle lock while holding Fsstore.Mu")
	}

	for _, path := range protection.paths {
		assertArchivePathProtected(t, state.guard, path, false)
	}
}

func assertArchivePathProtected(
	t *testing.T,
	guard *appendlog.DirectoryGuard,
	path string,
	want bool,
) {
	t.Helper()
	normalizedPath := pathidentity.AbsoluteOrCleanPath(path)
	got := false
	guard.WithProtectedPaths(func(paths map[string]struct{}) {
		_, got = paths[normalizedPath]
	})
	if got != want {
		t.Fatalf("path %q protected = %t, want %t", normalizedPath, got, want)
	}
}

func testFailureArchiveUploadConfig() objectstorage.UploadTargetConfig {
	return objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region:          "us-east-1",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Bucket:          "failure-archive",
			Prefix:          "backend",
		},
	}
}
