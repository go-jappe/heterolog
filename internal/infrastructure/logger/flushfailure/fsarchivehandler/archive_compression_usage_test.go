package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// 圧縮によるファイルの変更後は、rotation taskの完了処理が概算を無効化します。
// 同じtaskの後続cleanupが、TTL内の新しい概算でもディレクトリ全体を観測し直します。
func TestFailureArchiveCompressionInvalidatesDirectoryEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 9, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
	})
	handler := pipeline.handler
	failureCategory := failurearchive.FailureCategoryPermanent
	sourcePath := filepath.Join(handler.Fsstore.FileNamePermanent.Dir, "actual-compression.jsonl")
	if err := os.WriteFile(sourcePath, []byte("compressible failure archive payload"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	tracker := handler.directorySizeTracker(failureCategory)
	if !tracker.TryStartObservation(now, handler.Config, true) {
		t.Fatal("failed to initialize directory tracker")
	}
	tracker.FinishObservation(36)
	// 当日のage purgeを済ませ、概算の無効化だけが後続cleanupの観測理由になるようにします。
	handler.permanentDirectory.lastAgePurgeAt = now
	observations := countArchiveDirectoryObservations(handler)

	dispatchRotatedArchiveAndWait(
		t,
		handler,
		handler.Fsstore.ActiveFilePathPermanent,
		sourcePath,
		failureCategory,
	)
	if _, err := os.Stat(sourcePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(sourcePath + pkgfsstore.ExtensionCompressGzip); err != nil {
		t.Fatalf("stat compressed file: %v", err)
	}
	if got := observations(); got != 1 {
		t.Fatalf("directory observations after compression = %d, want 1 after invalidating the estimate", got)
	}
}

func TestFailureArchiveCompressionFailureAlsoInvalidatesDirectoryEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.compressionCodec = failingFailureArchiveCompressor{
			format: pkgfsstore.CompressGzip,
			err:    errors.New("compress failed"),
		}
	})
	handler := pipeline.handler
	failureCategory := failurearchive.FailureCategoryRetryable
	sourcePath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "failed-compression.jsonl")
	if err := os.WriteFile(sourcePath, []byte("source remains"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	tracker := handler.directorySizeTracker(failureCategory)
	if !tracker.TryStartObservation(now, handler.Config, true) {
		t.Fatal("failed to initialize directory tracker")
	}
	tracker.FinishObservation(14)
	handler.retryableDirectory.lastAgePurgeAt = now
	observations := countArchiveDirectoryObservations(handler)

	// 圧縮失敗やpanicでも一時ファイルが作られる場合があるため、同様に観測し直します。
	dispatchRotatedArchiveAndWait(
		t,
		handler,
		handler.Fsstore.ActiveFilePathRetryable,
		sourcePath,
		failureCategory,
	)
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("source file should remain: %v", err)
	}
	if got := observations(); got != 1 {
		t.Fatalf("directory observations after failed compression = %d, want 1 after invalidating the estimate", got)
	}
}

// panic時は圧縮ファイルの作成済み・未作成を確定できないため、rotation taskが
// 対応するuploaderの系列だけに再走査を予約します。
func TestCompressedArchivePanicArmsMatchingUploadRetrySchedule(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 30, 0, 0, time.UTC)
	const panicValue = "compression codec panic"
	reported := make(chan error, 1)
	failureCategory := failurearchive.FailureCategoryRetryable
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.compressionCodec = panicFailureArchiveCompressionCodec{
			format:     pkgfsstore.CompressGzip,
			panicValue: panicValue,
		}
		cfg.setArchiveUploader(failureCategory, &sequencedFailureArchiveUploader{})
	}, rotationPanicReportHandlers(reported))
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	if !state.uploadRetry.claimForAttempt(now, testUsageCacheTTL) {
		t.Fatal("failed to consume construction bootstrap schedule")
	}

	sourcePath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260828_1.jsonl",
	)
	if err := os.WriteFile(sourcePath, []byte("source remains"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	dispatchRotatedArchiveAndWait(
		t,
		handler,
		handler.Fsstore.ActiveFilePathRetryable,
		sourcePath,
		failureCategory,
	)
	requireRotationPanicReported(t, reported, panicValue)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
	if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
		t.Fatalf(
			"retry schedule after compression panic = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(testUsageCacheTTL),
		)
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(
		&handler.permanentDirectory.uploadRetry,
	); scheduled {
		t.Fatal("retryable compression panic armed permanent upload retry schedule")
	}
}

func TestCompressedArchivePanicDoesNotArmWithoutMatchingUploader(t *testing.T) {
	now := time.Date(2026, time.August, 29, 11, 0, 0, 0, time.UTC)
	const panicValue = "permanent compression codec panic"
	reported := make(chan error, 1)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.compressionCodec = panicFailureArchiveCompressionCodec{
			format:     pkgfsstore.CompressGzip,
			panicValue: panicValue,
		}
		// 別failure categoryのuploaderは、このpermanent archiveのretry条件になりません。
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, &sequencedFailureArchiveUploader{})
	}, rotationPanicReportHandlers(reported))
	handler := pipeline.handler
	if !handler.retryableDirectory.uploadRetry.claimForAttempt(now, testUsageCacheTTL) {
		t.Fatal("failed to consume retryable bootstrap schedule")
	}

	sourcePath := filepath.Join(
		handler.Fsstore.FileNamePermanent.Dir,
		"test-app_20260828_1.jsonl",
	)
	if err := os.WriteFile(sourcePath, []byte("source remains"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	dispatchRotatedArchiveAndWait(
		t,
		handler,
		handler.Fsstore.ActiveFilePathPermanent,
		sourcePath,
		failurearchive.FailureCategoryPermanent,
	)
	requireRotationPanicReported(t, reported, panicValue)
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(
		&handler.permanentDirectory.uploadRetry,
	); scheduled {
		t.Fatal("compression panic without matching uploader armed retry schedule")
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(
		&handler.retryableDirectory.uploadRetry,
	); scheduled {
		t.Fatal("permanent compression panic armed retryable upload retry schedule")
	}
}

// 未圧縮fileの送信がpanicしても、送信待ちは完了cursorで管理されます。受理済みの再開処理が
// 世代を保護して引き継ぎ、rawは送信後と同じくlocalに残します。完了cursorから送信待ちの
// rawを再発見できるため、この系列のtarget-only再試行も予約します。
func TestRawArchiveUploadPanicHandsOffRawGenerationToRecovery(t *testing.T) {
	now := time.Date(2026, time.August, 29, 11, 30, 0, 0, time.UTC)
	const panicValue = "raw archive uploader panic"
	reported := make(chan error, 1)
	failureCategory := failurearchive.FailureCategoryRetryable
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, panicFailureArchiveScheduleUploader{panicValue: panicValue})
	}, rotationPanicReportHandlers(reported))
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	if !state.uploadRetry.claimForAttempt(now, testUsageCacheTTL) {
		t.Fatal("failed to consume construction bootstrap schedule")
	}

	sourcePath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260828_1.jsonl",
	)
	if err := os.WriteFile(sourcePath, []byte("raw archive"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	dispatchRotatedArchiveAndWait(
		t,
		handler,
		handler.Fsstore.ActiveFilePathRetryable,
		sourcePath,
		failureCategory,
	)
	requireRotationPanicReported(t, reported, panicValue)
	key := commoncheckpoint.CheckpointKey{Generation: commoncheckpoint.ArchiveCheckpointGenerationKey("20260828", 1)}
	if !handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(failureCategory, key)) {
		t.Fatal("raw upload panic was not handed off to log file processing recovery")
	}
	assertArchivePathProtected(t, state.guard, sourcePath, true)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
	if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
		t.Fatalf(
			"retry schedule after raw upload panic = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(testUsageCacheTTL),
		)
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(&handler.permanentDirectory.uploadRetry); scheduled {
		t.Fatal("retryable raw upload panic armed permanent upload retry schedule")
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("raw archive should remain after upload panic: %v", err)
	}
}

type panicFailureArchiveCompressionCodec struct {
	format     pkgfsstore.CompressFormat
	panicValue any
}

func (c panicFailureArchiveCompressionCodec) Format() pkgfsstore.CompressFormat {
	return c.format
}

func (c panicFailureArchiveCompressionCodec) Compress(*os.File, *os.File) error {
	panic(c.panicValue)
}

func (panicFailureArchiveCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return nil
}

type panicFailureArchiveScheduleUploader struct {
	panicValue any
}

func (u panicFailureArchiveScheduleUploader) Upload(
	context.Context,
	uploader.UploadRequest,
) error {
	panic(u.panicValue)
}

func captureFailureArchivePanic(call func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	call()
	return nil
}
