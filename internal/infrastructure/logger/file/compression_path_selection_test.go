package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

func TestFindExistingCompressedArchivePathDoesNotReturnPlannedName(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260914_1.log")
	require.NoError(t, os.WriteFile(source, []byte("raw"), 0o600))
	// 検索は圧縮設定を必要とせず、未発見なら生成予定パスを返しません。
	logger := &FileLogger{}
	path, found, err := logger.findExistingCompressedArchivePath(source)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, path)
	require.FileExists(t, source)
	require.NoFileExists(t, source+".gz")
}

func TestFileLoggerMissingCompressedFileDoesNotScheduleUploadDuringPendingRecovery(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "missing-compressed", codec)
	upload := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	scheduled, _ := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
	require.False(t, scheduled)

	raw := writeInlineRecoveryRawArchive(t, logger, "20260913", 1)
	pending := logpattern.CompressionSourceDeletePendingPath(raw)
	require.NoError(t, os.Rename(raw, pending))
	// 正常圧縮後に圧縮ファイルが外部削除され、削除待ちsourceだけが残った状態。
	logger.inlinePurge.invalidateDirectorySizeEstimate()
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, raw)
	require.NoFileExists(t, pending)
	require.NoFileExists(t, raw+".gz")
	require.Zero(t, codec.compressCalls.Load())
	require.Empty(t, upload.inputsSnapshot())
	scheduled, _ = fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
	require.False(t, scheduled, "missing compressed file must not create an upload reservation")
}

func TestFileLoggerRotationProtectsPlannedOutputWithoutTreatingItAsExisting(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "planned-output", &inlineRecoveryCompressionCodec{})
	raw := writeInlineRecoveryRawArchive(t, logger, "20260913", 1)
	paths := logger.pathsToProtectForRotatedFile(raw)
	require.ElementsMatch(t, []string{raw, raw + ".gz"}, paths)
	existing, found, err := logger.findExistingCompressedArchivePath(raw)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, existing)
	require.NoFileExists(t, raw+".gz")
}
