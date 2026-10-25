package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// uploadを使う系列は、記録済みの世代を読めないcheckpointのまま書き込みを始めません。
// 記録済みの世代番号へ新しい内容を書くと、送信済みと判定され送信されないまま削除され得るためです。
// uploadを使わない系列は、残存checkpointの不備で書き込みを止めず、読めない記録を使わずに起動します。
// どちらのloggerも、checkpoint storeを構築しないrunner modeで起動時の世代確認だけを検証します。
func TestLoggersRequireReadableRecordedGenerationOnlyWhenUploading(t *testing.T) {
	const (
		target       = "recorded-generation"
		sourcePrefix = "recorded_generation"
	)
	for _, kind := range []string{"file", "delivery"} {
		for _, upload := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/upload=%t", kind, upload), func(t *testing.T) {
				root := t.TempDir()
				checkpointConfig := pkgfsstore.ArchiveCheckpointConfig{
					Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: sourcePrefix,
				}
				documentPath := writeCorruptRecordedGenerationForTest(t, root, target, checkpointConfig)
				corrupt, err := os.ReadFile(documentPath)
				require.NoError(t, err)

				var uploadConfig objectstorage.LogFileUploadConfig
				if upload {
					uploadConfig = recordedGenerationUploadConfigForTest()
				}
				var write func() error
				switch kind {
				case "file":
					cfg := newTestFileLoggerConfig(root, "logs", target)
					cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
					cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
					cfg.ArchiveCheckpointConfig = checkpointConfig
					cfg.ObjectStorageUploadConfig = uploadConfig
					logger, createErr := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
					err = createErr
					if logger != nil {
						closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
						write = func() error { return logger.testInfo(context.Background(), "write after startup") }
					}
				default:
					cfg := newTestDeliveryFileLoggerConfig(root, "logs", target)
					cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
					cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
					cfg.LifecycleRunnerSettings.ArchiveCheckpointConfig = checkpointConfig
					cfg.LifecycleRunnerSettings.ObjectStorageUploadConfig = uploadConfig
					logger, createErr := NewDeliveryFileLogger(context.Background(), NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
					err = createErr
					if logger != nil {
						closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
						write = func() error {
							report, writeErr := logger.testInfo(context.Background(), "write after startup")
							requireDeliverySucceeded(t, report, writeErr)
							return writeErr
						}
					}
				}

				if upload {
					require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
					require.Nil(t, write, "the logger must not start without the recorded generations")
					logs, globErr := filepath.Glob(filepath.Join(root, target, "logs", "*.log"))
					require.NoError(t, globErr)
					require.Empty(t, logs, "no log file may be opened before the recorded generations are known")
				} else {
					require.NoError(t, err)
					require.NoError(t, write())
				}
				contents, err := os.ReadFile(documentPath)
				require.NoError(t, err)
				require.Equal(t, corrupt, contents, "startup must not rewrite the unreadable checkpoint")
			})
		}
	}
}

// uploadを使わない通常のFileLoggerでも、読めるcheckpointに記録済みの世代は使用済みとして扱い、
// その後の番号から書き込みます。uploadを後で有効にしたときに、記録済みの世代と同じ名前の新しい
// ログが送信済みと判定されて送信されないまま削除されることを防ぐためです。記録済みの期間を現在の
// UTCの日付より後に置き、時計が記録より前の場合は記録済みの期間を続ける規則も使って、試験中に
// 日付が変わっても期待するfile名が変わらないようにします。
func TestFileLoggerWithoutUploadStartsAfterReadableRecordedGeneration(t *testing.T) {
	ctx := context.Background()
	const target = "recorded-generation-without-upload"
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "logs", target)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "recorded_generation_without_upload",
	}
	require.False(t, cfg.ObjectStorageUploadConfig.Enabled, "the series must not use upload")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	// checkpointの系列はログ保存先の実体pathで識別します。
	resolved, err := filepath.EvalSymlinks(directory)
	require.NoError(t, err)
	recordedPeriod := time.Now().UTC().AddDate(0, 0, 10).Format(pkgfsstore.FileDateFormat)
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.ArchiveCheckpointConfig.Directory))
	sourceID := checkpointstore.ArchiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(DefaultLogger),
		logpattern.FileNamePattern{Dir: resolved, Target: target, Extension: LogFileExtension})
	store, err := checkpointstore.NewFilesystemStore(cfg.ArchiveCheckpointConfig.Directory, sourceID)
	require.NoError(t, err)
	_, err = store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: recordedPeriod + "-4", Compressed: true},
		[]objectstorage.DestinationID{"archive"})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	require.Equal(t, filepath.Join(resolved, target+"_"+recordedPeriod+"_5.log"), logger.Fsstore.ActiveFilePath)
	require.NoError(t, logger.testInfo(ctx, "write after the recorded generation"))
	assertLogFileContains(t, logger.Fsstore.ActiveFilePath, "write after the recorded generation")
}

// writeCorruptRecordedGenerationForTestは、targetの通常ログ系列が読むcheckpoint文書を
// 解析できない内容で作り、その文書のpathを返します。ログ保存先は実体のpathで識別します。
func writeCorruptRecordedGenerationForTest(t *testing.T, root, target string, cfg pkgfsstore.ArchiveCheckpointConfig) string {
	t.Helper()
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	resolved, err := filepath.EvalSymlinks(directory)
	require.NoError(t, err)
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.Directory))
	pattern := logpattern.FileNamePattern{Dir: resolved, Target: target, Extension: LogFileExtension}
	sourceID := checkpointstore.ArchiveCheckpointSourceID(cfg.SourcePrefix, string(DefaultLogger), pattern)
	documentPath := filepath.Join(cfg.Directory, sourceID+".log-file-uploads.json")
	require.NoError(t, os.WriteFile(documentPath, []byte("invalid json"), 0o600))
	return documentPath
}

// recordedGenerationUploadConfigForTestは、構築時に通信しない送信先を有効にします。
func recordedGenerationUploadConfigForTest() objectstorage.LogFileUploadConfig {
	target := objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region: "ap-northeast-1", Endpoint: "https://s3.example.com",
			AccessKeyID: "test", SecretAccessKey: "test", Bucket: "archive",
		},
	}
	return objectstorage.LogFileUploadConfig{Enabled: true, Default: target, Error: target, Debug: target}
}
