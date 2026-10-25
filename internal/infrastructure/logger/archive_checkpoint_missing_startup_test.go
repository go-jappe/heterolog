package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestLoggerStartupUsesMissingArchiveCheckpointRecoverySetting(t *testing.T) {
	for _, contract := range []string{"sync", "async", "delivery"} {
		for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
			if contract == "delivery" && mode == pkgfsstore.FileLifecycleModeInline {
				continue // Delivery file loggers support runner lifecycle only.
			}
			for _, role := range []string{"default", "error", "debug"} {
				for _, recoverMissing := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/recover=%t", contract, mode, role, recoverMissing), func(t *testing.T) {
						ctx := context.Background()
						// checkpointのsource IDはproductionが解決した物理pathから決まるため、
						// 削除する登録済みcheckpointも解決済みpathから特定します。
						root := resolvedTempDirForTest(t)
						directory := filepath.Join(root, "checkpoints")
						var created io.Closer
						var startupErr error
						var missingPath string
						if contract == "delivery" {
							file := checkpointStartupDeliveryFileConfig(t, directory)
							configureStartupCheckpointRoles(&file.FsstoreConfig.Layout, &file.LifecycleRunnerSettings.ObjectStorageUploadConfig, filepath.Join(root, "logs"))
							file.LifecycleRunnerSettings.ArchiveCheckpointConfig.AutoRecoverMissingCheckpoint = recoverMissing
							missingPath = removeRegisteredStartupCheckpoint(t, role, file.FsstoreConfig.Layout,
								file.LifecycleRunnerSettings.ArchiveCheckpointConfig, file.LifecycleRunnerSettings.ObjectStorageUploadConfig.Default.S3)
							cfg := pkglogger.DeliveryLoggerConfig{
								LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
								Timeout:               time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, File: file,
							}
							created, startupErr = NewDeliveryLogger(ctx, cfg, streamrouting.None, observer.NopTracer{}, observer.NopMeter{})
						} else {
							file := checkpointStartupFileConfig(t, directory)
							file.LifecycleMode = mode
							configureStartupCheckpointRoles(&file.FsstoreConfig.Layout, &file.ObjectStorageUploadConfig, filepath.Join(root, "logs"))
							file.ArchiveCheckpointConfig.AutoRecoverMissingCheckpoint = recoverMissing
							missingPath = removeRegisteredStartupCheckpoint(t, role, file.FsstoreConfig.Layout,
								file.ArchiveCheckpointConfig, file.ObjectStorageUploadConfig.Default.S3)
							cfg := pkglogger.LoggerConfig{
								LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
								Timeout:               time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, File: file,
							}
							if contract == "sync" {
								created, startupErr = NewLogger(ctx, cfg, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{})
							} else {
								asyncCfg := asyncConfigFromLoggerConfig(cfg)
								asyncCfg.MaxLogEntryBytes, asyncCfg.MaxParallel = cfg.MaxLogEntryBytes, 1
								created, startupErr = NewAsyncLogger(ctx, asyncCfg, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{})
							}
						}
						if !recoverMissing {
							require.ErrorIs(t, startupErr, checkpointstore.ErrRegisteredCheckpointMissing)
							require.Nil(t, created)
							require.NoFileExists(t, missingPath)
							return
						}
						require.NoError(t, startupErr)
						require.NotNil(t, created)
						t.Cleanup(func() { require.NoError(t, created.Close()) })
						require.FileExists(t, missingPath, "the missing active checkpoint is restored before startup returns")
						require.NoError(t, created.Close())
						// 自動復旧はcursorを空に戻すため、宛先追加前の残存ログも再送対象です。
						source := strings.TrimSuffix(filepath.Base(missingPath), ".log-file-uploads.json")
						reopened, err := checkpointstore.NewFilesystemStore(filepath.Dir(missingPath), source)
						require.NoError(t, err)
						recovered, err := reopened.Load(ctx, checkpointstore.CheckpointKey{Generation: "20190101-4", Compressed: true})
						require.NoError(t, err)
						require.Empty(t, recovered.CompletedDestinationIDs, "復旧時は過去の送信不要判定を推測しない")
						cursors, err := reopened.DestinationCursors(ctx)
						require.NoError(t, err)
						require.Len(t, cursors, 1, "復旧した宛先を新規登録として扱わない")
						require.Empty(t, cursors[0].Generation)
						destinations := []objectstorage.DestinationID{cursors[0].DestinationID}
						require.Equal(t, destinations, recovered.PendingDestinationIDs(destinations))
						require.NoError(t, reopened.Close())
					})
				}
			}
		}
	}
}

func configureStartupCheckpointRoles(layout *pkgfsstore.FsstoreConfig, upload *objectstorage.LogFileUploadConfig, root string) {
	layout.LogPath = root
	layout.IsSeparateErrorLogsEnabled = true
	layout.IsSeparateDebugLogsEnabled = true
	layout.ErrorLogDir = layout.LogDir
	layout.IsSeparateDebugLogDirEnabled = false
	layout.SeparateErrorLogsPrefix = "error_"
	layout.SeparateDebugLogsPrefix = "debug_"
	upload.Error = upload.Default
	upload.Debug = upload.Default
}

func removeRegisteredStartupCheckpoint(
	t *testing.T,
	role string,
	layout pkgfsstore.FsstoreConfig,
	cfg pkgfsstore.ArchiveCheckpointConfig,
	s3 objectstorage.S3Config,
) string {
	t.Helper()
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.Directory))
	pattern := logpattern.FileNamePattern{
		Dir: filepath.Join(layout.LogPath, layout.Target, layout.LogDir), Target: layout.Target, Extension: "log",
	}
	if role == "error" {
		pattern.Prefix = layout.SeparateErrorLogsPrefix
		pattern.Dir = filepath.Join(layout.LogPath, layout.Target, layout.ErrorLogDir)
	} else if role == "debug" {
		pattern.Prefix = layout.SeparateDebugLogsPrefix
	}
	source := checkpointstore.ArchiveCheckpointSourceID(cfg.SourcePrefix, role, pattern)
	destination, err := s3uploader.ResolveDestinationID(s3)
	require.NoError(t, err)
	store, err := checkpointstore.NewFilesystemStore(cfg.Directory, source)
	require.NoError(t, err)
	require.NoError(t, store.RegisterDestinations(context.Background(), []objectstorage.DestinationID{destination}, []checkpointstore.CheckpointKey{{Generation: "20190101-4", Compressed: true}}))
	_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: checkpointstore.ArchiveCheckpointGenerationKey("20200101", 9), Compressed: true}, []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	require.NoError(t, store.Close())
	path := filepath.Join(cfg.Directory, source+".log-file-uploads.json")
	require.FileExists(t, path)
	require.NoError(t, os.Remove(path))
	return path
}
