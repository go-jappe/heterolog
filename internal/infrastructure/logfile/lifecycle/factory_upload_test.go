package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

func TestNewLogFileLifecyclesRejectsUnsupportedObjectStorageUploadTarget(t *testing.T) {
	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         "object-storage-upload-manager",
				LogPath:        resolvedTempDirForTest(t),
				LogDir:         "logs",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL: time.Hour,
				RotationType:  pkgfsstore.FileRotationDaily,
			},
		},
		ObjectStorageUploadConfig: objectstorage.LogFileUploadConfig{
			Enabled: true,
			Default: objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: []objectstorage.UploadTarget{"unsupported"},
			},
		},
	}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})

	var targetErr *objectstoragevalidation.UnsupportedUploadTargetError
	require.ErrorAs(t, err, &targetErr)
	require.Equal(t, objectstorage.UploadTarget("unsupported"), targetErr.Target)
	require.Nil(t, lifecycles)
}

func TestLogFileCheckpointConstructionDoesNotPrepareMissingDirectory(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ArchiveCheckpointConfig.Directory = filepath.Join(resolvedTempDirForTest(t), "unprepared-checkpoints")
	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.Nil(t, lifecycles)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoDirExists(t, cfg.ArchiveCheckpointConfig.Directory)
}

func TestLogFileLifecycleWithoutUploadDoesNotRequireCheckpointDirectory(t *testing.T) {
	for _, directoryState := range []string{"missing", "prepared"} {
		t.Run(directoryState, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
			if directoryState == "missing" {
				cfg.ArchiveCheckpointConfig.Directory = filepath.Join(resolvedTempDirForTest(t), "absent-checkpoints")
			}
			lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
			require.NoError(t, err)
			require.NotEmpty(t, lifecycles)
			for _, lifecycle := range lifecycles {
				for _, manager := range lifecycle.fileSeriesManagers {
					require.Nil(t, manager.objectStorageUpload)
				}
				require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
			}
			if directoryState == "missing" {
				require.NoDirExists(t, cfg.ArchiveCheckpointConfig.Directory)
			} else {
				entries, err := os.ReadDir(cfg.ArchiveCheckpointConfig.Directory)
				require.NoError(t, err)
				require.Empty(t, entries, "disabled upload must not initialize checkpoint documents or markers")
			}
		})
	}
}

func TestLogFileLifecycleWithoutUploadRejectsInvalidCheckpointDirectory(t *testing.T) {
	for _, directoryState := range []string{"file", "symlink"} {
		t.Run(directoryState, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
			path := filepath.Join(resolvedTempDirForTest(t), "checkpoint")
			if directoryState == "file" {
				require.NoError(t, os.WriteFile(path, []byte("not a directory"), 0o600))
			} else {
				testutil.Symlink(t, resolvedTempDirForTest(t), path)
			}
			cfg.ArchiveCheckpointConfig.Directory = path
			lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
			require.Error(t, err)
			require.Nil(t, lifecycles)
		})
	}
}

func TestLogFileLifecycleWithoutUploadRejectsInvalidCheckpointSource(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
	cfg.ArchiveCheckpointConfig.SourcePrefix = "Bad/Prefix"

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.ErrorIs(t, err, checkpointstore.ErrInvalidSourceID)
	require.Nil(t, lifecycles)
}

func TestLogFileLifecycleWithoutUploadIgnoresExistingCorruptCheckpoint(t *testing.T) {
	ctx := context.Background()
	cfg := configuredCheckpointFactoryTestConfig(t)
	enabledUploadConfig := cfg.ObjectStorageUploadConfig
	initial, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, initial, 1)
	manager := initial[0].fileSeriesManagers[0]
	store := checkpointStoreForFactoryTest(t, manager)
	checkpointPath := filepath.Join(store.Directory(), store.SourceID()+".log-file-uploads.json")
	fileName := manager.FileNamePattern()
	require.NoError(t, boundedwait.Close(t, "initial[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes()", initial[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(initial[0])))

	corruptPayload := []byte("{broken")
	require.NoError(t, os.WriteFile(checkpointPath, corruptPayload, 0o600))
	require.NoError(t, os.MkdirAll(fileName.Dir, 0o750))
	compressedPath := filepath.Join(fileName.Dir, fileName.Target+"_"+currentLogDate()+"_1.log.gz")
	writeGzipFile(t, compressedPath, []byte("retained while upload is disabled"))
	oldCompressedPath := filepath.Join(fileName.Dir, fileName.Target+"_20200101_1.log.gz")
	writeGzipFile(t, oldCompressedPath, []byte("expired while upload is disabled"))

	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
	cfg.FsstoreConfig.Lifecycle.MaxFileAge = 1
	disabled, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, disabled, 1)
	require.Nil(t, disabled[0].fileSeriesManagers[0].objectStorageUpload)
	report, err := disabled[0].RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, compressedPath)
	require.NoFileExists(t, oldCompressedPath, "upload無効でも通常の保持期限による削除は続ける")
	require.NoError(t, boundedwait.Close(t, "disabled[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes()", disabled[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(disabled[0])))
	payload, err := os.ReadFile(checkpointPath)
	require.NoError(t, err)
	require.Equal(t, corruptPayload, payload)

	// uploadを再び有効にしたときに初めて、残していたcheckpointを検証します。
	cfg.ObjectStorageUploadConfig = enabledUploadConfig
	reenabled, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.Nil(t, reenabled)
	require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
}

func TestNewLogFileLifecyclesRequiresCheckpointForUploader(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})

	require.Nil(t, lifecycles)
	require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
}

func TestNewLogFileLifecyclesRejectsPartialCheckpointConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		checkpoint pkgfsstore.ArchiveCheckpointConfig
	}{
		{
			name: "directory only",
			checkpoint: pkgfsstore.ArchiveCheckpointConfig{
				Directory: filepath.Join(resolvedTempDirForTest(t), "archive-checkpoints"),
			},
		},
		{
			name: "source prefix only",
			checkpoint: pkgfsstore.ArchiveCheckpointConfig{
				SourcePrefix: "logger_file",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.ArchiveCheckpointConfig = test.checkpoint

			lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})

			require.Nil(t, lifecycles)
			require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
			require.ErrorIs(t, err, ErrIncompleteCheckpointConfiguration)
		})
	}
}

func TestNewLogFileLifecyclesDoesNotCreateCheckpointStoreWithoutUploaderOrConfiguration(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})

	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	require.Nil(t, lifecycles[0].fileSeriesManagers[0].objectStorageUpload)
}

func TestNewLogFileLifecyclesBuildsStablePhysicalSourceCheckpoint(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})

	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	manager := lifecycles[0].fileSeriesManagers[0]
	require.NotNil(t, manager.objectStorageUpload.checkpointStore)
	require.NotEmpty(t, checkpointStoreForFactoryTest(t, manager).SourceID())
	fileName := manager.FileNamePattern()
	want := archiveCheckpointSourceID(
		cfg.ArchiveCheckpointConfig.SourcePrefix,
		string(fileClassDefault),
		fileName,
	)
	require.Equal(t, want, checkpointStoreForFactoryTest(t, manager).SourceID())

	changedPattern := fileName
	changedPattern.Dir = filepath.Join(filepath.Dir(changedPattern.Dir), "moved")
	require.NotEqual(t, want, archiveCheckpointSourceID(
		cfg.ArchiveCheckpointConfig.SourcePrefix,
		string(fileClassDefault),
		changedPattern,
	))
	require.Equal(t, fileName, manager.FileNamePattern())
	require.Equal(t, want, archiveCheckpointSourceID(
		cfg.ArchiveCheckpointConfig.SourcePrefix,
		string(fileClassDefault),
		logpattern.FileNamePattern{
			Dir:       filepath.Clean(fileName.Dir),
			Prefix:    fileName.Prefix,
			Target:    fileName.Target,
			Extension: fileName.Extension,
		},
	))
}

func TestDisablingSeparatedFileSeriesPreservesTheirUploadSuccesses(t *testing.T) {
	ctx := context.Background()
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.ErrorLogDir = "error-logs"
	cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
	cfg.ObjectStorageUploadConfig.Error = cfg.ObjectStorageUploadConfig.Default
	cfg.ObjectStorageUploadConfig.Debug = cfg.ObjectStorageUploadConfig.Default
	separated, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	managers := fileSeriesManagersForLifecycles(separated)
	require.Len(t, managers, 3)
	const destination = objectstorage.DestinationID("primary")
	key := checkpointstore.CheckpointKey{Generation: "20260920-1", Compressed: false}
	documents := make(map[string][]byte)
	for _, manager := range managers {
		if manager.FileNamePattern().Prefix == "" {
			continue
		}
		store := checkpointStoreForFactoryTest(t, manager)
		_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{destination})
		require.NoError(t, err)
		path := filepath.Join(store.Directory(), store.SourceID()+".log-file-uploads.json")
		documents[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}
	require.Len(t, documents, 2)
	for _, lifecycle := range separated {
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}

	// 分離対象を減らしても、担当しなくなった系列の成功記録は変更しません。
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	collapsed, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, fileSeriesManagersForLifecycles(collapsed), 1)
	for _, lifecycle := range collapsed {
		_, err = lifecycle.RunOnce(ctx)
		require.NoError(t, err)
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
	for path, expected := range documents {
		actual, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Equal(t, expected, actual)
	}

	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	restored, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	for _, manager := range fileSeriesManagersForLifecycles(restored) {
		if manager.FileNamePattern().Prefix == "" {
			continue
		}
		state, loadErr := manager.objectStorageUpload.checkpointStore.Load(ctx, key)
		require.NoError(t, loadErr)
		require.True(t, state.DestinationComplete(destination))
	}
	for _, lifecycle := range restored {
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
}

func fileSeriesManagersForLifecycles(lifecycles []*DirectoryLifecycle) []*FileSeriesManager {
	var managers []*FileSeriesManager
	for _, lifecycle := range lifecycles {
		managers = append(managers, lifecycle.fileSeriesManagers...)
	}
	return managers
}

func checkpointStoreForFactoryTest(t *testing.T, manager *FileSeriesManager) *checkpointstore.FilesystemStore {
	t.Helper()
	require.NotNil(t, manager.objectStorageUpload)
	store, ok := manager.objectStorageUpload.checkpointStore.(*checkpointstore.FilesystemStore)
	require.True(t, ok)
	return store
}

func configuredCheckpointFactoryTestConfig(t *testing.T) pkgfsstore.LogFileLifecycleRunnerConfig {
	t.Helper()
	root := resolvedTempDirForTest(t)
	checkpointDirectory := filepath.Join(resolvedTempDirForTest(t), "archive-checkpoints")
	require.NoError(t, checkpointstore.PrepareDirectory(checkpointDirectory))
	return pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         "checkpoint-factory",
				LogPath:        root,
				LogDir:         "logs",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:  time.Hour,
				RotationType:   pkgfsstore.FileRotationDaily,
				MaxFileBackups: 10,
				MaxFileAge:     36500,
				MaxFileSize:    1 << 20,
				MaxDirSize:     20 << 20,
			},
		},
		ObjectStorageUploadConfig: objectstorage.LogFileUploadConfig{
			Enabled: true,
			Default: objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
				S3: objectstorage.S3Config{
					Region:          "ap-northeast-1",
					Endpoint:        "https://s3.example.test",
					AccessKeyID:     "access-key",
					SecretAccessKey: "secret-key",
					Bucket:          "archive-bucket",
				},
			},
		},
		ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{
			Directory:    checkpointDirectory,
			SourcePrefix: "logger_file",
		},
	}
}
