package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	objectstoragefailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkguploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

func TestFailureArchiveWithoutUploadDoesNotPrepareMissingCheckpointDirectory(t *testing.T) {
	cfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
	cfg.ArchiveCheckpointConfig.Directory = filepath.Join(resolvedTempDirForTest(t), "unprepared-checkpoints")
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: newFailureArchiveTestConfigAdapter(cfg)})
	require.NoError(t, err)
	require.NotEmpty(t, lifecycles)
	for _, runner := range lifecycles {
		for _, manager := range runner.fileSeriesManagers {
			require.Nil(t, manager.objectStorageUpload)
		}
	}
	require.NoDirExists(t, cfg.ArchiveCheckpointConfig.Directory)
}

func TestNewFailureArchiveLifecyclesBuildsRetryableAndPermanentDirectories(t *testing.T) {
	t.Parallel()

	cfg := newFailureArchiveLifecycleTestConfig(t, objectstorage.UploadTargetConfig{})
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})

	require.NoError(t, err)
	require.Len(t, lifecycles, 2)

	wantFailureCategories := []struct {
		terminalDir string
	}{
		{terminalDir: cfg.DirCaseRetryable()},
		{terminalDir: cfg.DirCasePermanent()},
	}
	for index, wantFailureCategory := range wantFailureCategories {
		require.Len(t, lifecycles[index].fileSeriesManagers, 1)

		manager := lifecycles[index].fileSeriesManagers[0]
		wantDir, pathErr := failurearchive.GetFileArchiveDirectoryPath(cfg, wantFailureCategory.terminalDir)
		require.NoError(t, pathErr)
		fileName := manager.FileNamePattern()
		require.Equal(t, wantDir, fileName.Dir)
		require.Empty(t, fileName.Prefix)
		require.Equal(t, cfg.Target(), fileName.Target)
		require.Equal(t, failurearchive.FileExtension, fileName.Extension)
		require.Nil(t, manager.objectStorageUpload)
	}
}

func TestNewFailureArchiveLifecyclesBindEachFailureCategoryToDirectoryDestinationAndCheckpointSource(t *testing.T) {
	t.Parallel()

	uploadCfg := validFailureArchiveUploadTargetConfig()
	cfg := newFailureArchiveLifecycleTestConfig(t, uploadCfg)
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})

	require.NoError(t, err)
	require.Len(t, lifecycles, 2)

	tests := []struct {
		name            string
		failureCategory failurearchive.FailureCategory
		terminalDir     string
		runner          *DirectoryLifecycle
	}{
		{
			name:            "retryable",
			failureCategory: failurearchive.FailureCategoryRetryable,
			terminalDir:     cfg.DirCaseRetryable(),
			runner:          lifecycles[0],
		},
		{
			name:            "permanent",
			failureCategory: failurearchive.FailureCategoryPermanent,
			terminalDir:     cfg.DirCasePermanent(),
			runner:          lifecycles[1],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.name, string(tt.failureCategory), "stored failure category values must remain stable")
			require.Len(t, tt.runner.fileSeriesManagers, 1)
			manager := tt.runner.fileSeriesManagers[0]
			fileName := manager.FileNamePattern()

			wantDir, pathErr := failurearchive.GetFileArchiveDirectoryPath(cfg, tt.terminalDir)
			require.NoError(t, pathErr)
			require.Equal(t, wantDir, fileName.Dir)

			destinationUploader, ok := manager.objectStorageUpload.uploader.(pkguploader.DestinationUploader)
			require.True(t, ok, "uploader type %T", manager.objectStorageUpload.uploader)
			wantS3Config := uploadCfg.S3
			wantS3Config.Prefix = objectstoragefailurearchive.ObjectStoragePrefix(wantS3Config.Prefix, tt.failureCategory)
			wantDestinationID, resolveErr := s3uploader.ResolveDestinationID(wantS3Config)
			require.NoError(t, resolveErr)
			require.Equal(
				t,
				[]objectstorage.DestinationID{wantDestinationID},
				destinationUploader.DestinationIDs(),
			)

			require.NotNil(t, manager.objectStorageUpload.checkpointStore)
			wantSourceID := archiveCheckpointSourceID(
				cfg.ArchiveCheckpoint().SourcePrefix,
				tt.name,
				fileName,
			)
			require.Equal(t, wantSourceID, checkpointStoreForFactoryTest(t, manager).SourceID())
		})
	}
}

func TestNewFailureArchiveLifecyclesRequiresSharedSpoolRuntime(t *testing.T) {
	t.Parallel()

	uploadCfg := validFailureArchiveUploadTargetConfig()
	uploadCfg.CompressedLogFileUploadFailureStore = objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
	}
	spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "failure-runner", pkglogger.LoggerContractSync)
	uploadCfg.CompressedLogFileUploadSpool = objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig)
	cfg := newFailureArchiveLifecycleTestConfig(t, uploadCfg)
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})

	require.ErrorIs(t, err, compressedlogfailure.ErrInvalidConfig)
	require.ErrorIs(t, err, compressedlogfailure.ErrNoMatchingSpoolRuntime)
	require.Nil(t, lifecycles)
	entries, readErr := os.ReadDir(spoolConfig.SpoolPath)
	require.NoError(t, readErr)
	require.Empty(t, entries, "missing runtime must not trigger spool construction")
}

func TestNewFailureArchiveLifecyclesKeepsCallerOwnedSpoolRuntime(t *testing.T) {
	uploadCfg := validFailureArchiveUploadTargetConfig()
	uploadCfg.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()
	cfg := newFailureArchiveLifecycleTestConfig(t, uploadCfg)
	spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), cfg.Target(), pkglogger.LoggerContractSync)
	runtimes := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg, SpoolRuntimes: runtimes})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)
	for _, lifecycle := range lifecycles {
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
	_, err = compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig), []objectstorage.UploadTarget{objectstorage.UploadTargetS3})...)
	require.ErrorIs(t, err, compressedlog.ErrSpoolRootAlreadyOwned)
	require.NoError(t, boundedwait.Close(t, "runtimes.Close()", runtimes.Close, boundedwait.ForceCloserOf(runtimes)))
	replacement := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
	require.NoError(t, boundedwait.Close(t, "replacement.Close()", replacement.Close, boundedwait.ForceCloserOf(replacement)))
}

func TestNewFailureArchiveLifecyclesWrapsJSONLUploadWithFailureArchive(t *testing.T) {
	t.Parallel()
	uploadCfg := validFailureArchiveUploadTargetConfig()
	uploadCfg.CompressedLogFileUploadFailureStore = objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive},
		DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{
			FileArchiveConfig: objectstorage.CompressedLogFileUploadFailureArchiveConfig{
				RootPath: resolvedTempDirForTest(t), LoggerContract: "logger", LifecycleInterval: time.Hour,
			},
		},
	}
	cfg := newFailureArchiveLifecycleTestConfig(t, uploadCfg)
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)
	var destinations []objectstorage.DestinationID
	for _, lifecycle := range lifecycles {
		manager := lifecycle.fileSeriesManagers[0]
		managed, ok := manager.objectStorageUpload.uploader.(*compressedlogfailure.ManagedUploader)
		require.True(t, ok)
		destinations = append(destinations, managed.DestinationIDs()...)
		require.Len(t, lifecycle.objectStorageCompressedLogFileUploadFailureRouteOwners, 1)
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
	require.Len(t, destinations, 2)
	require.NotEqual(t, destinations[0], destinations[1], "元レコードの失敗分類別prefixで宛先も分かれる")
}

func TestNewFailureArchiveLifecyclesRequiresCheckpointForUploader(t *testing.T) {
	t.Parallel()

	durableCfg := newFailureArchiveLifecycleDurableConfig(t, validFailureArchiveUploadTargetConfig())
	durableCfg.ArchiveCheckpointConfig = fsstore.ArchiveCheckpointConfig{}
	cfg := newFailureArchiveTestConfigAdapter(durableCfg)

	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})

	require.Nil(t, lifecycles)
	require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
}

func TestNewFailureArchiveLifecyclesReportsUploaderConstructionKind(t *testing.T) {
	t.Parallel()

	cfg := newFailureArchiveLifecycleTestConfig(t, objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{"unsupported"},
	})
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})

	require.Nil(t, lifecycles)
	var uploaderErr *FailureArchiveUploaderError
	require.ErrorAs(t, err, &uploaderErr)
	require.Equal(t, failurearchive.FailureCategoryRetryable, uploaderErr.FailureCategory)
	var targetErr *objectstoragevalidation.UnsupportedUploadTargetError
	require.ErrorAs(t, err, &targetErr)
	require.Equal(t, objectstorage.UploadTarget("unsupported"), targetErr.Target)
}

func TestFailureArchiveLifecycleCompressesInactiveJSONL(t *testing.T) {
	t.Parallel()

	cfg := newFailureArchiveLifecycleTestConfig(t, objectstorage.UploadTargetConfig{})
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})
	require.NoError(t, err)

	dir, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.DirCaseRetryable())
	require.NoError(t, err)
	compressionSource := filepath.Join(dir, cfg.Target()+"_20240101_1.jsonl")
	latest := filepath.Join(dir, cfg.Target()+"_20240101_2.jsonl")
	require.NoError(t, os.WriteFile(compressionSource, []byte("{\"status\":\"retryable\"}\n"), 0o640))
	require.NoError(t, os.WriteFile(latest, []byte("{\"status\":\"retryable\"}\n"), 0o640))

	report, err := lifecycles[0].RunOnce(context.Background())

	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	require.Equal(t, compressionSource, report.Compressed[0].SourcePath)
	require.Equal(t, compressionSource+fsstore.ExtensionCompressZstd, report.Compressed[0].CompressedPath)
	_, err = os.Stat(compressionSource)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(compressionSource + fsstore.ExtensionCompressZstd)
	require.NoError(t, err)
	_, err = os.Stat(latest)
	require.NoError(t, err)
}

func TestFailureArchiveLifecyclePurgesJSONLByGeneration(t *testing.T) {
	t.Parallel()

	durableCfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
	durableCfg.EnableFileCompress = false
	cfg := newFailureArchiveTestConfigAdapter(durableCfg)
	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})
	require.NoError(t, err)

	dir, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.DirCaseRetryable())
	require.NoError(t, err)
	now := time.Now()
	paths := make([]string, 0, 4)
	for index := 1; index <= 4; index++ {
		path := filepath.Join(dir, fmt.Sprintf("%s_20240101_%d.jsonl", cfg.Target(), index))
		require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o640))
		modTime := now.Add(time.Duration(index) * time.Minute)
		require.NoError(t, os.Chtimes(path, modTime, modTime))
		paths = append(paths, path)
	}

	report, err := lifecycles[0].RunOnce(context.Background())

	require.NoError(t, err)
	require.True(t, report.Purge.ByGeneration.Ran)
	require.Len(t, report.Purge.ByGeneration.RemovalResults, 1)
	require.Equal(t, paths[0], report.Purge.ByGeneration.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByGeneration.RemovalResults[0].Err)
	_, err = os.Stat(paths[0])
	require.ErrorIs(t, err, os.ErrNotExist)
	for _, path := range paths[1:] {
		_, err = os.Stat(path)
		require.NoError(t, err)
	}
}

func newFailureArchiveLifecycleTestConfig(
	t *testing.T,
	uploadCfg objectstorage.UploadTargetConfig,
) FailureArchiveRunnerConfigProvider {
	t.Helper()
	return newFailureArchiveTestConfigAdapter(
		newFailureArchiveLifecycleDurableConfig(t, uploadCfg),
	)
}

func newFailureArchiveTestConfigAdapter(cfg durable.FileArchiveConfig) loggerdurable.FileArchiveConfigAdapter {
	return loggerdurable.NewFileArchiveConfigAdapter(cfg)
}

func newFailureArchiveLifecycleDurableConfig(
	t *testing.T,
	uploadCfg objectstorage.UploadTargetConfig,
) durable.FileArchiveConfig {
	t.Helper()
	checkpointDirectory := filepath.Join(resolvedTempDirForTest(t), "archive-checkpoints")
	require.NoError(t, checkpoint.PrepareDirectory(checkpointDirectory))
	return durable.FileArchiveConfig{
		Target:                     "failure-runner",
		MaxFileSize:                2 * 1024 * 1024,
		FailureFileArchivePath:     resolvedTempDirForTest(t),
		Dir:                        "backend-failures",
		DirCaseRetryable:           "retryable",
		DirCasePermanent:           "permanent",
		FileNameFormat:             fsstore.FailureArchiveFileNameFormat,
		FileDateFormat:             fsstore.FileDateFormat,
		EnableRotate:               true,
		MaxFileBackups:             2,
		MaxFileAge:                 36500,
		MaxDirSize:                 20 * 1024 * 1024,
		UsageCacheTTL:              time.Hour,
		EnableFileCompress:         true,
		CompressFormat:             fsstore.CompressZstd,
		LifecycleMode:              fsstore.FileLifecycleModeRunner,
		RotationType:               fsstore.FileRotationDaily,
		RotationDayOfWeek:          time.Sunday,
		MaxWriteRetries:            0,
		FileOperationRetryWaitTime: time.Millisecond,
		LifecycleRunnerConfig: fsstore.AppendLogRunnerConfig{
			Interval: 15 * time.Minute,
		},
		ObjectStorageUploadConfig: uploadCfg,
		ArchiveCheckpointConfig: fsstore.ArchiveCheckpointConfig{
			Directory:    checkpointDirectory,
			SourcePrefix: "failure_archive_test",
		},
		OverloadPolicyConfig: durable.FileArchiveOverloadPolicyConfig{
			OverloadPolicy:        durable.OverloadPolicyDrop,
			SpillDestination:      durable.SpillDestinationStdout,
			MinAvailableDiskSpace: durable.MinFileArchiveAvailableDiskSpace,
		},
	}
}

func validFailureArchiveUploadTargetConfig() objectstorage.UploadTargetConfig {
	return objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "https://s3.example.test",
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			Bucket:          "failure-archive",
			Prefix:          "backend/failures",
			ForcePathStyle:  true,
		},
	}
}
