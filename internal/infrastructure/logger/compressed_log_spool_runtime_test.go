package logger

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestCompressedLogSpoolRuntimeSharesRootAcrossIndependentProvidersAndRejectsIndependentOwner(t *testing.T) {
	upload := sharedCompressedLogSpoolTestUpload(t)
	independentUpload := sharedCompressedLogSpoolTestUpload(t, func(config *objectstorage.CompressedLogFileUploadSpoolConfig) {
		config.SpoolPath = upload.CompressedLogFileUploadSpool.SpoolPath()
	})
	owners, err := newCompressedLogSpoolRuntimeBuilder(upload, independentUpload)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owners.Close()) })
	require.Len(t, owners.OwnedRuntimes(), 1)
	runtimes, err := buildCompressedLogSpoolRuntimes(upload, owners)
	require.NoError(t, err)
	independentRuntimes, err := buildCompressedLogSpoolRuntimes(independentUpload, owners)
	require.NoError(t, err)
	require.Same(t, runtimes[objectstorage.UploadTargetS3], independentRuntimes[objectstorage.UploadTargetS3])
	require.NoDirExists(t, runtimes[objectstorage.UploadTargetS3].Root().Path(), "runtimeの構築だけではfilesystemを変更しない")

	_, err = newCompressedLogSpoolRuntimeBuilder(independentUpload)
	require.ErrorIs(t, err, compressedlog.ErrSpoolRootAlreadyOwned)
	require.NoError(t, owners.Close())
	replacement, err := newCompressedLogSpoolRuntimeBuilder(independentUpload)
	require.NoError(t, err)
	require.NoError(t, replacement.Close())
}

func TestCompressedLogSpoolRuntimeRejectsConflictingSettingsAndReleasesRoot(t *testing.T) {
	upload := sharedCompressedLogSpoolTestUpload(t)
	conflicting := sharedCompressedLogSpoolTestUpload(t, func(config *objectstorage.CompressedLogFileUploadSpoolConfig) {
		config.SpoolPath = upload.CompressedLogFileUploadSpool.SpoolPath()
		config.RunnerInterval += time.Second
	})
	_, err := newCompressedLogSpoolRuntimeBuilder(upload, conflicting)
	require.ErrorIs(t, err, compressedlog.ErrSpoolRootConfigMismatch)

	// 後から見つかった設定不一致で構築を中止しても、先に確保したrootは解放します。
	replacement, err := newCompressedLogSpoolRuntimeBuilder(upload)
	require.NoError(t, err)
	require.NoError(t, replacement.Close())
}

func TestCompressedLogSpoolRuntimeIgnoresUnselectedAndInactiveRoles(t *testing.T) {
	for _, mode := range []string{"disabled upload", "unselected spool", "inactive error role", "disabled lifecycle"} {
		t.Run(mode, func(t *testing.T) {
			fileConfig := newActivationFileConfig(t, "inactive-spool")
			upload := sharedCompressedLogSpoolTestUpload(t, func(config *objectstorage.CompressedLogFileUploadSpoolConfig) {
				config.RunnerInterval = 0
			})
			switch mode {
			case "disabled upload":
				upload.Enabled = false
			case "unselected spool":
				upload.Default.CompressedLogFileUploadFailureStore.DurableType = nil
			case "inactive error role":
				upload.Error = upload.Default
				upload.Default.Enabled = false
			case "disabled lifecycle":
				fileConfig.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
			}
			fileConfig.ObjectStorageUploadConfig = upload
			owners, err := newLoggerCompressedLogSpoolRuntimeBuilder(pkglogger.LoggerConfig{File: fileConfig})
			require.NoError(t, err)
			require.Empty(t, owners.OwnedRuntimes())
			require.NoError(t, owners.Close())
		})
	}
}

func TestCompressedLogSpoolRuntimeSharesBackendRootsAndBindsOnlyRequestedServices(t *testing.T) {
	upload := sharedCompressedLogSpoolTestUpload(t)
	otherUpload := upload
	const other objectstorage.UploadTarget = "other"
	otherUpload.Default.Targets = []objectstorage.UploadTarget{other, objectstorage.UploadTargetS3}
	owners, err := newCompressedLogSpoolRuntimeBuilder(upload, otherUpload)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owners.Close()) })
	require.Len(t, owners.OwnedRuntimes(), 2)
	runtimes, err := buildCompressedLogSpoolRuntimes(upload, owners)
	require.NoError(t, err)
	require.Len(t, runtimes, 1)
	otherRuntimes, err := buildCompressedLogSpoolRuntimes(otherUpload, owners)
	require.NoError(t, err)
	require.Len(t, otherRuntimes, 2)
	require.Same(t, runtimes[objectstorage.UploadTargetS3], otherRuntimes[objectstorage.UploadTargetS3])
	for _, service := range []objectstorage.UploadTarget{objectstorage.UploadTargetS3, other} {
		require.Equal(t, service, otherRuntimes[service].Root().Service())
	}
	require.NotEqual(t, otherRuntimes[objectstorage.UploadTargetS3].Root().Path(), otherRuntimes[other].Root().Path())
	require.NoError(t, owners.Close())
	replacement, err := newCompressedLogSpoolRuntimeBuilder(otherUpload)
	require.NoError(t, err, "全backendの初期化ownerを解放する")
	require.NoError(t, replacement.Close())
}

func TestCompressedLogSpoolRuntimeSharesLogAndBackendFailureArchive(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		t.Run(string(mode), func(t *testing.T) {
			upload := sharedCompressedLogSpoolTestUpload(t)
			archiveUpload := upload.Default
			archiveUpload.CompressedLogFileUploadSpool = upload.CompressedLogFileUploadSpool
			failure := durable.FailureStoreConfig{
				DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
				DurableStoreConfig: durable.DurableStoreConfig{FileArchiveConfig: durable.FileArchiveConfig{
					LifecycleMode: mode, ObjectStorageUploadConfig: archiveUpload,
				}},
			}
			base := newActivationFileConfig(t, "log-and-jsonl")
			base.ObjectStorageUploadConfig = upload
			cfg := pkglogger.LoggerConfig{File: base}
			setCheckpointStartupBackend(&cfg, pkglogger.LogOutputOpenSearch, failure)
			owners, err := newLoggerCompressedLogSpoolRuntimeBuilder(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, owners.Close()) })
			require.Len(t, owners.OwnedRuntimes(), 1)
			provider := loggerdurable.NewFailureStoreConfigAdapter(failure)
			fileRuntimes, err := buildCompressedLogSpoolRuntimes(upload, owners)
			require.NoError(t, err)
			archiveRuntimes, err := buildCompressedLogSpoolRuntimesForFailureStore(provider, owners)
			require.NoError(t, err)
			require.Same(t, fileRuntimes[objectstorage.UploadTargetS3], archiveRuntimes[objectstorage.UploadTargetS3])
			// .log出力を選択していなくても、backend archiveの退避先を準備します。
			require.NoError(t, owners.Close())
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
			cfg.File = nil
			archiveOnly, err := newLoggerCompressedLogSpoolRuntimeBuilder(cfg)
			require.NoError(t, err)
			require.Len(t, archiveOnly.OwnedRuntimes(), 1)
			archiveRuntimes, err = buildCompressedLogSpoolRuntimesForFailureStore(provider, archiveOnly)
			require.NoError(t, err)
			require.NotNil(t, archiveRuntimes[objectstorage.UploadTargetS3])
			require.NoError(t, archiveOnly.Close())
		})
	}
}

func TestDeliveryCompressedLogSpoolRuntimeIncludesBackendFailureArchive(t *testing.T) {
	upload := sharedCompressedLogSpoolTestUpload(t, func(config *objectstorage.CompressedLogFileUploadSpoolConfig) {
		config.LoggerContract = pkglogger.LoggerContractDelivery.String()
	})
	archiveUpload := upload.Default
	archiveUpload.CompressedLogFileUploadSpool = upload.CompressedLogFileUploadSpool
	failure := durable.DeliveryFailureStoreConfig{
		DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
		FileArchiveConfig: durable.DeliveryFileArchiveConfig{
			LifecycleMode: pkgfsstore.FileLifecycleModeInline, ObjectStorageUploadConfig: archiveUpload,
		},
	}
	var cfg pkglogger.DeliveryLoggerConfig
	setCheckpointStartupDeliveryBackend(&cfg, pkglogger.LogOutputOpenSearch, failure)
	owners, err := newDeliveryCompressedLogSpoolRuntimeBuilder(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owners.Close()) })
	provider := loggerdurable.NewDeliveryFailureStoreConfigAdapter(failure)
	runtimes, err := buildCompressedLogSpoolRuntimesForDeliveryFailureStore(provider, owners)
	require.NoError(t, err)
	require.NotNil(t, runtimes[objectstorage.UploadTargetS3])
}

func TestLoggerCompressedLogSpoolSharedByBaseAndInternalFilesUntilClose(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		t.Run(string(mode), func(t *testing.T) {
			base := checkpointStartupFileConfig(t, t.TempDir())
			base.LifecycleMode = mode
			base.ObjectStorageUploadConfig = sharedCompressedLogSpoolTestUpload(t)
			internal := dedicatedInternalLogFileConfig(base, t.TempDir())
			internal.ArchiveCheckpointConfig.SourcePrefix += "_internal"
			cfg := pkglogger.LoggerConfig{
				File:                    base,
				Timeout:                 time.Second,
				MaxLogEntryBytes:        pkglogger.DefaultMaxLogEntryBytes,
				InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{File: internal},
			}
			created, err := NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, created.Close()) })
			owned := created.(*topLevelLoggerRuntime)
			require.Len(t, owned.compressedLogSpoolRuntimes.runtimes, 1)
			_, err = owned.internalLogDestinations.runtimeOwnedFileLoggerSet.NewFileLogger(context.Background())
			require.NoError(t, err, "lazy internal fileは準備済みのrootへrouteだけを登録する")
			require.NoError(t, created.Close())
			replacement, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(base.ObjectStorageUploadConfig.CompressedLogFileUploadSpool, objectstorage.UploadTargetS3))
			require.NoError(t, err, "最後のtop-level Closeでrootを再取得できる")
			require.NoError(t, replacement.Close())
		})
	}
}

func TestLoggerCompressedLogSpoolConstructionRollbackReleasesRoot(t *testing.T) {
	base := newActivationFileConfig(t, "spool-rollback")
	base.ObjectStorageUploadConfig = sharedCompressedLogSpoolTestUpload(t)
	internal := dedicatedInternalLogFileConfig(base, t.TempDir())
	internal.FsstoreConfig.Layout.LogDir = ""
	_, err := newLoggerCore(context.Background(), pkglogger.LoggerConfig{
		Timeout:                 time.Second,
		MaxLogEntryBytes:        pkglogger.DefaultMaxLogEntryBytes,
		File:                    base,
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{File: internal},
	}, pkgcloudwatchlogs.StreamRoutingNone, nil, observer.NopTracer{}, observer.NopMeter{}, nil)
	// 共有rootを準備した後のinternal log file構築失敗でrollbackします。timeoutなど
	// 他の不備で早期に失敗するとroot解放の経路を通らないため、errorを特定します。
	require.ErrorIs(t, err, ErrInternalLogFileLoggerSetCreation)
	require.ErrorIs(t, err, pkgfsstore.ErrEmptyLogDirectory)
	replacement, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(base.ObjectStorageUploadConfig.CompressedLogFileUploadSpool, objectstorage.UploadTargetS3))
	require.NoError(t, err)
	require.NoError(t, replacement.Close())
}

func TestLoggerCompressedLogSpoolFanoutFailureClosesPreparedRunners(t *testing.T) {
	base := checkpointStartupFileConfig(t, t.TempDir())
	base.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	base.ObjectStorageUploadConfig = sharedCompressedLogSpoolTestUpload(t)
	cfg := pkglogger.LoggerConfig{File: base}
	require.NoError(t, prepareLoggerArchiveCheckpointDirectory(cfg))
	_, err := newLoggerCore(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, observer.NopTracer{}, observer.NopMeter{}, nil)
	require.ErrorIs(t, err, multi.ErrInvalidTimeout)
	replacement, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(base.ObjectStorageUploadConfig.CompressedLogFileUploadSpool, objectstorage.UploadTargetS3))
	require.NoError(t, err, "fanout構築失敗でも先に作成したrunnerのrouteを回収する")
	require.NoError(t, replacement.Close())
}

// 後続specのlifecycle runnerの検証（Interval=0）が失敗した場合、構築を中止する前に、先に
// 構築したspecのrunnerが圧縮ログspoolへ登録したrouteを閉じます。本番の構築と同じくspoolの
// 所有者（RootRuntimeBuilder）を渡すため、先行specはrouteを登録します。routeが残っていると、
// 所有者がrootを閉じても保存処理が続き、保存先の予約が解除されません。
// sync・asyncはnewLogFileMaintenanceRuntime、DeliveryはnewDeliveryFileMaintenanceRuntimeが
// それぞれ同じrollbackを持ちます。
func TestLoggerCompressedLogSpoolLaterMaintenanceFailureClosesEarlierRoutes(t *testing.T) {
	for _, test := range []struct {
		name string
		// buildは有効な先行specと周期設定が不正な後続specで構築し、spoolのupload設定とerrorを返します。
		build func(t *testing.T, owners *compressedlog.RootRuntimeBuilder) (objectstorage.LogFileUploadConfig, error)
	}{
		{
			name: "sync_and_async",
			build: func(t *testing.T, owners *compressedlog.RootRuntimeBuilder) (objectstorage.LogFileUploadConfig, error) {
				base := checkpointStartupFileConfig(t, t.TempDir())
				base.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
				base.ObjectStorageUploadConfig = sharedCompressedLogSpoolTestUpload(t)
				require.NoError(t, prepareLoggerArchiveCheckpointDirectory(pkglogger.LoggerConfig{File: base}))
				first := base.LifecycleRunnerConfig()
				second := *first
				second.RunnerConfig.Interval = 0
				_, err := newLogFileMaintenanceRuntime([]logFileMaintenanceRunnerSpec{
					{source: "first", config: first},
					{source: "invalid-second", config: &second},
				}, observer.NopTracer{}, observer.NopMeter{}, owners)
				return base.ObjectStorageUploadConfig, err
			},
		},
		{
			name: "delivery",
			build: func(t *testing.T, owners *compressedlog.RootRuntimeBuilder) (objectstorage.LogFileUploadConfig, error) {
				base := checkpointStartupDeliveryFileConfig(t, t.TempDir())
				base.LifecycleRunnerSettings.ObjectStorageUploadConfig = sharedCompressedLogSpoolTestUpload(t, func(config *objectstorage.CompressedLogFileUploadSpoolConfig) {
					config.LoggerContract = pkglogger.LoggerContractDelivery.String()
				})
				require.NoError(t, prepareDeliveryArchiveCheckpointDirectory(pkglogger.DeliveryLoggerConfig{File: base}))
				first := base.LifecycleRunnerConfig()
				second := *first
				second.RunnerConfig.Interval = 0
				_, err := newDeliveryFileMaintenanceRuntime([]deliveryFileMaintenanceRunnerSpec{
					{source: "first", config: first},
					{source: "invalid-second", config: &second},
				}, observer.NopTracer{}, observer.NopMeter{}, owners)
				return base.LifecycleRunnerSettings.ObjectStorageUploadConfig, err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			owners := compressedlog.NewRootRuntimeBuilder()
			upload, err := test.build(t, owners)
			// 失敗は後続specの周期設定の不備です（先行specのspool構築は成功しています）。
			require.ErrorIs(t, err, pkgfsstore.ErrInvalidAppendLogRunnerInterval)
			runtimes := owners.OwnedRuntimes()
			require.Len(t, runtimes, 1, "the first spec must have prepared its compressed log spool")
			// 失敗した場合もgoroutineを残さないよう、試験の終わりに強制終了します。
			t.Cleanup(func() { _ = runtimes[0].ForceClose() })

			require.NoError(t, owners.Close())
			// 先行specのrouteが閉じていれば、所有者のCloseで保存処理を終えて保存先の予約を解除します。
			require.True(t, runtimes[0].ShutdownComplete(), "routes registered by the first spec were left open after the construction failure")
			replacement, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(upload.CompressedLogFileUploadSpool, objectstorage.UploadTargetS3))
			require.NoError(t, err, "the compressed log spool root must be released")
			require.NoError(t, replacement.Close())
		})
	}
}

func sharedCompressedLogSpoolTestUpload(t *testing.T, options ...func(*objectstorage.CompressedLogFileUploadSpoolConfig)) objectstorage.LogFileUploadConfig {
	t.Helper()
	target := checkpointStartupUploadConfig()
	target.CompressedLogFileUploadFailureStore.DurableType = []failurestore.DurableType{failurestore.DurableTypeSpool}
	spoolConfig := objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  filepath.Join(t.TempDir(), "uncreated"),
		Target:                     "shared-spool",
		LoggerContract:             pkglogger.LoggerContractSync.String(),
		RunnerInterval:             time.Hour,
		LifecycleInterval:          time.Hour,
		CircuitBreakerOpenCooldown: time.Minute,
	}
	for _, option := range options {
		option(&spoolConfig)
	}
	return objectstorage.LogFileUploadConfig{
		Enabled:                      true,
		Default:                      target,
		CompressedLogFileUploadSpool: objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig),
	}
}
