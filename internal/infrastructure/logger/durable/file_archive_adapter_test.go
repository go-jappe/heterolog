package durable

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFileArchiveConfigAdaptersExposeInternalLogConfig(t *testing.T) {
	handlers := handler.HandlerConfig{
		OnErrorConfig: []string{"stdout", "slack"},
		OnWarnConfig:  []string{"file"},
		OnInfoConfig:  []string{"stdout"},
		OnDebugConfig: []string{"slack", "file"},
	}
	for _, debug := range []bool{false, true} {
		cfg := pkgdurable.FileArchiveConfig{
			HandlerConfig:            handlers,
			IsDebug:                  debug,
			ErrorTraceDepth:          6,
			ErrorTraceDepthDebugMode: 12,
		}
		providers := map[string]FileArchiveConfigProvider{
			"direct":   NewFileArchiveConfigAdapter(cfg),
			"standard": NewDurableStoreConfigAdapter(pkgdurable.DurableStoreConfig{FileArchiveConfig: cfg}).FileArchiveConfig(),
			"delivery": NewDeliveryDurableStoreConfigAdapter(pkgdurable.DeliveryFileArchiveConfig{
				HandlerConfig:            handlers,
				IsDebug:                  debug,
				ErrorTraceDepth:          6,
				ErrorTraceDepthDebugMode: 12,
			}).FileArchiveConfig(),
		}
		for name, adapter := range providers {
			got := adapter.Handler()
			if !reflect.DeepEqual(got.OnError(), handlers.OnErrorConfig) ||
				!reflect.DeepEqual(got.OnWarn(), handlers.OnWarnConfig) ||
				!reflect.DeepEqual(got.OnInfo(), handlers.OnInfoConfig) ||
				!reflect.DeepEqual(got.OnDebug(), handlers.OnDebugConfig) {
				t.Fatalf("%s handler routing = error:%v warn:%v info:%v debug:%v", name, got.OnError(), got.OnWarn(), got.OnInfo(), got.OnDebug())
			}
			if adapter.IsDebug() != debug || adapter.ErrorTraceDepth() != 6 || adapter.ErrorTraceDepthDebugMode() != 12 {
				t.Fatalf("%s internal log config = debug:%v trace:%d debugTrace:%d", name, adapter.IsDebug(), adapter.ErrorTraceDepth(), adapter.ErrorTraceDepthDebugMode())
			}
		}
	}
}

func TestFileArchiveConfigAdapterExposesConsumerConfig(t *testing.T) {
	cfg := pkgdurable.FileArchiveConfig{
		ObjectStorageUploadConfig: objectstorage.UploadTargetConfig{
			S3: objectstorage.S3Config{Bucket: "failure-archive"},
		},
		ArchiveCheckpointConfig: fsstore.ArchiveCheckpointConfig{Directory: "checkpoints"},
		OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
			OverloadPolicy:        pkgdurable.OverloadPolicyDegrade,
			SpillDestination:      pkgdurable.SpillDestinationStdout,
			MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
		},
	}
	adapter := NewFileArchiveConfigAdapter(cfg)
	if got := adapter.ObjectStorageUpload().S3.Bucket; got != "failure-archive" {
		t.Fatalf("ObjectStorageUpload().S3.Bucket = %q", got)
	}
	if got := adapter.ArchiveCheckpoint().Directory; got != "checkpoints" {
		t.Fatalf("ArchiveCheckpoint().Directory = %q", got)
	}
	overload := adapter.OverloadPolicyConfig()
	if overload.OverloadPolicy() != cfg.OverloadPolicyConfig.OverloadPolicy ||
		overload.SpillDestination() != cfg.OverloadPolicyConfig.SpillDestination ||
		overload.MinAvailableDiskSpace() != cfg.OverloadPolicyConfig.MinAvailableDiskSpace {
		t.Fatalf("OverloadPolicyConfig() = %+v, want %+v", overload, cfg.OverloadPolicyConfig)
	}
}

func TestFileArchiveStorageProviderDoesNotRequireLoggerConfig(t *testing.T) {
	providerType := reflect.TypeOf((*failurearchive.FileArchiveConfigProvider)(nil)).Elem()
	for _, name := range []string{"Handler", "IsDebug", "ErrorTraceDepth", "ErrorTraceDepthDebugMode", "ObjectStorageUpload", "ArchiveCheckpoint", "OverloadPolicyConfig", "LifecycleRunnerConfig"} {
		if _, exists := providerType.MethodByName(name); exists {
			t.Errorf("storage provider requires logger method %s", name)
		}
	}
}

func TestNewFileArchiveConfigAdapterMapsDurableConfig(t *testing.T) {
	t.Parallel()

	cfg := pkgdurable.FileArchiveConfig{
		Target:                     "search-app",
		LifecycleMode:              fsstore.FileLifecycleModeRunner,
		RotationType:               fsstore.FileRotationWeekly,
		RotationDayOfWeek:          time.Monday,
		FileNameFormat:             fsstore.FailureArchiveFileNameFormat,
		FileDateFormat:             fsstore.FileDateFormat,
		MaxFileSize:                2 * 1024 * 1024,
		UsageCacheTTL:              23 * time.Minute,
		MaxDirSize:                 20 * 1024 * 1024,
		MaxFileAge:                 7,
		MaxFileBackups:             5,
		EnableFileCompress:         true,
		CompressFormat:             fsstore.CompressGzip,
		FailureFileArchivePath:     "/var/lib/archive",
		Dir:                        "backend",
		DirCaseRetryable:           "retryable",
		DirCasePermanent:           "permanent",
		EnableRotate:               true,
		MaxWriteRetries:            2,
		FileOperationRetryWaitTime: 50 * time.Millisecond,
	}

	adapter := NewFileArchiveConfigAdapter(cfg)

	if adapter.Target() != cfg.Target {
		t.Fatalf("Target = %q, want %q", adapter.Target(), cfg.Target)
	}
	if adapter.LifecycleMode() != cfg.LifecycleMode {
		t.Fatalf("LifecycleMode = %q, want %q", adapter.LifecycleMode(), cfg.LifecycleMode)
	}
	if adapter.RotationType() != cfg.RotationType {
		t.Fatalf("RotationType = %q, want %q", adapter.RotationType(), cfg.RotationType)
	}
	if adapter.RotationDayOfWeek() != cfg.RotationDayOfWeek {
		t.Fatalf("RotationDayOfWeek = %v, want %v", adapter.RotationDayOfWeek(), cfg.RotationDayOfWeek)
	}
	if adapter.FileNameFormat() != cfg.FileNameFormat {
		t.Fatalf("FileNameFormat = %q, want %q", adapter.FileNameFormat(), cfg.FileNameFormat)
	}
	if adapter.FileDateFormat() != cfg.FileDateFormat {
		t.Fatalf("FileDateFormat = %q, want %q", adapter.FileDateFormat(), cfg.FileDateFormat)
	}
	if adapter.MaxFileSize() != cfg.MaxFileSize {
		t.Fatalf("MaxFileSize = %d, want %d", adapter.MaxFileSize(), cfg.MaxFileSize)
	}
	if adapter.UsageCacheTTL() != cfg.UsageCacheTTL {
		t.Fatalf(
			"UsageCacheTTL = %s, want %s",
			adapter.UsageCacheTTL(),
			cfg.UsageCacheTTL,
		)
	}
	if adapter.MaxDirSize() != cfg.MaxDirSize {
		t.Fatalf("MaxDirSize = %d, want %d", adapter.MaxDirSize(), cfg.MaxDirSize)
	}
	if adapter.MaxFileAge() != cfg.MaxFileAge {
		t.Fatalf("MaxFileAge = %d, want %d", adapter.MaxFileAge(), cfg.MaxFileAge)
	}
	if adapter.MaxFileBackups() != cfg.MaxFileBackups {
		t.Fatalf("MaxFileBackups = %d, want %d", adapter.MaxFileBackups(), cfg.MaxFileBackups)
	}
	if adapter.EnableFileCompress() != cfg.EnableFileCompress {
		t.Fatalf("EnableFileCompress = %v, want %v", adapter.EnableFileCompress(), cfg.EnableFileCompress)
	}
	if adapter.CompressFormat() != cfg.CompressFormat {
		t.Fatalf("CompressFormat = %q, want %q", adapter.CompressFormat(), cfg.CompressFormat)
	}
	if adapter.ArchivePath() != cfg.FailureFileArchivePath {
		t.Fatalf("ArchivePath = %q, want %q", adapter.ArchivePath(), cfg.FailureFileArchivePath)
	}
	if adapter.Dir() != cfg.Dir {
		t.Fatalf("Dir = %q, want %q", adapter.Dir(), cfg.Dir)
	}
	if adapter.DirCaseRetryable() != cfg.DirCaseRetryable {
		t.Fatalf("DirCaseRetryable = %q, want %q", adapter.DirCaseRetryable(), cfg.DirCaseRetryable)
	}
	if adapter.DirCasePermanent() != cfg.DirCasePermanent {
		t.Fatalf("DirCasePermanent = %q, want %q", adapter.DirCasePermanent(), cfg.DirCasePermanent)
	}
	if adapter.EnableRotate() != cfg.EnableRotate {
		t.Fatalf("EnableRotate = %v, want %v", adapter.EnableRotate(), cfg.EnableRotate)
	}
	if adapter.MaxWriteRetries() != cfg.MaxWriteRetries {
		t.Fatalf("MaxWriteRetries = %d, want %d", adapter.MaxWriteRetries(), cfg.MaxWriteRetries)
	}
	if adapter.FileOperationRetryWaitTime() != cfg.FileOperationRetryWaitTime {
		t.Fatalf("FileOperationRetryWaitTime = %s, want %s", adapter.FileOperationRetryWaitTime(), cfg.FileOperationRetryWaitTime)
	}
}

func TestNewFileArchiveConfigAdapterDefaultsEmptyLifecycleModeToInline(t *testing.T) {
	t.Parallel()

	adapter := NewFileArchiveConfigAdapter(pkgdurable.FileArchiveConfig{})
	if got := adapter.LifecycleMode(); got != fsstore.FileLifecycleModeInline {
		t.Fatalf("LifecycleMode = %q, want %q", got, fsstore.FileLifecycleModeInline)
	}
}

func TestNewFileArchiveConfigAdapterPreservesZeroUsageCacheTTL(t *testing.T) {
	t.Parallel()

	adapter := NewFileArchiveConfigAdapter(pkgdurable.FileArchiveConfig{})
	if got := adapter.UsageCacheTTL(); got != 0 {
		t.Fatalf("UsageCacheTTL() = %s, want zero passthrough", got)
	}
}
