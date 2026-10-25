package logger

import (
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

type fileLifecycleTestCompressionCodec struct{}

func (*fileLifecycleTestCompressionCodec) Format() fsstore.CompressFormat { return "br" }
func (*fileLifecycleTestCompressionCodec) Compress(_, _ *os.File) error   { return nil }
func (*fileLifecycleTestCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return nil
}

func TestFileLoggerConfigLifecycleRunnerConfigUsesCurrentCanonicalConfig(t *testing.T) {
	wantRunnerConfig := fsstore.AppendLogRunnerConfig{
		Interval:    2 * time.Minute,
		StopOnError: true,
		Supervisor: fsstore.AppendLogRunnerSupervisorConfig{
			Enabled:      true,
			RestartDelay: 3 * time.Second,
			MaxRestarts:  4,
		},
	}
	cfg := &FileLoggerConfig{
		LifecycleMode:           fsstore.FileLifecycleModeRunner,
		LifecycleRunnerSettings: wantRunnerConfig,
		FsstoreConfig: fsstore.Config{
			Layout: fsstore.FsstoreConfig{LogPath: "/first"},
			Lifecycle: fsstore.FileLifecycleConfig{
				ErrorPurge: &fsstore.FilePurgeConfig{MaxFileAge: 30},
				DebugPurge: &fsstore.FilePurgeConfig{MaxFileAge: 3},
			},
		},
	}

	cfg.FsstoreConfig.Layout.LogPath = "/updated"
	got := cfg.LifecycleRunnerConfig()
	if got == nil {
		t.Fatal("LifecycleRunnerConfig() = nil")
	}
	if got.FsstoreConfig.Layout.LogPath != "/updated" {
		t.Fatalf("runner log path = %q, want current canonical path", got.FsstoreConfig.Layout.LogPath)
	}
	if got.RunnerConfig != wantRunnerConfig {
		t.Fatalf("runner config = %+v, want %+v", got.RunnerConfig, wantRunnerConfig)
	}
	if got.FsstoreConfig.Lifecycle.ErrorPurge.MaxFileAge != 30 || got.FsstoreConfig.Lifecycle.DebugPurge.MaxFileAge != 3 {
		t.Fatalf("runner lost role purge configuration: %+v", got.FsstoreConfig.Lifecycle)
	}
}

func TestDeliveryFileLoggerConfigLifecycleRunnerConfigDerivesSharedSettings(t *testing.T) {
	codec := &fileLifecycleTestCompressionCodec{}
	wantRunnerConfig := fsstore.AppendLogRunnerConfig{
		Interval: 5 * time.Minute,
		Supervisor: fsstore.AppendLogRunnerSupervisorConfig{
			Enabled:      true,
			RestartDelay: 6 * time.Second,
			MaxRestarts:  7,
		},
	}
	cfg := &DeliveryFileLoggerConfig{
		LifecycleMode: fsstore.FileLifecycleModeRunner,
		FsstoreConfig: fsstore.DeliveryFileWriteConfig{
			Layout: fsstore.FsstoreConfig{LogPath: "/updated"},
			Rotation: fsstore.FileRotationConfig{
				EnableRotate: true,
				MaxFileSize:  42,
			},
			Duration: fsstore.DurationConfig{WriteFileWarnThresholdDuration: time.Second},
			Retry:    fsstore.RetryConfig{MaxWriteAttempts: 3},
		},
		LifecycleRunnerSettings: DeliveryFileLifecycleRunnerSettings{
			MaxFileBackups:   7,
			ErrorPurge:       &fsstore.FilePurgeConfig{MaxFileAge: 30},
			DebugPurge:       &fsstore.FilePurgeConfig{MaxFileAge: 3},
			CompressionCodec: codec,
			RunnerConfig:     wantRunnerConfig,
		},
	}

	got := cfg.LifecycleRunnerConfig()
	if got.FsstoreConfig.Layout.LogPath != "/updated" ||
		got.FsstoreConfig.Lifecycle.MaxFileSize != 42 ||
		got.FsstoreConfig.Duration.WriteFileWarnThresholdDuration != time.Second ||
		got.FsstoreConfig.Retry.MaxWriteAttempts != 3 ||
		got.FsstoreConfig.Lifecycle.MaxFileBackups != 7 ||
		got.FsstoreConfig.Lifecycle.ErrorPurge.MaxFileAge != 30 ||
		got.FsstoreConfig.Lifecycle.DebugPurge.MaxFileAge != 3 ||
		got.FsstoreConfig.CompressionCodec != codec ||
		got.RunnerConfig != wantRunnerConfig {
		t.Fatalf("derived delivery lifecycle config = %+v", got.FsstoreConfig)
	}
}
