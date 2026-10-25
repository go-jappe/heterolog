package runner

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loglifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/stretchr/testify/require"
)

func TestNewLogFileRunnersUsesSupervisorFromTypedConfig(t *testing.T) {
	runnerConfig := supervisedAppendLogRunnerConfig()
	cfg := supervisedLogFileLifecycleConfig(t, runnerConfig)

	runners, err := NewLogFileRunners(loglifecycle.LogFileOptions{Config: cfg}, nil)

	require.NoError(t, err)
	require.Len(t, runners, 1)
	assertRunnerUsesTypedSupervisorConfig(t, runners[0], runnerConfig)
}

func TestNewLogFileRunnersAllowsCallerManagedStopOnErrorWithoutSupervisor(t *testing.T) {
	runnerConfig := pkgfsstore.AppendLogRunnerConfig{
		Interval:    37 * time.Millisecond,
		StopOnError: true,
	}
	cfg := supervisedLogFileLifecycleConfig(t, runnerConfig)

	runners, err := NewLogFileRunners(loglifecycle.LogFileOptions{Config: cfg}, nil)

	require.NoError(t, err)
	require.Len(t, runners, 1)
	require.NotNil(t, runners[0].Config)
	require.Equal(t, runnerConfig.Interval, runners[0].Config.Interval())
	require.True(t, runners[0].Config.StopOnError())
	require.Nil(t, runners[0].supervisor)
}

func TestNewDeliveryFileRunnersUsesSupervisorFromTypedConfig(t *testing.T) {
	runnerConfig := supervisedAppendLogRunnerConfig()
	logCfg := supervisedLogFileLifecycleConfig(t, runnerConfig)
	cfg := pkgfsstore.DeliveryFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
			Layout: logCfg.FsstoreConfig.Layout,
			Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
				UsageCacheTTL: time.Hour,
				EnableRotate:  true,
				RotationType:  pkgfsstore.FileRotationDaily,
				// delivery lifecycleはdefault系列の保持設定も検証するため、有効な下限値を使います。
				MaxFileBackups:     2,
				MaxFileAge:         1,
				MaxFileSize:        1 << 20,
				MaxDirSize:         10 << 20,
				EnableFileCompress: false,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
		RunnerConfig: runnerConfig,
	}

	runners, err := NewDeliveryFileRunners(loglifecycle.DeliveryFileOptions{Config: cfg}, nil)

	require.NoError(t, err)
	require.Len(t, runners, 1)
	assertRunnerUsesTypedSupervisorConfig(t, runners[0], runnerConfig)
}

func TestNewLogFileRunnersRejectsInvalidTypedRunnerConfigBeforeBuildingLifecycles(t *testing.T) {
	cfg := supervisedLogFileLifecycleConfig(t, pkgfsstore.AppendLogRunnerConfig{})
	// Runner設定が先に検証されることを確認するため、lifecycle側も不正値にする。
	cfg.FsstoreConfig.Layout.Target = ""

	runners, err := NewLogFileRunners(loglifecycle.LogFileOptions{Config: cfg}, nil)

	require.Nil(t, runners)
	require.ErrorIs(t, err, pkgfsstore.ErrInvalidAppendLogRunnerInterval)
}

func TestNewFailureArchiveRunnersWrapsEachDirectoryWithoutStartingIt(t *testing.T) {
	t.Parallel()
	cfg := failureArchiveFactoryConfig(t)
	provider := loggerdurable.NewFileArchiveConfigAdapter(cfg)
	callback := lifecycleRunReporterFunc(func(context.Context, loglifecycle.RunReport, error) {
		t.Error("factory must not start a lifecycle cycle")
	})
	reporter := &callback
	runners, err := NewFailureArchiveRunners(loglifecycle.FailureArchiveOptions{Config: provider, Reporter: reporter}, nil)
	require.NoError(t, err)
	require.Len(t, runners, 2)
	for index, terminalDir := range []string{cfg.DirCaseRetryable, cfg.DirCasePermanent} {
		want, err := failurearchive.GetFileArchiveDirectoryPath(provider, terminalDir)
		require.NoError(t, err)
		got, err := runners[index].DirectoryPath()
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, cfg.LifecycleRunnerConfig.Interval, runners[index].Config.Interval())
		require.Same(t, reporter, runners[index].Reporter)
		require.False(t, runners[index].running.Load())
	}
}

func TestNewFailureArchiveRunnersUsesIndependentSupervisorsFromTypedConfig(t *testing.T) {
	t.Parallel()
	cfg := failureArchiveFactoryConfig(t)
	cfg.LifecycleRunnerConfig = supervisedAppendLogRunnerConfig()
	provider := struct {
		loglifecycle.FailureArchiveRunnerConfigProvider
	}{
		loggerdurable.NewFileArchiveConfigAdapter(cfg),
	}
	runners, err := NewFailureArchiveRunners(loglifecycle.FailureArchiveOptions{Config: provider}, nil)
	require.NoError(t, err)
	require.Len(t, runners, 2)
	for _, runner := range runners {
		assertRunnerUsesTypedSupervisorConfig(t, runner, cfg.LifecycleRunnerConfig)
	}
	require.NotSame(t, runners[0].supervisor, runners[1].supervisor)
}

func TestNewFailureArchiveRunnersRejectsInvalidTypedRunnerConfigBeforeBuildingLifecycles(t *testing.T) {
	t.Parallel()
	cfg := failureArchiveFactoryConfig(t)
	cfg.LifecycleRunnerConfig = pkgfsstore.AppendLogRunnerConfig{}
	cfg.Target = ""
	runners, err := NewFailureArchiveRunners(loglifecycle.FailureArchiveOptions{Config: loggerdurable.NewFileArchiveConfigAdapter(cfg)}, nil)
	require.Nil(t, runners)
	require.ErrorIs(t, err, pkgfsstore.ErrInvalidAppendLogRunnerInterval)
}

func TestNewFailureArchiveRunnersRejectsNonRunnerLifecycleMode(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeDisable} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := failureArchiveFactoryConfig(t)
			cfg.LifecycleMode = mode
			runners, err := NewFailureArchiveRunners(loglifecycle.FailureArchiveOptions{Config: loggerdurable.NewFileArchiveConfigAdapter(cfg)}, nil)
			require.Nil(t, runners)
			require.ErrorIs(t, err, ErrFailureArchiveRunnerModeRequired)
			var modeErr *FailureArchiveRunnerModeError
			require.ErrorAs(t, err, &modeErr)
			require.Equal(t, mode, modeErr.Mode)
		})
	}
}

func failureArchiveFactoryConfig(t *testing.T) durable.FileArchiveConfig {
	t.Helper()
	checkpointDirectory := filepath.Join(resolvedTempDirForTest(t), "archive-checkpoints")
	require.NoError(t, checkpoint.PrepareDirectory(checkpointDirectory))
	return durable.FileArchiveConfig{
		Target: "failure-runner", MaxFileSize: 2 * 1024 * 1024,
		FailureFileArchivePath: resolvedTempDirForTest(t), Dir: "backend-failures",
		DirCaseRetryable: "retryable", DirCasePermanent: "permanent",
		FileNameFormat: pkgfsstore.FailureArchiveFileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
		EnableRotate: true, MaxFileBackups: 2, MaxFileAge: 36500, MaxDirSize: 20 * 1024 * 1024,
		UsageCacheTTL: time.Hour, EnableFileCompress: true, CompressFormat: pkgfsstore.CompressZstd,
		LifecycleMode: pkgfsstore.FileLifecycleModeRunner, RotationType: pkgfsstore.FileRotationDaily, RotationDayOfWeek: time.Sunday,
		MaxWriteAttempts: 1, FileOperationRetryWaitTime: time.Millisecond,
		LifecycleRunnerConfig:   pkgfsstore.AppendLogRunnerConfig{Interval: 15 * time.Minute},
		ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{Directory: checkpointDirectory, SourcePrefix: "failure_archive_test"},
		OverloadPolicyConfig: durable.FileArchiveOverloadPolicyConfig{
			OverloadPolicy: durable.OverloadPolicyDrop, SpillDestination: durable.SpillDestinationStdout,
			MinAvailableDiskSpace: durable.MinFileArchiveAvailableDiskSpace,
		},
	}
}

func supervisedAppendLogRunnerConfig() pkgfsstore.AppendLogRunnerConfig {
	return pkgfsstore.AppendLogRunnerConfig{
		Interval:    37 * time.Millisecond,
		StopOnError: true,
		Supervisor: pkgfsstore.AppendLogRunnerSupervisorConfig{
			Enabled:      true,
			RestartDelay: 19 * time.Millisecond,
			MaxRestarts:  4,
		},
	}
}

func supervisedLogFileLifecycleConfig(
	t *testing.T,
	runnerConfig pkgfsstore.AppendLogRunnerConfig,
) pkgfsstore.LogFileLifecycleRunnerConfig {
	t.Helper()
	return pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         "supervised-runner-factory",
				LogPath:        resolvedTempDirForTest(t),
				LogDir:         "logs",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxDirSize:         1024,
				EnableFileCompress: false,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
		RunnerConfig: runnerConfig,
	}
}

func assertRunnerUsesTypedSupervisorConfig(
	t *testing.T,
	runner *Runner,
	want pkgfsstore.AppendLogRunnerConfig,
) {
	t.Helper()
	require.NotNil(t, runner)
	require.NotNil(t, runner.Config)
	require.Equal(t, want.Interval, runner.Config.Interval())
	require.Equal(t, want.StopOnError, runner.Config.StopOnError())
	require.NotNil(t, runner.supervisor)
	require.Same(t, runner, runner.supervisor.runner)
	require.Equal(t, want.Supervisor.RestartDelay, runner.supervisor.config.RestartDelay())
	require.Equal(t, want.Supervisor.MaxRestarts, runner.supervisor.config.MaxRestarts())
}
