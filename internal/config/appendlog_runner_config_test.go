package config

import (
	"errors"
	"testing"
	"time"

	configappendlogrunner "github.com/go-jappe/heterolog/internal/config/appendlogrunner"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPublicEnvExampleExposesApplicationWideAppendLogRunnerDefaults(t *testing.T) {
	wantValues := map[string]string{
		"HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND":                      "900",
		"HETEROLOG_ENABLE_APPENDLOG_RUNNER_STOP_ON_ERROR":                 "false",
		"HETEROLOG_ENABLE_APPENDLOG_RUNNER_SUPERVISOR":                    "false",
		"HETEROLOG_APPENDLOG_RUNNER_SUPERVISOR_RESTART_DELAY_MILLISECOND": "1000",
		"HETEROLOG_MAX_APPENDLOG_RUNNER_SUPERVISOR_RESTARTS":              "3",
	}

	// readPublicEnvExampleは同じkeyが複数回宣言されていればtestを失敗させる。
	values := readPublicEnvExample(t)
	for key, want := range wantValues {
		got, exists := values[key]
		if !exists {
			t.Errorf(".env.example is missing appendlog runner key %s", key)
			continue
		}
		if got != want {
			t.Errorf(".env.example %s = %q, want %q", key, got, want)
		}
	}
}

func TestLoadConfigReadsApplicationWideAppendLogRunnerConfig(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND=1234",
		"HETEROLOG_ENABLE_APPENDLOG_RUNNER_STOP_ON_ERROR=true",
		"HETEROLOG_ENABLE_APPENDLOG_RUNNER_SUPERVISOR=true",
		"HETEROLOG_APPENDLOG_RUNNER_SUPERVISOR_RESTART_DELAY_MILLISECOND=5678",
		"HETEROLOG_MAX_APPENDLOG_RUNNER_SUPERVISOR_RESTARTS=9",
	})

	want := configappendlogrunner.Config{
		Interval:    1234 * time.Second,
		StopOnError: true,
		Supervisor: configappendlogrunner.SupervisorConfig{
			Enabled:      true,
			RestartDelay: 5678 * time.Millisecond,
			MaxRestarts:  9,
		},
	}
	if got := cfg.AppendLogRunnerConfig; got != want {
		t.Fatalf("AppendLogRunnerConfig = %+v, want %+v", got, want)
	}
}

func TestAppendLogRunnerConfigMapsToEveryFileLifecycleRunner(t *testing.T) {
	raw := configappendlogrunner.Config{
		Interval:    17 * time.Second,
		StopOnError: true,
		Supervisor: configappendlogrunner.SupervisorConfig{
			Enabled:      true,
			RestartDelay: 23 * time.Second,
			MaxRestarts:  29,
		},
	}
	cfg := AppConfig{
		AppendLogRunnerConfig: raw,
		RuntimeEnv: configruntime.RuntimeEnv{
			Cloud: &configruntime.CloudInfo{},
		},
	}
	want := newAppendLogRunnerConfig(raw)

	configs := []struct {
		name string
		got  pkgfsstore.AppendLogRunnerConfig
	}{
		{
			name: "sync file",
			got:  NewFileLoggerConfig(&cfg).LifecycleRunnerSettings,
		},
		{
			name: "async file",
			got:  NewAsyncFileLoggerConfig(&cfg).LifecycleRunnerSettings,
		},
		{
			name: "delivery file",
			got:  NewDeliveryFileLoggerConfig(&cfg).LifecycleRunnerSettings.RunnerConfig,
		},
		{
			name: "sync opensearch failure archive",
			got: NewOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "async opensearch failure archive",
			got: NewAsyncOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "delivery opensearch failure archive",
			got: NewDeliveryOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "sync loki failure archive",
			got: NewLokiFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "async loki failure archive",
			got: NewAsyncLokiFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "delivery loki failure archive",
			got: NewDeliveryLokiFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "sync cloudwatchlogs failure archive",
			got: NewCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "async cloudwatchlogs failure archive",
			got: NewAsyncCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "delivery cloudwatchlogs failure archive",
			got: NewDeliveryCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "sync clickhouse failure archive",
			got: NewClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "async clickhouse failure archive",
			got: NewAsyncClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.LifecycleRunnerConfig,
		},
		{
			name: "delivery clickhouse failure archive",
			got: NewDeliveryClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.LifecycleRunnerConfig,
		},
	}

	for _, tt := range configs {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != want {
				t.Fatalf("appendlog runner config = %+v, want %+v", tt.got, want)
			}
		})
	}
}

func TestMapLoggerConfigRejectsInvalidActiveAppendLogRunnerConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  configappendlogrunner.Config
		wantErr error
	}{
		{
			name:    "zero interval",
			config:  configappendlogrunner.Config{},
			wantErr: pkgfsstore.ErrInvalidAppendLogRunnerInterval,
		},
		{
			name: "negative interval",
			config: configappendlogrunner.Config{
				Interval: -time.Nanosecond,
			},
			wantErr: pkgfsstore.ErrInvalidAppendLogRunnerInterval,
		},
		{
			name: "stop on error without supervisor",
			config: configappendlogrunner.Config{
				Interval:    time.Second,
				StopOnError: true,
			},
			wantErr: mappedvalidation.ErrAppendLogRunnerStopOnErrorRequiresSupervisor,
		},
		{
			name: "zero supervisor restart delay",
			config: configappendlogrunner.Config{
				Interval: time.Second,
				Supervisor: configappendlogrunner.SupervisorConfig{
					Enabled:     true,
					MaxRestarts: 1,
				},
			},
			wantErr: pkgfsstore.ErrInvalidAppendLogSupervisorRestartDelay,
		},
		{
			name: "negative supervisor restart delay",
			config: configappendlogrunner.Config{
				Interval: time.Second,
				Supervisor: configappendlogrunner.SupervisorConfig{
					Enabled:      true,
					RestartDelay: -time.Nanosecond,
					MaxRestarts:  1,
				},
			},
			wantErr: pkgfsstore.ErrInvalidAppendLogSupervisorRestartDelay,
		},
		{
			name: "zero supervisor max restarts",
			config: configappendlogrunner.Config{
				Interval: time.Second,
				Supervisor: configappendlogrunner.SupervisorConfig{
					Enabled:      true,
					RestartDelay: time.Second,
				},
			},
			wantErr: pkgfsstore.ErrInvalidAppendLogSupervisorMaxRestarts,
		},
		{
			name: "negative supervisor max restarts",
			config: configappendlogrunner.Config{
				Interval: time.Second,
				Supervisor: configappendlogrunner.SupervisorConfig{
					Enabled:      true,
					RestartDelay: time.Second,
					MaxRestarts:  -1,
				},
			},
			wantErr: pkgfsstore.ErrInvalidAppendLogSupervisorMaxRestarts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := AppConfig{
				AppendLogRunnerConfig: tt.config,
				MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
				RuntimeEnv: configruntime.RuntimeEnv{
					Cloud: &configruntime.CloudInfo{},
				},
			}
			cfg.Logger.LogOutputDestinations = []pkglogger.LogOutputDestination{
				pkglogger.LogOutputFile,
			}
			cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode =
				pkgfsstore.FileLifecycleModeRunner

			_, err := MapLoggerConfig(cfg)
			requireMappedConfigError(t, err, "logger file", mappedvalidation.ConfigSectionAppendLogRunner, tt.wantErr)
		})
	}
}

func TestMapLoggerConfigIgnoresUnusedAppendLogRunnerConfig(t *testing.T) {
	cfg := AppConfig{
		AppendLogRunnerConfig: configappendlogrunner.Config{Interval: -time.Second},
		RuntimeEnv: configruntime.RuntimeEnv{
			Cloud: &configruntime.CloudInfo{},
		},
	}
	cfg.Logger.LogOutputDestinations = []pkglogger.LogOutputDestination{
		pkglogger.LogOutputFile,
	}
	cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode =
		pkgfsstore.FileLifecycleModeInline

	_, err := MapLoggerConfig(cfg)
	if errors.Is(err, pkgfsstore.ErrInvalidAppendLogRunnerInterval) {
		t.Fatalf("MapLoggerConfig() validated unused appendlog runner config: %v", err)
	}
}
