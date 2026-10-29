package logger

import (
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/otel"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	pkgspoollifecycle "github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

// DefaultConfigは、INFO以上のlogをstdout（WARNとERRORはstderr）へ書く、呼び出しごとに
// 独立した設定を返します。時刻はUTCで、RFC3339Nanoの形式です。
func DefaultConfig() pkglogger.LoggerConfig {
	return pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Timeout:               5 * time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		Stdout: &pkglogger.StdoutLoggerConfig{
			RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo},
			TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat:        time.RFC3339Nano,
				ErrorTraceDepth:          8,
				ErrorTraceDepthDebugMode: 16,
			},
		},
		Resource: &otel.Resource{},
	}
}

// DefaultAsyncConfigは、backendの処理を最大16並列で行う、stdoutの設定を返します。
// logを記録するmethodは、送信のerrorを返しません。
func DefaultAsyncConfig() pkglogger.AsyncBestEffortMultiLoggerConfig {
	cfg := DefaultConfig()
	return pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: cfg.LogOutputDestinations,
		Timeout:               cfg.Timeout,
		MaxLogEntryBytes:      cfg.MaxLogEntryBytes,
		MaxParallel:           16,
		Stdout:                cfg.Stdout,
		Resource:              cfg.Resource,
	}
}

// DefaultDeliveryConfigは、送信結果を返すloggerのstdoutの設定を返します。Delivery
// loggerはsamplingをせず、severityで絞り込むのはSlackの出力だけです。
func DefaultDeliveryConfig() pkglogger.DeliveryLoggerConfig {
	cfg := DefaultConfig()
	return pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: cfg.LogOutputDestinations,
		Timeout:               cfg.Timeout,
		MaxLogEntryBytes:      cfg.MaxLogEntryBytes,
		Stdout: &pkglogger.DeliveryStdoutLoggerConfig{
			TimeConfig:    cfg.Stdout.TimeConfig,
			LogBodyConfig: cfg.Stdout.LogBodyConfig,
		},
		Resource: cfg.Resource,
	}
}

// DefaultSpoolConfigは、対応しているすべてのbackendの再送の設定を返します。保持して
// いる記録を暗黙に消さないよう、purgeは無効です。
func DefaultSpoolConfig() pkgspool.SpoolConfig {
	backend := pkgspoolrunner.BackendSpoolRunnerConfig{
		Runner: pkgspoolrunner.RunnerConfig{Interval: 100 * time.Millisecond},
		Worker: pkgspoolrunner.WorkerConfig{
			Finalize:      pkgspoolrunner.FinalizeConfig{RetryDelay: time.Second, MaxEntries: 4098},
			MaxRetryAfter: time.Hour,
		},
		LifeCycle: pkgspoollifecycle.LifecycleConfig{SizePurgeInterval: time.Minute},
	}
	return pkgspool.SpoolConfig{
		SupervisorConfig: pkgspoolsupervisor.RunnerSupervisorConfig{RestartDelay: time.Second},
		RunnerConfig: pkgspoolrunner.SpoolRunnerConfig{
			OpensearchConfig:     backend,
			LokiConfig:           backend,
			CloudWatchLogsConfig: backend,
			ClickHouseConfig:     backend,
		},
	}
}
