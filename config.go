package heterolog

import (
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/otel"
)

// Config configures a synchronous logger. See pkg/logger for backend settings.
type Config = pkglogger.LoggerConfig

// AsyncConfig configures a logger with asynchronous best-effort delivery.
type AsyncConfig = pkglogger.AsyncBestEffortMultiLoggerConfig

// DeliveryConfig configures a logger that returns a report for each destination.
type DeliveryConfig = pkglogger.DeliveryLoggerConfig

// Level is a heterolog severity level, independent of slog's numeric levels.
type Level = pkglogger.LogLevel

const (
	LevelDebug = pkglogger.LogLevelDebug
	LevelInfo  = pkglogger.LogLevelInfo
	LevelWarn  = pkglogger.LogLevelWarn
	LevelError = pkglogger.LogLevelError
	// DefaultMaxLogEntryBytes is the default size limit before backend fan-out.
	DefaultMaxLogEntryBytes = pkglogger.DefaultMaxLogEntryBytes
)

// DefaultConfig returns independent configuration for INFO and higher logs on
// stdout (WARN and ERROR on stderr). Timestamps use UTC and RFC3339Nano.
func DefaultConfig() Config {
	return Config{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Timeout:               5 * time.Second,
		MaxLogEntryBytes:      DefaultMaxLogEntryBytes,
		Stdout: &pkglogger.StdoutLoggerConfig{
			RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: LevelInfo},
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

// DefaultAsyncConfig returns stdout configuration with at most 16 parallel
// backend operations. Logging methods do not return delivery errors.
func DefaultAsyncConfig() AsyncConfig {
	cfg := DefaultConfig()
	return AsyncConfig{
		LogOutputDestinations: cfg.LogOutputDestinations,
		Timeout:               cfg.Timeout,
		MaxLogEntryBytes:      cfg.MaxLogEntryBytes,
		MaxParallel:           16,
		Stdout:                cfg.Stdout,
		Resource:              cfg.Resource,
	}
}

// DefaultDeliveryConfig returns stdout configuration for reporting delivery.
// Delivery loggers do not filter severity or sample entries.
func DefaultDeliveryConfig() DeliveryConfig {
	cfg := DefaultConfig()
	return DeliveryConfig{
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
