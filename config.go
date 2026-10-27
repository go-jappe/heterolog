package heterolog

import (
	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
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
func DefaultConfig() Config { return loggerruntime.DefaultConfig() }

// DefaultAsyncConfig returns stdout configuration with at most 16 parallel
// backend operations. Logging methods do not return delivery errors.
func DefaultAsyncConfig() AsyncConfig { return loggerruntime.DefaultAsyncConfig() }

// DefaultDeliveryConfig returns stdout configuration for reporting delivery.
// Delivery loggers do not sample entries, and only their Slack output filters
// severity.
func DefaultDeliveryConfig() DeliveryConfig { return loggerruntime.DefaultDeliveryConfig() }
