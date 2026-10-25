// Package heterolog provides structured logging to files, standard output,
// Slack, OpenSearch, Loki, CloudWatch Logs, and ClickHouse.
//
// Start with DefaultConfig and New, and call Close or Shutdown when the
// application exits to flush buffered logs and release owned resources.
// Configuration is explicit: importing this package or constructing a logger
// does not load environment files, install signal handlers, or change the
// process-wide slog logger or OpenTelemetry providers. To configure loggers
// from HETEROLOG_ environment variables and an env file, call LoadEnvConfig.
//
// Advanced backend and durability settings are available in pkg/logger and
// its related public packages. A configuration must not be modified while a
// logger constructed from it is in use.
package heterolog

// Keep configured IANA time zones available in minimal containers and Windows.
import _ "time/tzdata"
