// Package admin holds the configuration types of the admin API, which reads and
// changes the level and sampling of the outputs of running loggers and reports
// the configuration of loggers and telemetries. The heterolog package serves
// the API with NewAdminHandler and StartAdminServer, which record every change
// request in an audit logger.
package admin

import (
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/telemetry"
)

// MinTokenLength is the minimum length in bytes of an admin API token.
const MinTokenLength = 32

// Members are the loggers and telemetries the admin API serves, each under the
// name used in the API. A name must not appear in more than one map.
type Members struct {
	// Loggers are the loggers whose settings the API reads and changes, and
	// whose configuration it reports. At least one is required.
	Loggers map[string]pkglogger.SettingsController
	// DeliveryLoggers are delivery loggers whose configuration the API reports.
	// They have no settings that change while running.
	DeliveryLoggers map[string]pkglogger.DeliveryLogger
	// Telemetries are telemetries whose configuration the API reports.
	Telemetries map[string]telemetry.Telemetry
}

// HandlerConfig configures the admin API handler.
type HandlerConfig struct {
	// Token is the secret that every request sends as
	// "Authorization: Bearer <Token>". It must be at least MinTokenLength bytes;
	// create it at random, for example with "openssl rand -hex 32".
	Token string
	// Members are the loggers and telemetries the API serves.
	Members Members
	// Status configures GET /status. The zero value uses the defaults of
	// DefaultStatusConfig.
	Status StatusConfig
}

// StatusConfig configures the status report of GET /status: how often the
// directories are scanned, and the thresholds of its warnings. A zero field
// uses the default of DefaultStatusConfig.
type StatusConfig struct {
	// DirectoryScanInterval is the least time between two scans that count the
	// files of the spool, log and failure archive directories. A scan starts in
	// the background when GET /status finds the last one older than this, so
	// the counts are approximate. DisableDirectoryScan turns the scans off; the
	// counts are then reported as not scanned.
	DirectoryScanInterval time.Duration
	DisableDirectoryScan  bool
	// DiskFreeWarningPercent and DiskFreeCriticalPercent are the free space of
	// a file system, in percent of its size, below which a warning or a
	// critical warning is reported.
	DiskFreeWarningPercent  float64
	DiskFreeCriticalPercent float64
	// DirectorySizeWarningPercent is the use of a directory, in percent of its
	// configured limit, from which a warning is reported; over the limit is
	// critical.
	DirectorySizeWarningPercent float64
	// BackendFailureWarningPercent is the share of records that could not be
	// sent to a destination, in percent, above which a warning is reported.
	BackendFailureWarningPercent float64
	// SlowRequest is the average duration of the requests to a destination
	// above which a warning is reported.
	SlowRequest time.Duration
}

// DefaultStatusConfig returns the default status settings: a scan every 5
// minutes, disk warnings at 10% and 5% free, directory warnings at 90% of the
// limit, backend warnings at 10% failures and requests slower than 5 seconds.
func DefaultStatusConfig() StatusConfig {
	return StatusConfig{
		DirectoryScanInterval:        5 * time.Minute,
		DiskFreeWarningPercent:       10,
		DiskFreeCriticalPercent:      5,
		DirectorySizeWarningPercent:  90,
		BackendFailureWarningPercent: 10,
		SlowRequest:                  5 * time.Second,
	}
}

// ServerConfig configures the dedicated admin API server.
type ServerConfig struct {
	// Addr is the TCP address to listen on, such as "127.0.0.1:9100". With an
	// empty host, such as ":9100", the server listens on 127.0.0.1 only. To
	// listen on a non-loopback address, name the host and set TLSCertFile and
	// TLSKeyFile.
	Addr string
	// Token is the bearer token that every request sends, as in HandlerConfig.
	Token string
	// TLSCertFile and TLSKeyFile are the PEM files of the server certificate and
	// its private key. Setting both serves HTTPS.
	TLSCertFile string
	TLSKeyFile  string
	// Status configures GET /status, as in HandlerConfig.
	Status StatusConfig
}
