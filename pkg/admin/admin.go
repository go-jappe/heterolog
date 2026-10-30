// Package admin holds the configuration types of the admin API, which reads and
// changes the level and sampling of the outputs of running loggers and reports
// the configuration of loggers and telemetries. The heterolog package serves
// the API with NewAdminHandler and StartAdminServer, which record every change
// request in an audit logger.
package admin

import (
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
}
