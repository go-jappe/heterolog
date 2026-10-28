package heterolog

import (
	"context"
	"net/http"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	"github.com/go-jappe/heterolog/internal/presentation/http/admin"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// MinAdminTokenLength is the minimum length in bytes of an admin API token.
const MinAdminTokenLength = pkgadmin.MinTokenLength

var (
	// ErrAdminTokenTooShort reports an admin API token shorter than
	// MinAdminTokenLength. The error never contains the token.
	ErrAdminTokenTooShort = admin.ErrTokenTooShort
	// ErrAdminNoLoggers reports an admin API configuration without loggers.
	ErrAdminNoLoggers = outputsettings.ErrNoLoggers
	// ErrAdminAuditLoggerRequired reports an admin API without the delivery
	// logger that records its audit log.
	ErrAdminAuditLoggerRequired = outputsettings.ErrNoAuditLogger
	// ErrAdminServerAddrRequired reports an AdminServerConfig without Addr.
	ErrAdminServerAddrRequired = admin.ErrServerAddrRequired
	// ErrAdminServerInsecure reports an admin server that would listen on a
	// non-loopback address without TLS, which would send the token over the
	// network in clear text.
	ErrAdminServerInsecure = admin.ErrServerInsecure
	// ErrAdminServerTLSIncomplete reports an AdminServerConfig that sets only
	// one of TLSCertFile and TLSKeyFile.
	ErrAdminServerTLSIncomplete = admin.ErrServerTLSIncomplete
)

// SettingsController is a logger whose output settings can change while it
// runs. *Runtime and *AsyncRuntime implement it.
type SettingsController = pkglogger.SettingsController

var (
	_ SettingsController = (*Runtime)(nil)
	_ SettingsController = (*AsyncRuntime)(nil)
)

// AdminHandlerConfig configures NewAdminHandler: the bearer Token (at least
// MinAdminTokenLength bytes) and the Loggers by the names used in the API.
type AdminHandlerConfig = pkgadmin.HandlerConfig

// AdminServerConfig configures StartAdminServer. Addr is the TCP address to
// listen on; with an empty host, such as ":9100", the server listens on
// 127.0.0.1 only. Listening on a non-loopback address requires TLSCertFile and
// TLSKeyFile. Token is as in AdminHandlerConfig.
type AdminServerConfig = pkgadmin.ServerConfig

// AdminServer serves the admin API until Shutdown or Close stops it.
type AdminServer struct {
	// serverは、起動したgoroutineが状態を書き込むため、複製せずpointerで持ちます。
	server *admin.Server
}

// NewAdminHandler returns an http.Handler for the admin API, which reads and
// changes the level and sampling of the outputs of running loggers. Mount it
// on your own server, behind your own TLS when it is reachable over a network;
// StartAdminServer starts a dedicated server instead. The handler serves:
//
//	GET   /settings  returns the current settings of every logger
//	PATCH /settings  changes the fields given in the JSON body
//
// Every request must send "Authorization: Bearer <Token>". PATCH requests are
// processed one at a time, and a PATCH checks all of its changes before
// applying any of them. Changes take effect from the next entry and are kept
// in memory only, so a restart returns to the configured settings.
//
// audit records the audit log of the API: each change request, the settings
// when the change starts and after it, and every rejected request; only a
// successful GET is not recorded. Every record has the field log_kind=audit
// and the request ID, which the response also sends in X-Request-Id. When the
// settings after a change do not match the request, the change is not
// reverted: the mismatch is recorded at ERROR and the response is 500. When a
// record does not reach every output of audit, the change still goes ahead,
// the response carries a warning, and this is recorded at ERROR too. Use a
// delivery logger made for the audit log, such as the one of
// EnvConfig.NewAdminAudit, and close it after the handler is no longer used.
// It returns ErrAdminAuditLoggerRequired if audit is nil.
func NewAdminHandler(cfg AdminHandlerConfig, audit *DeliveryRuntime) (http.Handler, error) {
	if audit == nil || audit.DeliveryLogger == nil {
		return nil, ErrAdminAuditLoggerRequired
	}
	return admin.NewHandler(cfg, audit)
}

// StartAdminServer starts a dedicated HTTP server for the admin API of
// NewAdminHandler and returns once it listens. loggers maps the names used in
// the API to the loggers whose settings it reads and changes, and audit records
// the audit log as in NewAdminHandler. The server listens on a loopback address
// unless cfg names another host and enables TLS. Call Shutdown or Close to stop
// it, and close audit after that.
func StartAdminServer(cfg AdminServerConfig, loggers map[string]SettingsController, audit *DeliveryRuntime) (*AdminServer, error) {
	if audit == nil || audit.DeliveryLogger == nil {
		return nil, ErrAdminAuditLoggerRequired
	}
	server, err := admin.StartServer(cfg, loggers, audit)
	if err != nil {
		return nil, err
	}
	return &AdminServer{server: server}, nil
}

// Addr returns the address the server listens on, including the port chosen
// when AdminServerConfig.Addr asked for port 0.
func (s *AdminServer) Addr() string { return s.server.Addr() }

// Shutdown stops accepting connections and waits until active requests finish
// or ctx ends. It returns ctx's error if ctx ends first, and otherwise the
// error that stopped the server, if any.
func (s *AdminServer) Shutdown(ctx context.Context) error { return s.server.Shutdown(ctx) }

// Close stops the server immediately and closes active connections.
func (s *AdminServer) Close() error { return s.server.Close() }
