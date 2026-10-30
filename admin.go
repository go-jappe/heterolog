package heterolog

import (
	"context"
	"net/http"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	"github.com/go-jappe/heterolog/internal/presentation/http/admin"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
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
	// ErrAdminSharedLiveSettings reports that two outputs of the loggers given
	// to the admin API share one pkg/logger LiveSettings, set through
	// RuntimeConfig.Live, in one logger or in different loggers. A logger given
	// under two names shares all of its outputs. A change of one output would
	// also change the other, which the audit log could neither record nor
	// verify.
	ErrAdminSharedLiveSettings = outputsettings.ErrSharedLiveSettings
	// ErrAdminDuplicateName reports a name used in more than one map of
	// AdminMembers.
	ErrAdminDuplicateName = admin.ErrDuplicateName
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
// MinAdminTokenLength bytes) and the Members served by the API.
type AdminHandlerConfig = pkgadmin.HandlerConfig

// AdminMembers are the loggers and telemetries the admin API serves, each
// under the name used in the API: Loggers, whose settings it reads and changes
// and whose configuration it reports, and DeliveryLoggers and Telemetries,
// whose configuration it reports. A name must not appear in more than one map.
type AdminMembers = pkgadmin.Members

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
// changes the level and sampling of the outputs of running loggers and reports
// the configuration of loggers and telemetries. Mount it on your own server,
// behind your own TLS when it is reachable over a network; StartAdminServer
// starts a dedicated server instead. The handler serves:
//
//	GET   /settings       returns the current settings of every logger
//	PATCH /settings       changes the fields given in the JSON body
//	GET   /config         returns the configuration of every member
//	GET   /config/<path>  returns one value of that configuration
//
// Every request must send "Authorization: Bearer <Token>". PATCH requests are
// processed one at a time, and a PATCH checks all of its changes before
// applying any of them. Changes take effect from the next entry and are kept
// in memory only, so a restart returns to the configured settings.
//
// GET /config returns the configuration each member was created with, as the
// structs of Config, AsyncConfig, DeliveryConfig and TelemetryConfig in JSON
// with snake_case keys. Tokens, passwords and keys are replaced by "***".
// Only members created by heterolog are reported; other implementations of
// SettingsController, DeliveryLogger and Telemetry are left out. Changes made
// through PATCH /settings are not part of it. GET /config/<path> walks the
// document by its keys and array indexes, for example
// /config/loggers/app/config/file/log_dir, and is 404 when the path does not
// exist, and GET /config?path=<path>&path=<path> returns several values in
// one request, as {"values": {"<path>": ...}}. It returns
// ErrAdminDuplicateName if a name is used in more than one map of
// cfg.Members.
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
//
// Each output of the loggers must have its own settings, so that every change
// is recorded and verified for the output it changes. It returns
// ErrAdminSharedLiveSettings if two outputs share one LiveSettings. Loggers
// created by New and NewAsync are checked; other implementations of
// SettingsController are not.
func NewAdminHandler(cfg AdminHandlerConfig, audit *DeliveryRuntime) (http.Handler, error) {
	if audit == nil || audit.DeliveryLogger == nil {
		return nil, ErrAdminAuditLoggerRequired
	}
	cfg.Members = adminMembers(cfg.Members)
	return admin.NewHandler(cfg, audit)
}

// StartAdminServer starts a dedicated HTTP server for the admin API of
// NewAdminHandler and returns once it listens. members are the loggers and
// telemetries served under the names used in the API, and audit records the
// audit log; both are checked as in NewAdminHandler. The server listens on a
// loopback address unless cfg names another host and enables TLS. Call
// Shutdown or Close to stop it, and close audit after that.
func StartAdminServer(cfg AdminServerConfig, members AdminMembers, audit *DeliveryRuntime) (*AdminServer, error) {
	if audit == nil || audit.DeliveryLogger == nil {
		return nil, ErrAdminAuditLoggerRequired
	}
	server, err := admin.StartServer(cfg, adminMembers(members), audit)
	if err != nil {
		return nil, err
	}
	return &AdminServer{server: server}, nil
}

// adminMembersは、heterologが作るloggerとtelemetryを、処理を受け持つ内部のruntimeに
// 置き換えた複製を返します。内部のruntimeは出力先ごとのLiveSettingsと、自分を作った
// 設定を示せるため、管理用APIは出力先どうしの共有を見つけ、設定の文書を作れます。設定の
// 読み取りと変更は、公開の型を通した場合と同じく内部のruntimeが行います。
func adminMembers(members AdminMembers) AdminMembers {
	result := AdminMembers{
		Loggers:         make(map[string]SettingsController, len(members.Loggers)),
		DeliveryLoggers: make(map[string]DeliveryLogger, len(members.DeliveryLoggers)),
		Telemetries:     make(map[string]pkgtelemetry.Telemetry, len(members.Telemetries)),
	}
	for name, logger := range members.Loggers {
		switch runtime := logger.(type) {
		case *Runtime:
			if runtime != nil {
				logger = runtime.impl()
			}
		case *AsyncRuntime:
			if runtime != nil {
				logger = runtime.impl()
			}
		}
		result.Loggers[name] = logger
	}
	for name, logger := range members.DeliveryLoggers {
		if runtime, ok := logger.(*DeliveryRuntime); ok && runtime != nil {
			logger = runtime.impl()
		}
		result.DeliveryLoggers[name] = logger
	}
	for name, telemetry := range members.Telemetries {
		if runtime, ok := telemetry.(*Telemetry); ok && runtime != nil {
			telemetry = runtime.impl()
		}
		result.Telemetries[name] = telemetry
	}
	return result
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
