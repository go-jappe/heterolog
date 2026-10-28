package heterolog

import (
	"context"

	"github.com/go-jappe/heterolog/internal/presentation/envconfig"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
)

// ErrEnvLoggerDisabled reports that EnvConfig was asked for a logger that the
// environment disabled with HETEROLOG_ENABLE_LOGGING,
// HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING or
// HETEROLOG_ENABLE_DELIVERY_LOGGING set to false.
var ErrEnvLoggerDisabled = envconfig.ErrLoggerDisabled

// ErrEnvAdminAuditNotEnabled reports that EnvConfig has no settings for the
// audit logger of the admin API because HETEROLOG_ADMIN_ADDR is empty.
var ErrEnvAdminAuditNotEnabled = envconfig.ErrAdminAuditNotEnabled

// EnvFileError reports that LoadEnvConfig could not read the env file. Path is
// the file and Err the cause; errors.Is(err, fs.ErrNotExist) reports a missing
// file.
type EnvFileError = pkgenv.EnvFileError

// InvalidEnvironmentValueError reports a value that LoadEnvConfig rejected
// after reading it. Key names the variable and Err the reason. Value is empty
// for values that may contain credentials. Values that are not of the expected
// type, such as a number, are reported as a pkg/env VariableError, except
// values that may contain credentials, which are reported as this error.
type InvalidEnvironmentValueError = pkgenv.InvalidEnvironmentValueError

// RenamedEnvironmentKeyError reports a variable whose name was corrected. Old is
// the name that is set and New the current name. LoadEnvConfig does not read
// the old name as an alias, because ignoring it would silently drop a setting.
type RenamedEnvironmentKeyError = pkgenv.RenamedEnvironmentKeyError

// EnvConfig holds the settings that LoadEnvConfig read from HETEROLOG_
// environment variables. New, NewAsync and NewDelivery construct loggers from
// them; Config, AsyncConfig and DeliveryConfig return the settings to adjust
// before calling the package-level constructors.
//
// The environment does not configure tracing; pass WithObservers as an option.
// A spool that a HETEROLOG_USE_MEMORY_PRESSURE_* setting enables judges memory
// pressure with HETEROLOG_MEMORY_CRITICAL_RATIO,
// HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB and
// HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS, unless WithMemoryPressureMonitor
// supplies a monitor.
type EnvConfig struct {
	// configは処理を受け持つ内部の設定です。zero値のEnvConfigではnilです。
	config *envconfig.EnvConfig
}

// LoadEnvConfig reads the HETEROLOG_ settings from the process environment and,
// when path is not empty, from the env file at path. A variable that is set in
// the process environment takes precedence over the file. LoadEnvConfig does
// not modify the process environment and reads no other env file: it ignores
// HETEROLOG_ENV_PATH and a .env file that path does not name. The AWS SDK's
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN and AWS_REGION
// supply defaults for the S3 and CloudWatch Logs settings.
//
// Settings that are not set use the defaults documented in .env.example, so an
// empty environment yields loggers that write to stdout. LoadEnvConfig checks
// the settings of every enabled logger and of the loggers together, such as
// directories that two stores would share, and returns the problems it finds
// joined with errors.Join. Some problems, such as a value of the wrong type,
// stop the later checks, so fixing them can reveal others. Use errors.As with
// EnvFileError, InvalidEnvironmentValueError, RenamedEnvironmentKeyError or
// pkg/env VariableError to identify a setting.
func LoadEnvConfig(path string) (*EnvConfig, error) {
	config, err := envconfig.Load(path)
	if err != nil {
		return nil, err
	}
	return &EnvConfig{config: config}, nil
}

// implは、処理を渡す内部の設定を返します。zero値のEnvConfigには内部の設定のzero値を
// 返し、内部の型のzero値と同じく振る舞うようにします。
func (c *EnvConfig) impl() *envconfig.EnvConfig {
	if c.config == nil {
		return &envconfig.EnvConfig{}
	}
	return c.config
}

// Config returns the synchronous logger settings and the options for the spool
// replay settings and CloudWatch Logs stream routing read from the
// environment. Pass both to New. Each call returns a new Config, but slices in
// it, such as LogOutputDestinations, can share their backing arrays with the
// EnvConfig, so replace a slice instead of changing its elements. It returns
// an error wrapping ErrEnvLoggerDisabled when HETEROLOG_ENABLE_LOGGING is
// false.
func (c *EnvConfig) Config() (Config, []Option, error) { return c.impl().Config() }

// AsyncConfig returns the asynchronous logger settings and the options for
// NewAsync, like Config. It returns an error wrapping ErrEnvLoggerDisabled when
// HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING is false.
func (c *EnvConfig) AsyncConfig() (AsyncConfig, []Option, error) { return c.impl().AsyncConfig() }

// DeliveryConfig returns the delivery logger settings and the options for
// NewDelivery. Delivery loggers do not replay spools, so the options carry only
// the CloudWatch Logs stream routing. It returns an error wrapping
// ErrEnvLoggerDisabled when HETEROLOG_ENABLE_DELIVERY_LOGGING is false.
func (c *EnvConfig) DeliveryConfig() (DeliveryConfig, []Option, error) {
	return c.impl().DeliveryConfig()
}

// New constructs a synchronous logger from the settings and options that
// Config returns. opts are applied after those options, so they can replace
// them or add WithObservers and WithMemoryPressureMonitor.
func (c *EnvConfig) New(ctx context.Context, opts ...Option) (*Runtime, error) {
	return newRuntime(c.impl().New(ctx, opts...))
}

// NewAsync constructs an asynchronous best-effort logger from the settings and
// options that AsyncConfig returns, followed by opts.
func (c *EnvConfig) NewAsync(ctx context.Context, opts ...Option) (*AsyncRuntime, error) {
	return newAsyncRuntime(c.impl().NewAsync(ctx, opts...))
}

// NewDelivery constructs a delivery-reporting logger from the settings and
// options that DeliveryConfig returns, followed by opts.
func (c *EnvConfig) NewDelivery(ctx context.Context, opts ...Option) (*DeliveryRuntime, error) {
	return newDeliveryRuntime(c.impl().NewDelivery(ctx, opts...))
}

// AdminServerConfig returns the admin server settings read from
// HETEROLOG_ADMIN_ADDR, HETEROLOG_ADMIN_TOKEN, HETEROLOG_ADMIN_TLS_CERT_FILE and
// HETEROLOG_ADMIN_TLS_KEY_FILE, and whether HETEROLOG_ADMIN_ADDR enables the
// server. LoadEnvConfig has already checked them. Pass the settings to
// StartAdminServer together with the audit logger of NewAdminAudit.
func (c *EnvConfig) AdminServerConfig() (AdminServerConfig, bool) {
	return c.impl().AdminServerConfig()
}

// AdminAuditConfig returns the settings of the delivery logger that records the
// audit log of the admin API, and the options for NewAdminAudit. Its outputs
// are HETEROLOG_ADMIN_AUDIT_LOG_TYPE, by default stdout and file, and the
// HETEROLOG_ADMIN_AUDIT_* settings give where each output records, such as the
// file directory, by default audit_logs. The other settings, such as endpoints,
// credentials and timeouts, are those of the delivery logger
// (HETEROLOG_DELIVERY_*), even when HETEROLOG_ENABLE_DELIVERY_LOGGING is false.
// The audit logger keeps its own directories: its files are written to one file
// per period for every level, are not compressed, uploaded or removed, and its
// failure archives use their own directories. It returns
// ErrEnvAdminAuditNotEnabled when HETEROLOG_ADMIN_ADDR is empty.
func (c *EnvConfig) AdminAuditConfig() (DeliveryConfig, []Option, error) {
	return c.impl().AdminAuditConfig()
}

// NewAdminAudit constructs the audit logger of the admin API from the settings
// and options that AdminAuditConfig returns, followed by opts. Pass it to
// StartAdminServer or NewAdminHandler, and close it after the admin API stops.
func (c *EnvConfig) NewAdminAudit(ctx context.Context, opts ...Option) (*DeliveryRuntime, error) {
	return newDeliveryRuntime(c.impl().NewAdminAudit(ctx, opts...))
}
