package heterolog

import (
	"context"
	"errors"
	"fmt"

	appconfig "github.com/go-jappe/heterolog/internal/config"
)

// ErrEnvLoggerDisabled reports that EnvConfig was asked for a logger that the
// environment disabled with HETEROLOG_ENABLE_LOGGING,
// HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING or
// HETEROLOG_ENABLE_DELIVERY_LOGGING set to false.
var ErrEnvLoggerDisabled = errors.New("logger is disabled by the environment")

// EnvFileError reports that LoadEnvConfig could not read the env file. Path is
// the file and Err the cause; errors.Is(err, fs.ErrNotExist) reports a missing
// file.
type EnvFileError = appconfig.EnvFileError

// InvalidEnvironmentValueError reports a value that LoadEnvConfig rejected
// after reading it. Key names the variable and Err the reason. Value is empty
// for values that may contain credentials. Values that are not of the expected
// type, such as a number, are reported as a pkg/env VariableError.
type InvalidEnvironmentValueError = appconfig.InvalidEnvironmentValueError

// RenamedEnvironmentKeyError reports a variable whose name was corrected. Old is
// the name that is set and New the current name. LoadEnvConfig does not read
// the old name as an alias, because ignoring it would silently drop a setting.
type RenamedEnvironmentKeyError = appconfig.RenamedEnvironmentKeyError

// EnvConfig holds the settings that LoadEnvConfig read from HETEROLOG_
// environment variables. New, NewAsync and NewDelivery construct loggers from
// them; Config, AsyncConfig and DeliveryConfig return the settings to adjust
// before calling the package-level constructors.
//
// The environment does not configure tracing or a memory pressure monitor.
// Pass WithObservers and, when HETEROLOG_USE_MEMORY_PRESSURE_* settings are
// enabled, WithMemoryPressureMonitor as options.
type EnvConfig struct {
	app appconfig.AppConfig
}

// LoadEnvConfig reads the HETEROLOG_ settings from the process environment and,
// when path is not empty, from the env file at path. A variable that is set in
// the process environment takes precedence over the file. LoadEnvConfig does
// not modify the process environment and reads no other file: it ignores
// HETEROLOG_ENV_PATH and a .env file that path does not name. The AWS SDK's
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN and AWS_REGION
// supply defaults for the S3 and CloudWatch Logs settings.
//
// Settings that are not set use the defaults documented in .env.example, so an
// empty environment yields loggers that write to stdout. LoadEnvConfig checks
// the settings of every enabled logger and of the loggers together, such as
// directories that two stores would share, and returns all problems joined
// with errors.Join. Use errors.As with EnvFileError,
// InvalidEnvironmentValueError, RenamedEnvironmentKeyError or pkg/env
// VariableError to identify a setting.
func LoadEnvConfig(path string) (*EnvConfig, error) {
	app, err := appconfig.LoadConfigFile(path)
	if err != nil {
		return nil, err
	}
	errs := []error{appconfig.ValidateApplicationConfig(app)}
	if app.LoggerEnabled {
		_, err := appconfig.MapLoggerConfig(app)
		errs = append(errs, err)
	}
	if app.AsyncLoggerEnabled {
		_, err := appconfig.MapAsyncLoggerConfig(app)
		errs = append(errs, err)
	}
	if app.DeliveryLoggerEnabled {
		_, err := appconfig.MapDeliveryLoggerConfig(app)
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &EnvConfig{app: app}, nil
}

// Config returns the synchronous logger settings and the options for the spool
// replay settings and CloudWatch Logs stream routing read from the
// environment. Pass both to New. Each call returns new settings. It returns an
// error wrapping ErrEnvLoggerDisabled when HETEROLOG_ENABLE_LOGGING is false.
func (c *EnvConfig) Config() (Config, []Option, error) {
	if !c.app.LoggerEnabled {
		return Config{}, nil, fmt.Errorf("%w: HETEROLOG_ENABLE_LOGGING is false", ErrEnvLoggerDisabled)
	}
	cfg, err := appconfig.MapLoggerConfig(c.app)
	if err != nil {
		return Config{}, nil, err
	}
	return cfg, []Option{
		WithSpool(c.app.SpoolConfig),
		WithCloudWatchLogsStreamRouting(c.app.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// AsyncConfig returns the asynchronous logger settings and the options for
// NewAsync, like Config. It returns an error wrapping ErrEnvLoggerDisabled when
// HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING is false.
func (c *EnvConfig) AsyncConfig() (AsyncConfig, []Option, error) {
	if !c.app.AsyncLoggerEnabled {
		return AsyncConfig{}, nil, fmt.Errorf("%w: HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING is false", ErrEnvLoggerDisabled)
	}
	cfg, err := appconfig.MapAsyncLoggerConfig(c.app)
	if err != nil {
		return AsyncConfig{}, nil, err
	}
	return cfg, []Option{
		WithSpool(c.app.SpoolConfig),
		WithCloudWatchLogsStreamRouting(c.app.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// DeliveryConfig returns the delivery logger settings and the options for
// NewDelivery. Delivery loggers do not replay spools, so the options carry only
// the CloudWatch Logs stream routing. It returns an error wrapping
// ErrEnvLoggerDisabled when HETEROLOG_ENABLE_DELIVERY_LOGGING is false.
func (c *EnvConfig) DeliveryConfig() (DeliveryConfig, []Option, error) {
	if !c.app.DeliveryLoggerEnabled {
		return DeliveryConfig{}, nil, fmt.Errorf("%w: HETEROLOG_ENABLE_DELIVERY_LOGGING is false", ErrEnvLoggerDisabled)
	}
	cfg, err := appconfig.MapDeliveryLoggerConfig(c.app)
	if err != nil {
		return DeliveryConfig{}, nil, err
	}
	return cfg, []Option{
		WithCloudWatchLogsStreamRouting(c.app.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// New constructs a synchronous logger from the settings and options that
// Config returns. opts are applied after those options, so they can replace
// them or add WithObservers and WithMemoryPressureMonitor.
func (c *EnvConfig) New(ctx context.Context, opts ...Option) (*Runtime, error) {
	cfg, envOpts, err := c.Config()
	if err != nil {
		return nil, err
	}
	return New(ctx, cfg, append(envOpts, opts...)...)
}

// NewAsync constructs an asynchronous best-effort logger from the settings and
// options that AsyncConfig returns, followed by opts.
func (c *EnvConfig) NewAsync(ctx context.Context, opts ...Option) (*AsyncRuntime, error) {
	cfg, envOpts, err := c.AsyncConfig()
	if err != nil {
		return nil, err
	}
	return NewAsync(ctx, cfg, append(envOpts, opts...)...)
}

// NewDelivery constructs a delivery-reporting logger from the settings and
// options that DeliveryConfig returns, followed by opts.
func (c *EnvConfig) NewDelivery(ctx context.Context, opts ...Option) (*DeliveryRuntime, error) {
	cfg, envOpts, err := c.DeliveryConfig()
	if err != nil {
		return nil, err
	}
	return NewDelivery(ctx, cfg, append(envOpts, opts...)...)
}
