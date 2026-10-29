package loggerruntime_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestConstructorsRejectInvalidStdoutPresentation(t *testing.T) {
	for _, name := range []string{"sync", "async", "delivery"} {
		t.Run(name, func(t *testing.T) {
			for _, invalid := range []string{"level", "trace depth", "debug trace depth"} {
				if name == "delivery" && invalid == "level" {
					continue
				}
				t.Run(invalid, func(t *testing.T) {
					mutate := func(level *pkglogger.LogLevel, body *pkglogger.LogBodyConfig) {
						switch invalid {
						case "level":
							*level = 999
						case "trace depth":
							body.ErrorTraceDepth = -1
						case "debug trace depth":
							body.ErrorTraceDepthDebugMode = -1
						}
					}
					var created io.Closer
					var err error
					switch name {
					case "sync":
						cfg := loggerruntime.DefaultConfig()
						mutate(&cfg.Stdout.RuntimeConfig.LogLevel, &cfg.Stdout.LogBodyConfig)
						created, err = loggerruntime.New(context.Background(), &recordingBuilder{}, cfg)
					case "async":
						cfg := loggerruntime.DefaultAsyncConfig()
						mutate(&cfg.Stdout.RuntimeConfig.LogLevel, &cfg.Stdout.LogBodyConfig)
						created, err = loggerruntime.NewAsync(context.Background(), &recordingBuilder{}, cfg)
					case "delivery":
						cfg := loggerruntime.DefaultDeliveryConfig()
						mutate(nil, &cfg.Stdout.LogBodyConfig)
						created, err = loggerruntime.NewDelivery(context.Background(), &recordingBuilder{}, cfg)
					}
					if err == nil {
						_ = created.Close()
						t.Fatalf("constructor accepted invalid %s", invalid)
					}
					field, kind := pkglogger.ConfigFieldLogLevel, pkglogger.ErrInvalidLogLevel
					switch invalid {
					case "trace depth":
						field, kind = pkglogger.ConfigFieldErrorTraceDepth, pkglogger.ErrNegativeErrorTraceDepth
					case "debug trace depth":
						field, kind = pkglogger.ConfigFieldErrorTraceDepthDebugMode, pkglogger.ErrNegativeErrorTraceDepth
					}
					if !hasConfigFieldError(err, pkglogger.LogOutputStdout, field, kind) {
						t.Fatalf("constructor error does not identify bad stdout setting: %v", err)
					}
				})
			}
		})
	}
}

func TestConstructorValidatesEverySelectedBackendLevel(t *testing.T) {
	invalidRuntime := pkglogger.RuntimeConfig{LogLevel: 999}
	for _, destination := range pkglogger.AllLogOutputDestinations() {
		t.Run(destination.String(), func(t *testing.T) {
			cfg := loggerruntime.DefaultConfig()
			cfg.Stdout = nil
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{destination}
			switch destination {
			case pkglogger.LogOutputFile:
				cfg.File = &pkglogger.FileLoggerConfig{RuntimeConfig: invalidRuntime}
			case pkglogger.LogOutputStdout:
				cfg.Stdout = &pkglogger.StdoutLoggerConfig{RuntimeConfig: invalidRuntime}
			case pkglogger.LogOutputSlack:
				cfg.Slack = &pkglogger.SlackLoggerConfig{RuntimeConfig: invalidRuntime}
			case pkglogger.LogOutputOpenSearch:
				cfg.OpenSearch = &pkglogger.OpenSearchConfig{OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{RuntimeConfig: invalidRuntime}}
			case pkglogger.LogOutputLoki:
				cfg.Loki = &pkglogger.LokiConfig{LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{RuntimeConfig: invalidRuntime}}
			case pkglogger.LogOutputCloudWatchLogs:
				cfg.CloudWatchLogs = &pkglogger.CloudWatchLogsConfig{CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{RuntimeConfig: invalidRuntime}}
			case pkglogger.LogOutputClickHouse:
				cfg.ClickHouse = &pkglogger.ClickHouseConfig{ClickHouseLoggerBaseConfig: pkglogger.ClickHouseLoggerBaseConfig{RuntimeConfig: invalidRuntime}}
			}
			created, err := loggerruntime.New(context.Background(), &recordingBuilder{}, cfg)
			if err == nil {
				_ = created.Close()
				t.Fatal("accepted invalid backend level")
			}
			if !hasConfigFieldError(err, destination, pkglogger.ConfigFieldLogLevel, pkglogger.ErrInvalidLogLevel) {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestConstructorOnlyValidatesActiveBackendPresentation(t *testing.T) {
	cfg := loggerruntime.DefaultConfig()
	cfg.File = &pkglogger.FileLoggerConfig{
		RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: 999},
		LogBodyConfig: pkglogger.LogBodyConfig{ErrorTraceDepth: -1},
	}
	created, err := loggerruntime.New(context.Background(), &recordingBuilder{}, cfg)
	if err != nil {
		t.Fatalf("unused backend rejected: %v", err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Stdout.HandlerConfig = handler.HandlerConfig{OnErrorConfig: []string{"file"}}
	created, err = loggerruntime.New(context.Background(), &recordingBuilder{}, cfg)
	if err == nil {
		_ = created.Close()
		t.Fatal("active fallback with invalid presentation accepted")
	}
	if !hasConfigFieldError(err, pkglogger.LogOutputFile, pkglogger.ConfigFieldLogLevel, pkglogger.ErrInvalidLogLevel) {
		t.Fatalf("active fallback error: %v", err)
	}
}

// hasConfigFieldErrorは、errに、backendのfieldをkindの不正として示すConfigFieldErrorが
// 含まれるかを返します。errors.Joinでまとめた複数の不正からも探します。
func hasConfigFieldError(
	err error,
	backend pkglogger.LogOutputDestination,
	field pkglogger.ConfigField,
	kind error,
) bool {
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Backend == backend && fieldErr.Field == field && errors.Is(fieldErr, kind) {
			return true
		}
	}
	return false
}
