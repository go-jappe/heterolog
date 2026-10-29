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

// 同期・非同期loggerのconstructorは、Builderを呼ぶ前に、稼働中に変えられる出力先の
// levelを検証します。本文の表示の設定はBuilderの実装が検証します。
func TestConstructorsRejectInvalidStdoutLevel(t *testing.T) {
	for _, name := range []string{"sync", "async"} {
		t.Run(name, func(t *testing.T) {
			builder := &recordingBuilder{}
			var created io.Closer
			var err error
			switch name {
			case "sync":
				cfg := stdoutConfig()
				cfg.Stdout.RuntimeConfig.LogLevel = 999
				created, err = loggerruntime.New(context.Background(), builder, cfg)
			case "async":
				cfg := stdoutAsyncConfig()
				cfg.Stdout.RuntimeConfig.LogLevel = 999
				created, err = loggerruntime.NewAsync(context.Background(), builder, cfg)
			}
			if err == nil {
				_ = created.Close()
				t.Fatal("constructor accepted invalid level")
			}
			if !hasConfigFieldError(err, pkglogger.LogOutputStdout, pkglogger.ConfigFieldLogLevel, pkglogger.ErrInvalidLogLevel) {
				t.Fatalf("constructor error does not identify bad stdout setting: %v", err)
			}
			if builder.built != 0 {
				t.Errorf("builder built %d loggers from an invalid level", builder.built)
			}
		})
	}
}

func TestConstructorValidatesEverySelectedBackendLevel(t *testing.T) {
	invalidRuntime := pkglogger.RuntimeConfig{LogLevel: 999}
	for _, destination := range pkglogger.AllLogOutputDestinations() {
		t.Run(destination.String(), func(t *testing.T) {
			cfg := stdoutConfig()
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

func TestConstructorOnlyValidatesActiveBackendSettings(t *testing.T) {
	cfg := stdoutConfig()
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
		t.Fatal("active fallback with invalid level accepted")
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
