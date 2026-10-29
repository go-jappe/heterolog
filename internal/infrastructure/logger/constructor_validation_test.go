package logger

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
)

// constructorは、正でないTimeoutとMaxParallelを、出所と項目を示すConfigFieldErrorで
// 拒否します。
func TestConstructorsReportNonPositiveLimitsAsConfigFieldErrors(t *testing.T) {
	ctx := context.Background()
	constructors := map[string]func(timeout time.Duration) (io.Closer, error){
		"logger": func(timeout time.Duration) (io.Closer, error) {
			cfg := DefaultConfig()
			cfg.Timeout = timeout
			return NewLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil)
		},
		"async logger": func(timeout time.Duration) (io.Closer, error) {
			cfg := DefaultAsyncConfig()
			cfg.Timeout = timeout
			return NewAsyncLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil)
		},
		"delivery logger": func(timeout time.Duration) (io.Closer, error) {
			cfg := DefaultDeliveryConfig()
			cfg.Timeout = timeout
			return NewDeliveryLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil)
		},
	}
	for source, construct := range constructors {
		t.Run(source, func(t *testing.T) {
			for _, timeout := range []time.Duration{0, -time.Second} {
				requireConfigFieldError(t, closeIfConstructed(construct(timeout)), source, pkglogger.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)
			}
		})
	}
	// zero値の設定にも、正のTimeoutはありません。
	err := closeIfConstructed(NewLogger(ctx, pkglogger.LoggerConfig{}, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil))
	requireConfigFieldError(t, err, "logger", pkglogger.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)

	for _, maxParallel := range []int{0, -1} {
		cfg := DefaultAsyncConfig()
		cfg.MaxParallel = maxParallel
		err := closeIfConstructed(NewAsyncLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil))
		requireConfigFieldError(t, err, "async logger", pkglogger.ConfigFieldMaxParallel, pkglogger.ErrNonPositiveSetting)
	}
}

// CloudWatch Logsを使わない設定でも、対応していないstream分割の指定は拒否します。
func TestConstructorsRejectUnsupportedStreamRoutingWithoutCloudWatchLogs(t *testing.T) {
	ctx := context.Background()
	const routing pkgcloudwatchlogs.StreamRouting = "invalid"
	for name, construct := range map[string]func() (io.Closer, error){
		"logger": func() (io.Closer, error) {
			return NewLogger(ctx, DefaultConfig(), routing, nil, nil, nil, nil)
		},
		"async logger": func() (io.Closer, error) {
			return NewAsyncLogger(ctx, DefaultAsyncConfig(), routing, nil, nil, nil, nil)
		},
		"delivery logger": func() (io.Closer, error) {
			return NewDeliveryLogger(ctx, DefaultDeliveryConfig(), routing, nil, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := closeIfConstructed(construct())
			var routingErr *pkgcloudwatchlogs.InvalidStreamRoutingError
			if !errors.Is(err, pkgcloudwatchlogs.ErrInvalidStreamRouting) || !errors.As(err, &routingErr) {
				t.Fatalf("invalid stream routing error = %v, want ErrInvalidStreamRouting", err)
			}
		})
	}
}

func TestConstructorsRejectInvalidStdoutBodyConfig(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"sync", "async", "delivery"} {
		t.Run(name, func(t *testing.T) {
			for _, invalid := range []string{"trace depth", "debug trace depth"} {
				t.Run(invalid, func(t *testing.T) {
					mutate := func(body *pkglogger.LogBodyConfig) {
						switch invalid {
						case "trace depth":
							body.ErrorTraceDepth = -1
						case "debug trace depth":
							body.ErrorTraceDepthDebugMode = -1
						}
					}
					var err error
					switch name {
					case "sync":
						cfg := DefaultConfig()
						mutate(&cfg.Stdout.LogBodyConfig)
						err = closeIfConstructed(NewLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil))
					case "async":
						cfg := DefaultAsyncConfig()
						mutate(&cfg.Stdout.LogBodyConfig)
						err = closeIfConstructed(NewAsyncLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil))
					case "delivery":
						cfg := DefaultDeliveryConfig()
						mutate(&cfg.Stdout.LogBodyConfig)
						err = closeIfConstructed(NewDeliveryLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil))
					}
					field := pkglogger.ConfigFieldErrorTraceDepth
					if invalid == "debug trace depth" {
						field = pkglogger.ConfigFieldErrorTraceDepthDebugMode
					}
					if !hasConfigFieldError(err, pkglogger.LogOutputStdout, field, pkglogger.ErrNegativeErrorTraceDepth) {
						t.Fatalf("constructor error does not identify bad stdout setting: %v", err)
					}
				})
			}
		})
	}
}

// 本文の表示の設定は、通常の出力先か、代替の経路で使う出力先だけで検証します。
func TestConstructorOnlyValidatesActiveBackendBodyConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.File = &pkglogger.FileLoggerConfig{LogBodyConfig: pkglogger.LogBodyConfig{ErrorTraceDepth: -1}}
	created, err := NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unused backend rejected: %v", err)
	}
	closeConstructedForTest(t, created)

	cfg.Stdout.HandlerConfig = handler.HandlerConfig{OnErrorConfig: []string{"file"}}
	err = closeIfConstructed(NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, nil, nil))
	if !hasConfigFieldError(err, pkglogger.LogOutputFile, pkglogger.ConfigFieldErrorTraceDepth, pkglogger.ErrNegativeErrorTraceDepth) {
		t.Fatalf("active fallback error: %v", err)
	}
}

// closeIfConstructedは、誤って作ったloggerを閉じ、constructorのerrorを返します。
func closeIfConstructed[L io.Closer](logger L, err error) error {
	if err == nil {
		_ = logger.Close()
	}
	return err
}

// requireConfigFieldErrorは、errがsourceの設定fieldを、指定した種類の不正として
// 拒否していることを確かめます。
func requireConfigFieldError(t *testing.T, err error, source string, field pkglogger.ConfigField, kind error) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("error = %v, want %v", err, kind)
	}
	var fieldErr *pkglogger.ConfigFieldError
	if !errors.As(err, &fieldErr) || fieldErr.Source != source || fieldErr.Field != field {
		t.Fatalf("error = %v (%+v), want a ConfigFieldError for %s %s", err, fieldErr, source, field)
	}
}

// hasConfigFieldErrorは、errに、backendのfieldをkindの不正として示すConfigFieldErrorが
// 含まれるかを返します。errors.Joinでまとめた複数の不正からも探します。
func hasConfigFieldError(err error, backend pkglogger.LogOutputDestination, field pkglogger.ConfigField, kind error) bool {
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Backend == backend && fieldErr.Field == field && errors.Is(fieldErr, kind) {
			return true
		}
	}
	return false
}
