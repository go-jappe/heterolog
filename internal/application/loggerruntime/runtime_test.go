package loggerruntime_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestRuntimeRecordsAndClosesWithoutEnvironment(t *testing.T) {
	t.Setenv("HETEROLOG_ENV_PATH", filepath.Join(t.TempDir(), "does-not-exist.env"))
	t.Setenv("HETEROLOG_ENABLE_LOGGING", "false")
	t.Setenv("HETEROLOG_STDOUT_LOG_LEVEL", "ERROR")
	cfg := loggerruntime.DefaultConfig()
	cfg.Resource = nil // 任意のresourceのmetadataがなくてもpanicしてはいけません。
	// 設定は値で渡すため、runtimeと共有するのはpointerの下位の設定だけです。
	// DefaultConfigは、比較に使える独立した値を返します。
	want := loggerruntime.DefaultConfig()
	want.Resource = nil
	output := testutil.CaptureStdout(t, func() {
		logger, err := loggerruntime.New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = logger.Close() })
		field := pkglogger.MustFieldKey("count").Int(42)
		if err := logger.Log(context.Background(), pkglogger.LogLevelInfo, "public library entry", field); err != nil {
			t.Fatal(err)
		}
		if err := logger.Debug(context.Background(), "filtered debug"); err != nil {
			t.Fatal(err)
		}
		if err := logger.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
		if err := logger.Info(context.Background(), "after close"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("log after shutdown = %v", err)
		}
	})
	if !strings.Contains(output, "public library entry") || !strings.Contains(output, `"count":42`) {
		t.Fatalf("missing log entry or fields: %s", output)
	}
	if strings.Contains(output, "filtered debug") || strings.Contains(output, "after close") {
		t.Fatalf("unexpected output: %s", output)
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("runtime mutated caller config: got %+v, want %+v", *cfg.Stdout, *want.Stdout)
	}
}

func TestAsyncRuntimeDrainsAcceptedEntries(t *testing.T) {
	cfg, want := loggerruntime.DefaultAsyncConfig(), loggerruntime.DefaultAsyncConfig()
	output := testutil.CaptureStdout(t, func() {
		synctest.Test(t, func(t *testing.T) {
			logger, err := loggerruntime.NewAsync(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = logger.Close() })
			for range 20 {
				logger.Info(context.Background(), "accepted async entry")
			}
			if err := logger.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			logger.Info(context.Background(), "after async close")
			// 誤って受け付けたlogは非同期で送られます。出力を確かめる前に、それが始めた
			// goroutineをすべて終わらせます。
			synctest.Wait()
		})
	})
	if got := strings.Count(output, "accepted async entry"); got != 20 {
		t.Fatalf("drained %d entries, want 20", got)
	}
	if strings.Contains(output, "after async close") {
		t.Fatal("accepted entry after async shutdown")
	}
	// StdoutとResourceは、呼び出し元とruntimeが共有するpointerです。
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("async runtime mutated caller config: stdout=%+v resource=%+v", *cfg.Stdout, *cfg.Resource)
	}
}

func TestDeliveryRuntimeReportsDestination(t *testing.T) {
	cfg, want := loggerruntime.DefaultDeliveryConfig(), loggerruntime.DefaultDeliveryConfig()
	output := testutil.CaptureStdout(t, func() {
		logger, err := loggerruntime.NewDelivery(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = logger.Close() })
		report, err := logger.Info(context.Background(), "delivered entry")
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliverySucceeded {
			t.Fatalf("delivery report = %+v", report)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = logger.Info(context.Background(), "after delivery close")
		if !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("log after shutdown = %v", err)
		}
	})
	if !strings.Contains(output, "delivered entry") {
		t.Fatalf("missing delivered log: %s", output)
	}
	// StdoutとResourceは、呼び出し元とruntimeが共有するpointerです。
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("delivery runtime mutated caller config: stdout=%+v resource=%+v", *cfg.Stdout, *cfg.Resource)
	}
}

// constructorは、拒否した引数をそれぞれ種類（errors.Is）で示し、設定の場合は
// ConfigFieldErrorの出所と項目でも示します。
func TestConstructorRejectsInvalidConfiguration(t *testing.T) {
	ctx := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	constructors := map[string]func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error{
		"logger": func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error {
			cfg := loggerruntime.DefaultConfig()
			cfg.Timeout = timeout
			return closeConstructed(loggerruntime.New(ctx, cfg, opts...))
		},
		"async logger": func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error {
			cfg := loggerruntime.DefaultAsyncConfig()
			cfg.Timeout = timeout
			return closeConstructed(loggerruntime.NewAsync(ctx, cfg, opts...))
		},
		"delivery logger": func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error {
			cfg := loggerruntime.DefaultDeliveryConfig()
			cfg.Timeout = timeout
			return closeConstructed(loggerruntime.NewDelivery(ctx, cfg, opts...))
		},
	}
	for source, construct := range constructors {
		t.Run(source, func(t *testing.T) {
			if err := construct(nil, time.Second); !errors.Is(err, loggerruntime.ErrNilContext) {
				t.Errorf("nil context error = %v, want ErrNilContext", err)
			}
			if err := construct(canceled, time.Second); !errors.Is(err, context.Canceled) {
				t.Errorf("canceled context error = %v, want context.Canceled", err)
			}
			for _, timeout := range []time.Duration{0, -time.Second} {
				err := construct(ctx, timeout)
				requireConfigFieldError(t, err, source, loggerruntime.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)
			}
			err := construct(ctx, time.Second, loggerruntime.WithCloudWatchLogsStreamRouting("invalid"))
			var routingErr *pkgcloudwatchlogs.InvalidStreamRoutingError
			if !errors.Is(err, pkgcloudwatchlogs.ErrInvalidStreamRouting) || !errors.As(err, &routingErr) {
				t.Errorf("invalid stream routing error = %v, want ErrInvalidStreamRouting", err)
			}
		})
	}
	// zero値の設定にも、正のTimeoutはありません。
	_, err := loggerruntime.New(ctx, pkglogger.LoggerConfig{})
	requireConfigFieldError(t, err, "logger", loggerruntime.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)

	for _, maxParallel := range []int{0, -1} {
		cfg := loggerruntime.DefaultAsyncConfig()
		cfg.MaxParallel = maxParallel
		err := closeConstructed(loggerruntime.NewAsync(ctx, cfg))
		requireConfigFieldError(t, err, "async logger", loggerruntime.ConfigFieldMaxParallel, pkglogger.ErrNonPositiveSetting)
	}
}

// Delivery loggerは送信の結果を呼び出し元へ返し、再送のworkerを持たないため、
// NewDeliveryはWithSpoolを無視せずに拒否します。
func TestNewDeliveryRejectsWithSpool(t *testing.T) {
	err := closeConstructed(loggerruntime.NewDelivery(context.Background(), loggerruntime.DefaultDeliveryConfig(),
		loggerruntime.WithSpool(loggerruntime.DefaultSpoolConfig())))
	if !errors.Is(err, loggerruntime.ErrDeliverySpoolUnsupported) {
		t.Fatalf("NewDelivery(WithSpool) error = %v, want ErrDeliverySpoolUnsupported", err)
	}
}

// closeConstructedは、誤って作ったloggerを閉じ、constructorのerrorを返します。
func closeConstructed[R interface{ Close() error }](runtime R, err error) error {
	if err == nil {
		_ = runtime.Close()
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

func TestEntryLimitAndImmutableFields(t *testing.T) {
	cfg := loggerruntime.DefaultConfig()
	cfg.MaxLogEntryBytes = 64
	logger, err := loggerruntime.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	err = logger.Info(context.Background(), strings.Repeat("x", 100))
	var tooLarge *pkglogger.LogEntryTooLargeError
	if !errors.Is(err, pkglogger.ErrLogEntryTooLarge) || !errors.As(err, &tooLarge) {
		t.Fatalf("oversized entry = %v", err)
	}
	original := map[string]any{"value": "before"}
	field, err := pkglogger.Any("snapshot", original)
	if err != nil {
		t.Fatal(err)
	}
	original["value"] = "after"
	if field.Value().(map[string]any)["value"] != "before" {
		t.Fatal("field did not snapshot input")
	}
	if _, err := pkglogger.String("invalid key", "value"); !errors.Is(err, pkglogger.ErrFieldKeyInvalid) {
		t.Fatalf("invalid field key = %v", err)
	}
}
