package heterolog_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPublicRuntimeRecordsAndClosesWithoutEnvironment(t *testing.T) {
	t.Setenv("HETEROLOG_ENV_PATH", filepath.Join(t.TempDir(), "does-not-exist.env"))
	t.Setenv("HETEROLOG_ENABLE_LOGGING", "false")
	t.Setenv("HETEROLOG_STDOUT_LOG_LEVEL", "ERROR")
	cfg := heterolog.DefaultConfig()
	cfg.Resource = nil // Optional resource metadata must not cause a panic.
	// Config is passed by value, so only the pointer sub-configs are shared
	// with the runtime. DefaultConfig returns independent values for comparison.
	want := heterolog.DefaultConfig()
	want.Resource = nil
	output := captureRuntimeStdout(t, func() {
		logger, err := heterolog.New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = logger.Close() })
		field := heterolog.MustFieldKey("count").Int(42)
		if err := logger.Log(context.Background(), heterolog.LevelInfo, "public library entry", field); err != nil {
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
		if err := logger.Info(context.Background(), "after close"); !errors.Is(err, heterolog.ErrLoggerClosed) {
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

func TestPublicAsyncRuntimeDrainsAcceptedEntries(t *testing.T) {
	cfg, want := heterolog.DefaultAsyncConfig(), heterolog.DefaultAsyncConfig()
	output := captureRuntimeStdout(t, func() {
		synctest.Test(t, func(t *testing.T) {
			logger, err := heterolog.NewAsync(context.Background(), cfg)
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
			// An entry accepted by mistake is delivered asynchronously. Let every
			// goroutine it started finish before the output is inspected.
			synctest.Wait()
		})
	})
	if got := strings.Count(output, "accepted async entry"); got != 20 {
		t.Fatalf("drained %d entries, want 20", got)
	}
	if strings.Contains(output, "after async close") {
		t.Fatal("accepted entry after async shutdown")
	}
	// Stdout and Resource are pointers shared between the caller and the runtime.
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("async runtime mutated caller config: stdout=%+v resource=%+v", *cfg.Stdout, *cfg.Resource)
	}
}

func TestPublicDeliveryRuntimeReportsDestination(t *testing.T) {
	cfg, want := heterolog.DefaultDeliveryConfig(), heterolog.DefaultDeliveryConfig()
	output := captureRuntimeStdout(t, func() {
		logger, err := heterolog.NewDelivery(context.Background(), cfg)
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
		if !errors.Is(err, heterolog.ErrLoggerClosed) {
			t.Fatalf("log after shutdown = %v", err)
		}
	})
	if !strings.Contains(output, "delivered entry") {
		t.Fatalf("missing delivered log: %s", output)
	}
	// Stdout and Resource are pointers shared between the caller and the runtime.
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("delivery runtime mutated caller config: stdout=%+v resource=%+v", *cfg.Stdout, *cfg.Resource)
	}
}

// The constructors identify each rejected argument by kind (errors.Is) and, for
// settings, by the ConfigFieldError's source and field.
func TestPublicConstructorRejectsInvalidConfiguration(t *testing.T) {
	ctx := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	constructors := map[string]func(ctx context.Context, timeout time.Duration, opts ...heterolog.Option) error{
		"logger": func(ctx context.Context, timeout time.Duration, opts ...heterolog.Option) error {
			cfg := heterolog.DefaultConfig()
			cfg.Timeout = timeout
			return closeConstructed(heterolog.New(ctx, cfg, opts...))
		},
		"async logger": func(ctx context.Context, timeout time.Duration, opts ...heterolog.Option) error {
			cfg := heterolog.DefaultAsyncConfig()
			cfg.Timeout = timeout
			return closeConstructed(heterolog.NewAsync(ctx, cfg, opts...))
		},
		"delivery logger": func(ctx context.Context, timeout time.Duration, opts ...heterolog.Option) error {
			cfg := heterolog.DefaultDeliveryConfig()
			cfg.Timeout = timeout
			return closeConstructed(heterolog.NewDelivery(ctx, cfg, opts...))
		},
	}
	for source, construct := range constructors {
		t.Run(source, func(t *testing.T) {
			if err := construct(nil, time.Second); !errors.Is(err, heterolog.ErrNilContext) {
				t.Errorf("nil context error = %v, want ErrNilContext", err)
			}
			if err := construct(canceled, time.Second); !errors.Is(err, context.Canceled) {
				t.Errorf("canceled context error = %v, want context.Canceled", err)
			}
			for _, timeout := range []time.Duration{0, -time.Second} {
				err := construct(ctx, timeout)
				requirePublicConfigFieldError(t, err, source, heterolog.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)
			}
			err := construct(ctx, time.Second, heterolog.WithCloudWatchLogsStreamRouting("invalid"))
			var routingErr *heterolog.InvalidStreamRoutingError
			if !errors.Is(err, heterolog.ErrInvalidStreamRouting) || !errors.As(err, &routingErr) {
				t.Errorf("invalid stream routing error = %v, want ErrInvalidStreamRouting", err)
			}
		})
	}
	// The zero Config has no positive Timeout either.
	_, err := heterolog.New(ctx, heterolog.Config{})
	requirePublicConfigFieldError(t, err, "logger", heterolog.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)

	for _, maxParallel := range []int{0, -1} {
		cfg := heterolog.DefaultAsyncConfig()
		cfg.MaxParallel = maxParallel
		err := closeConstructed(heterolog.NewAsync(ctx, cfg))
		requirePublicConfigFieldError(t, err, "async logger", heterolog.ConfigFieldMaxParallel, pkglogger.ErrNonPositiveSetting)
	}
}

// A Delivery logger reports each delivery to the caller and owns no replay
// workers, so NewDelivery rejects WithSpool instead of ignoring it.
func TestPublicNewDeliveryRejectsWithSpool(t *testing.T) {
	err := closeConstructed(heterolog.NewDelivery(context.Background(), heterolog.DefaultDeliveryConfig(),
		heterolog.WithSpool(heterolog.DefaultSpoolConfig())))
	if !errors.Is(err, heterolog.ErrDeliverySpoolUnsupported) {
		t.Fatalf("NewDelivery(WithSpool) error = %v, want ErrDeliverySpoolUnsupported", err)
	}
}

// closeConstructed closes a logger that was constructed by mistake and returns
// the constructor error.
func closeConstructed[R interface{ Close() error }](runtime R, err error) error {
	if err == nil {
		_ = runtime.Close()
	}
	return err
}

// requirePublicConfigFieldError checks that err rejects the setting field of
// source with the given kind.
func requirePublicConfigFieldError(t *testing.T, err error, source string, field pkglogger.ConfigField, kind error) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("error = %v, want %v", err, kind)
	}
	var fieldErr *pkglogger.ConfigFieldError
	if !errors.As(err, &fieldErr) || fieldErr.Source != source || fieldErr.Field != field {
		t.Fatalf("error = %v (%+v), want a ConfigFieldError for %s %s", err, fieldErr, source, field)
	}
}

func TestPublicEntryLimitAndImmutableFields(t *testing.T) {
	cfg := heterolog.DefaultConfig()
	cfg.MaxLogEntryBytes = 64
	logger, err := heterolog.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	err = logger.Info(context.Background(), strings.Repeat("x", 100))
	var tooLarge *heterolog.LogEntryTooLargeError
	if !errors.Is(err, heterolog.ErrLogEntryTooLarge) || !errors.As(err, &tooLarge) {
		t.Fatalf("oversized entry = %v", err)
	}
	original := map[string]any{"value": "before"}
	field, err := heterolog.Any("snapshot", original)
	if err != nil {
		t.Fatal(err)
	}
	original["value"] = "after"
	if field.Value().(map[string]any)["value"] != "before" {
		t.Fatal("field did not snapshot input")
	}
	if _, err := heterolog.String("invalid key", "value"); !errors.Is(err, heterolog.ErrFieldKeyInvalid) {
		t.Fatalf("invalid field key = %v", err)
	}
}

func captureRuntimeStdout(t *testing.T, fn func()) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	original := os.Stdout
	os.Stdout = file
	defer func() { os.Stdout = original }()
	fn()
	os.Stdout = original
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}
