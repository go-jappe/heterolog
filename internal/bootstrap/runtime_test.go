package bootstrap_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestRuntimeRecordsAndClosesWithoutEnvironment(t *testing.T) {
	t.Setenv("HETEROLOG_ENV_PATH", filepath.Join(t.TempDir(), "does-not-exist.env"))
	t.Setenv("HETEROLOG_ENABLE_LOGGING", "false")
	t.Setenv("HETEROLOG_STDOUT_LOG_LEVEL", "ERROR")
	cfg := bootstrap.DefaultConfig()
	cfg.Resource = nil // 任意のresourceのmetadataがなくてもpanicしてはいけません。
	// 設定は値で渡すため、runtimeと共有するのはpointerの下位の設定だけです。
	// DefaultConfigは、比較に使える独立した値を返します。
	want := bootstrap.DefaultConfig()
	want.Resource = nil
	output := testutil.CaptureStdout(t, func() {
		logger, err := bootstrap.New(context.Background(), cfg)
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
	cfg, want := bootstrap.DefaultAsyncConfig(), bootstrap.DefaultAsyncConfig()
	output := testutil.CaptureStdout(t, func() {
		synctest.Test(t, func(t *testing.T) {
			logger, err := bootstrap.NewAsync(context.Background(), cfg)
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
	cfg, want := bootstrap.DefaultDeliveryConfig(), bootstrap.DefaultDeliveryConfig()
	output := testutil.CaptureStdout(t, func() {
		logger, err := bootstrap.NewDelivery(context.Background(), cfg)
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

func TestEntryLimitAndImmutableFields(t *testing.T) {
	cfg := bootstrap.DefaultConfig()
	cfg.MaxLogEntryBytes = 64
	logger, err := bootstrap.New(context.Background(), cfg)
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

// closeConstructedは、誤って作ったloggerを閉じ、constructorのerrorを返します。
func closeConstructed[R interface{ Close() error }](runtime R, err error) error {
	if err == nil {
		_ = runtime.Close()
	}
	return err
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
