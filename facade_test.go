package heterolog_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// ルートの公開の型は、内部の実装へ処理を渡すだけです。ここでは、各methodが対応する処理へ
// 渡ることを確かめます。処理そのものの振る舞いは、internal/application/loggerruntime・
// internal/presentation/envconfig・internal/presentation/sloghandlerで試験します。

func TestRuntimeForwardsToTheImplementation(t *testing.T) {
	ctx := context.Background()
	output := testutil.CaptureStdout(t, func() {
		runtime, err := heterolog.New(ctx, heterolog.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runtime.Close() })
		if err := runtime.Log(ctx, heterolog.LevelInfo, "facade entry"); err != nil {
			t.Fatalf("Log: %v", err)
		}
		warn := heterolog.LevelWarn
		if err := runtime.UpdateOutputSettings(map[heterolog.LogOutputDestination]heterolog.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Level: &warn},
		}); err != nil {
			t.Fatalf("UpdateOutputSettings: %v", err)
		}
		if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelWarn {
			t.Errorf("stdout level = %v after UpdateOutputSettings, want WARN", got)
		}
		if err := runtime.Info(ctx, "filtered by the new level"); err != nil {
			t.Fatalf("Info: %v", err)
		}
		if err := runtime.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatalf("Close after Shutdown: %v", err)
		}
		if err := runtime.Info(ctx, "after close"); !errors.Is(err, heterolog.ErrLoggerClosed) {
			t.Errorf("Info after Close = %v, want ErrLoggerClosed", err)
		}
	})
	if !strings.Contains(output, "facade entry") || strings.Contains(output, "filtered by the new level") {
		t.Errorf("stdout = %q, want only the entry logged before the level change", output)
	}

	forced, err := heterolog.New(ctx, heterolog.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := forced.ForceClose(); err != nil && !errors.Is(err, heterolog.ErrForcedClose) {
		t.Fatalf("ForceClose: %v", err)
	}
	if err := forced.Info(ctx, "after force close"); !errors.Is(err, heterolog.ErrLoggerClosed) {
		t.Errorf("Info after ForceClose = %v, want ErrLoggerClosed", err)
	}
}

func TestAsyncAndDeliveryRuntimesForwardToTheImplementation(t *testing.T) {
	ctx := context.Background()
	testutil.CaptureStdout(t, func() {
		async, err := heterolog.NewAsync(ctx, heterolog.DefaultAsyncConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = async.Close() })
		debug := heterolog.LevelDebug
		if err := async.UpdateOutputSettings(map[heterolog.LogOutputDestination]heterolog.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Level: &debug},
		}); err != nil {
			t.Fatalf("async UpdateOutputSettings: %v", err)
		}
		if got := async.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelDebug {
			t.Errorf("async stdout level = %v, want DEBUG", got)
		}
		if err := async.Shutdown(ctx); err != nil {
			t.Fatalf("async Shutdown: %v", err)
		}
		if err := async.Close(); err != nil {
			t.Fatalf("async Close: %v", err)
		}
		if err := async.ForceClose(); err != nil && !errors.Is(err, heterolog.ErrForcedClose) {
			t.Fatalf("async ForceClose after Close: %v", err)
		}

		delivery, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = delivery.Close() })
		report, err := delivery.Info(ctx, "delivered through the facade")
		if err != nil || len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliverySucceeded {
			t.Fatalf("delivery Info = %+v, %v; want one succeeded target", report, err)
		}
		if err := delivery.Shutdown(ctx); err != nil {
			t.Fatalf("delivery Shutdown: %v", err)
		}
		if _, err := delivery.Info(ctx, "after shutdown"); !errors.Is(err, heterolog.ErrLoggerClosed) {
			t.Errorf("delivery Info after Shutdown = %v, want ErrLoggerClosed", err)
		}
	})
}

// zero値の公開の型は、内部の型のzero値と同じく、終了と設定の読み取りを安全に行えます。
func TestZeroValuesOfThePublicTypes(t *testing.T) {
	ctx := context.Background()
	var runtime heterolog.Runtime
	var async heterolog.AsyncRuntime
	var delivery heterolog.DeliveryRuntime
	for name, closer := range map[string]interface {
		Close() error
		Shutdown(context.Context) error
		ForceClose() error
	}{"sync": &runtime, "async": &async, "delivery": &delivery} {
		if err := closer.Close(); err != nil {
			t.Errorf("%s zero value Close = %v", name, err)
		}
		if err := closer.Shutdown(ctx); err != nil {
			t.Errorf("%s zero value Shutdown = %v", name, err)
		}
		if err := closer.ForceClose(); err != nil {
			t.Errorf("%s zero value ForceClose = %v", name, err)
		}
	}
	if got := runtime.OutputSettings(); len(got) != 0 {
		t.Errorf("zero value OutputSettings = %v, want empty", got)
	}
	debug := heterolog.LevelDebug
	if err := async.UpdateOutputSettings(map[heterolog.LogOutputDestination]heterolog.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: &debug},
	}); !errors.Is(err, heterolog.ErrUnknownOutput) {
		t.Errorf("zero value UpdateOutputSettings = %v, want ErrUnknownOutput", err)
	}

	var handler heterolog.SlogHandler
	if !handler.Enabled(ctx, slog.LevelInfo) || handler.Enabled(ctx, slog.LevelDebug) {
		t.Error("zero value SlogHandler does not use the default Info level")
	}
	if _, ok := handler.WithGroup("request").(*heterolog.SlogHandler); !ok {
		t.Error("zero value SlogHandler.WithGroup did not return a *SlogHandler")
	}

	var env heterolog.EnvConfig
	if _, _, err := env.Config(); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("zero value EnvConfig.Config = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, ok := env.AdminServerConfig(); ok {
		t.Error("zero value EnvConfig enables the admin server")
	}
}

func TestEnvConfigForwardsToTheImplementation(t *testing.T) {
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "heterolog.env")
	lines := []string{
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_STDOUT_LOG_LEVEL=ERROR",
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:0",
		"HETEROLOG_ADMIN_TOKEN=" + strings.Repeat("t", heterolog.MinAdminTokenLength),
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env, err := heterolog.LoadEnvConfig(path)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}

	cfg, opts, err := env.Config()
	if err != nil || cfg.Stdout == nil || cfg.Stdout.RuntimeConfig.LogLevel != heterolog.LevelError || len(opts) == 0 {
		t.Fatalf("Config = %+v, %d options, %v; want stdout at ERROR with options", cfg.Stdout, len(opts), err)
	}
	if _, _, err := env.AsyncConfig(); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("AsyncConfig = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, err := env.NewAsync(context.Background()); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("NewAsync = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, _, err := env.DeliveryConfig(); err != nil {
		t.Errorf("DeliveryConfig: %v", err)
	}
	if server, ok := env.AdminServerConfig(); !ok || server.Addr != "127.0.0.1:0" {
		t.Errorf("AdminServerConfig = %+v, %v; want the configured address", server, ok)
	}

	runtime, err := env.New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelError {
		t.Errorf("stdout level of the runtime from the environment = %v, want ERROR", got)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	delivery, err := env.NewDelivery(context.Background())
	if err != nil {
		t.Fatalf("NewDelivery: %v", err)
	}
	if err := delivery.Close(); err != nil {
		t.Fatal(err)
	}
}

// facadeCaptureLoggerは、受け取ったlogのmessageとfieldのkeyを記録するLoggerです。
type facadeCaptureLogger struct {
	messages []string
	keys     [][]string
}

func (l *facadeCaptureLogger) record(message string, fields []heterolog.Field) error {
	keys := make([]string, 0, len(fields))
	for _, field := range fields {
		keys = append(keys, field.Key())
	}
	l.messages = append(l.messages, message)
	l.keys = append(l.keys, keys)
	return nil
}

func (l *facadeCaptureLogger) Debug(_ context.Context, message string, fields ...heterolog.Field) error {
	return l.record(message, fields)
}

func (l *facadeCaptureLogger) Info(_ context.Context, message string, fields ...heterolog.Field) error {
	return l.record(message, fields)
}

func (l *facadeCaptureLogger) Warn(_ context.Context, message string, fields ...heterolog.Field) error {
	return l.record(message, fields)
}

func (l *facadeCaptureLogger) Error(_ context.Context, message string, fields ...heterolog.Field) error {
	return l.record(message, fields)
}

func (l *facadeCaptureLogger) ErrorWithStack(_ context.Context, message string, _ error, fields ...heterolog.Field) error {
	return l.record(message, fields)
}

func TestSlogHandlerForwardsToTheImplementation(t *testing.T) {
	capture := &facadeCaptureLogger{}
	handler := heterolog.NewSlogHandler(capture, &slog.HandlerOptions{Level: slog.LevelDebug})
	if !handler.Enabled(context.Background(), slog.LevelDebug) || handler.Enabled(context.Background(), slog.LevelDebug-1) {
		t.Error("Enabled does not follow the configured level")
	}
	withAttrs, ok := handler.WithAttrs([]slog.Attr{slog.String("service", "api")}).(*heterolog.SlogHandler)
	if !ok {
		t.Fatal("WithAttrs did not return a *SlogHandler")
	}
	withGroup, ok := withAttrs.WithGroup("request").(*heterolog.SlogHandler)
	if !ok {
		t.Fatal("WithGroup did not return a *SlogHandler")
	}
	slog.New(withGroup).Info("handled", "id", 7)
	if len(capture.messages) != 1 || capture.messages[0] != "handled" {
		t.Fatalf("messages = %q, want the one handled record", capture.messages)
	}
	if keys := strings.Join(capture.keys[0], ","); !strings.Contains(keys, "service") || !strings.Contains(keys, "request") {
		t.Errorf("field keys = %s, want the attribute and the group", keys)
	}
}
