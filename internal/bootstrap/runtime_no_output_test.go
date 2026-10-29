package bootstrap_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 出力先が一つもない設定ではlogが黙って失われ、Delivery loggerも送信先のない結果しか
// 返せません。sync/async/Deliveryの各constructorは、runtimeを構築する前に
// ErrNoActiveLogOutputを返します。LogOutputDestinationsが空ならnon-nilのbackend設定が
// 通常出力になるため、backend設定を一つも持たない設定が対象です。
func TestConstructorsRejectConfigWithoutActiveLogOutput(t *testing.T) {
	ctx := context.Background()

	syncConfigs := map[string]pkglogger.LoggerConfig{
		"required values only": {Timeout: time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes},
		"default without stdout": func() pkglogger.LoggerConfig {
			cfg := loggerruntime.DefaultConfig()
			cfg.LogOutputDestinations, cfg.Stdout = nil, nil
			return cfg
		}(),
	}
	for name, cfg := range syncConfigs {
		t.Run("sync/"+name, func(t *testing.T) {
			runtime, err := bootstrap.New(ctx, cfg)
			if runtime != nil {
				_ = runtime.Close()
			}
			if !errors.Is(err, pkglogger.ErrNoActiveLogOutput) || runtime != nil {
				t.Fatalf("New() runtime=%v error=%v, want ErrNoActiveLogOutput without a runtime", runtime, err)
			}
		})
	}

	t.Run("async/default without stdout", func(t *testing.T) {
		cfg := loggerruntime.DefaultAsyncConfig()
		cfg.LogOutputDestinations, cfg.Stdout = nil, nil
		runtime, err := bootstrap.NewAsync(ctx, cfg)
		if runtime != nil {
			_ = runtime.Close()
		}
		if !errors.Is(err, pkglogger.ErrNoActiveLogOutput) || runtime != nil {
			t.Fatalf("NewAsync() runtime=%v error=%v, want ErrNoActiveLogOutput without a runtime", runtime, err)
		}
	})

	t.Run("delivery/default without stdout", func(t *testing.T) {
		cfg := loggerruntime.DefaultDeliveryConfig()
		cfg.LogOutputDestinations, cfg.Stdout = nil, nil
		runtime, err := bootstrap.NewDelivery(ctx, cfg)
		if runtime != nil {
			_ = runtime.Close()
		}
		if !errors.Is(err, pkglogger.ErrNoActiveLogOutput) || runtime != nil {
			t.Fatalf("NewDelivery() runtime=%v error=%v, want ErrNoActiveLogOutput without a runtime", runtime, err)
		}
	})
}

// DefaultConfig系はstdoutを通常出力として選択するため、そのまま構築できます。
// LogOutputDestinationsを空にした直接構築でも、non-nilのStdout設定は通常出力です。
func TestDefaultConfigsConstructWithActiveStdoutOutput(t *testing.T) {
	ctx := context.Background()

	implicit := loggerruntime.DefaultConfig()
	implicit.LogOutputDestinations = nil
	for name, cfg := range map[string]pkglogger.LoggerConfig{
		"explicit stdout": loggerruntime.DefaultConfig(),
		"implicit stdout": implicit,
	} {
		t.Run("sync/"+name, func(t *testing.T) {
			runtime, err := bootstrap.New(ctx, cfg)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := runtime.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}

	t.Run("async", func(t *testing.T) {
		runtime, err := bootstrap.NewAsync(ctx, loggerruntime.DefaultAsyncConfig())
		if err != nil {
			t.Fatalf("NewAsync() error = %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	t.Run("delivery", func(t *testing.T) {
		runtime, err := bootstrap.NewDelivery(ctx, loggerruntime.DefaultDeliveryConfig())
		if err != nil {
			t.Fatalf("NewDelivery() error = %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
}
