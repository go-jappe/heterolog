package file

import (
	"errors"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestValidateFileLoggerConfigRequiresUsageCacheTTLForActiveLifecycle(t *testing.T) {
	tests := []struct {
		name    string
		mode    pkgfsstore.FileLifecycleMode
		wantErr bool
	}{
		{name: "default inline", wantErr: true},
		{name: "explicit inline", mode: pkgfsstore.FileLifecycleModeInline, wantErr: true},
		{name: "runner", mode: pkgfsstore.FileLifecycleModeRunner, wantErr: true},
		{name: "disable", mode: pkgfsstore.FileLifecycleModeDisable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "usage-cache-validation")
			cfg.LifecycleMode = test.mode
			// 対照の設定はUsageCacheTTLが正であることだけが異なり、どのmodeでも受理されます。
			if err := validateFileLoggerConfigProvider(NewConfigAdapter(cfg)); err != nil {
				t.Fatalf("validateFileLoggerConfigProvider() with positive usage cache TTL error = %v, want nil", err)
			}
			cfg.FsstoreConfig.Lifecycle.UsageCacheTTL = -time.Second

			err := validateFileLoggerConfigProvider(NewConfigAdapter(cfg))
			if test.wantErr {
				if !errors.Is(err, pkgfsstore.ErrInvalidUsageCacheTTL) {
					t.Fatalf("validateFileLoggerConfigProvider() error = %v, want %v", err, pkgfsstore.ErrInvalidUsageCacheTTL)
				}
			} else if err != nil {
				t.Fatalf("validateFileLoggerConfigProvider() error = %v, want nil", err)
			}
		})
	}
}

func TestFileLoggerConfigRejectsZeroUsageCacheTTLForInlineLifecycle(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "usage-cache-zero")
	cfg.FsstoreConfig.Lifecycle.UsageCacheTTL = 0
	adapter := NewConfigAdapter(cfg)

	if got := adapter.Fsstore().Purge().UsageCacheTTL(); got != 0 {
		t.Fatalf("UsageCacheTTL() = %s, want zero passthrough", got)
	}
	if err := validateFileLoggerConfigProvider(adapter); !errors.Is(err, pkgfsstore.ErrInvalidUsageCacheTTL) {
		t.Fatalf("validateFileLoggerConfigProvider() error = %v, want %v", err, pkgfsstore.ErrInvalidUsageCacheTTL)
	}
	// 0との違いはUsageCacheTTLだけの対照で、正の最小値は受理されることを確認します。
	cfg.FsstoreConfig.Lifecycle.UsageCacheTTL = time.Nanosecond
	if err := validateFileLoggerConfigProvider(NewConfigAdapter(cfg)); err != nil {
		t.Fatalf("validateFileLoggerConfigProvider() with 1ns usage cache TTL error = %v, want nil", err)
	}
}
