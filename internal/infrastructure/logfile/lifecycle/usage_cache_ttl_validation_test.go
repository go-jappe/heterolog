package lifecycle

import (
	"errors"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestNewLogFileLifecyclesRejectsZeroUsageCacheTTL(t *testing.T) {
	_, err := NewLogFileLifecycles(LogFileOptions{
		Config: pkgfsstore.LogFileLifecycleRunnerConfig{
			FsstoreConfig: pkgfsstore.Config{
				Layout:    pkgfsstore.FsstoreConfig{LogDir: "logs"},
				Lifecycle: pkgfsstore.FileLifecycleConfig{},
			},
		},
	})

	if !errors.Is(err, ErrInvalidUsageCacheConfiguration) {
		t.Fatalf("NewLogFileLifecycles() error = %v, want ErrInvalidUsageCacheConfiguration", err)
	}
}

func TestNewDeliveryFileLifecyclesRejectsZeroUsageCacheTTL(t *testing.T) {
	_, err := NewDeliveryFileLifecycles(DeliveryFileOptions{
		Config: pkgfsstore.DeliveryFileLifecycleRunnerConfig{
			FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
				Layout:    pkgfsstore.FsstoreConfig{LogDir: "logs"},
				Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{},
			},
		},
	})

	if !errors.Is(err, ErrInvalidUsageCacheConfiguration) {
		t.Fatalf("NewDeliveryFileLifecycles() error = %v, want ErrInvalidUsageCacheConfiguration", err)
	}
}
