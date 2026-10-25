package compressedlog

import (
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRootRuntimesSeparatesServicesAndReleasesOwnership(t *testing.T) {
	cfg := objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: t.TempDir(), Target: "heterolog", LoggerContract: pkglogger.LoggerContractSync.String(),
		RunnerInterval: time.Hour, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: time.Minute,
	}
	services := []objectstorage.UploadTarget{objectstorage.UploadTargetS3, "another-backend", objectstorage.UploadTargetS3}
	runtimes, err := NewRootRuntimes(NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), services)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtimes.Close() })
	if len(runtimes) != 2 {
		t.Fatalf("runtime count = %d, want 2", len(runtimes))
	}
	for _, service := range services {
		runtime, exists := runtimes[service]
		if !exists || runtime == nil {
			t.Fatalf("constructed runtime for service %q is missing or nil", service)
		}
		if runtime.Root().Service() != service {
			t.Fatalf("wrong runtime for service %q", service)
		}
	}
	if runtimes[services[0]].Root().Path() == runtimes[services[1]].Root().Path() {
		t.Fatal("different backends share a root")
	}
	if err := runtimes.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewRootRuntimes(NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), services)...)
	if err != nil {
		t.Fatalf("ownership not released: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRootRuntimesConstructionFailureReleasesEarlierRoots(t *testing.T) {
	cfg := objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: t.TempDir(), Target: "heterolog", LoggerContract: pkglogger.LoggerContractSync.String(),
		RunnerInterval: time.Hour, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: time.Minute,
	}
	for _, test := range []struct {
		name   string
		config ConfigProvider
	}{
		{name: "invalid service", config: NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), "invalid/service")},
		{name: "nil provider"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3)
			if _, err := NewRootRuntimes(config, test.config); err == nil {
				t.Fatal("invalid config was accepted")
			}
			runtimes, err := NewRootRuntimes(config)
			if err != nil {
				t.Fatalf("failed construction retained S3 root: %v", err)
			}
			if err := runtimes.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
