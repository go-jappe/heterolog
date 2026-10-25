package compressedlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRootRuntimeBuilderSharesCopiedSettingsByResolvedRoot(t *testing.T) {
	builder := NewRootRuntimeBuilder()
	t.Cleanup(func() { _ = builder.Close() })
	config := managedUploaderTestConfig(t.TempDir())
	first, err := builder.Build(builderTestConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	copied := config
	copied.SpoolPath += string(filepath.Separator) + "."
	second, err := builder.Build(builderTestConfig(copied))
	if err != nil {
		t.Fatal(err)
	}
	if first[objectstorage.UploadTargetS3] != second[objectstorage.UploadTargetS3] {
		t.Fatal("copied settings for the same resolved root did not share a runtime")
	}
	owned := builder.OwnedRuntimes()
	if len(owned) != 1 {
		t.Fatalf("owned runtime count = %d, want 1", len(owned))
	}
	owned[0] = nil
	if builder.OwnedRuntimes()[0] == nil {
		t.Fatal("OwnedRuntimes exposed the builder's ownership list")
	}

	otherConfig := config
	otherConfig.SpoolPath = t.TempDir()
	other, err := builder.Build(builderTestConfig(otherConfig))
	if err != nil {
		t.Fatal(err)
	}
	if first[objectstorage.UploadTargetS3] == other[objectstorage.UploadTargetS3] || len(builder.OwnedRuntimes()) != 2 {
		t.Fatal("different roots were not retained as separate runtime owners")
	}
}

func TestRootRuntimeBuilderRejectsEveryConflictingRuntimeSetting(t *testing.T) {
	config := managedUploaderTestConfig(t.TempDir())
	config.Done = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{MaxFileAgeDays: 1, MaxDirSize: 100}
	config.Dead = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{MaxFileAgeDays: 2, MaxDirSize: 200}
	config.Quarantine = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{MaxFileAgeDays: 3, MaxDirSize: 300}
	builder := NewRootRuntimeBuilder()
	t.Cleanup(func() { _ = builder.Close() })
	if _, err := builder.Build(builderTestConfig(config)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*objectstorage.CompressedLogFileUploadSpoolConfig)
	}{
		{"runner interval", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.RunnerInterval += time.Second }},
		{"purge interval", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.LifecycleInterval += time.Second }},
		{"circuit breaker cooldown", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.CircuitBreakerOpenCooldown += time.Second }},
		{"done enabled", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Done.Enabled = true }},
		{"done age", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Done.MaxFileAgeDays++ }},
		{"done size", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Done.MaxDirSize++ }},
		{"dead enabled", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Dead.Enabled = true }},
		{"dead age", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Dead.MaxFileAgeDays++ }},
		{"dead size", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Dead.MaxDirSize++ }},
		{"quarantine enabled", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Quarantine.Enabled = true }},
		{"quarantine age", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Quarantine.MaxFileAgeDays++ }},
		{"quarantine size", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Quarantine.MaxDirSize++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := config
			test.change(&changed)
			if _, err := builder.Build(builderTestConfig(changed)); !errors.Is(err, ErrSpoolRootConfigMismatch) {
				t.Fatalf("Build() error = %v, want ErrSpoolRootConfigMismatch", err)
			}
			if len(builder.OwnedRuntimes()) != 1 {
				t.Fatal("conflicting settings created another runtime")
			}
		})
	}
}

func TestRootRuntimeBuilderSharesFilesystemAlias(t *testing.T) {
	directory := t.TempDir()
	realPath := filepath.Join(directory, "real")
	if err := os.Mkdir(realPath, 0700); err != nil {
		t.Fatal(err)
	}
	aliasPath := filepath.Join(directory, "alias")
	if err := os.Symlink(realPath, aliasPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	builder := NewRootRuntimeBuilder()
	t.Cleanup(func() { _ = builder.Close() })
	config := managedUploaderTestConfig(realPath)
	first, err := builder.Build(builderTestConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	config.SpoolPath = aliasPath
	second, err := builder.Build(builderTestConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	if first[objectstorage.UploadTargetS3] != second[objectstorage.UploadTargetS3] {
		t.Fatal("filesystem aliases of the same root acquired separate runtimes")
	}
}

func TestRootRuntimeBuilderDoesNotShareBetweenOwners(t *testing.T) {
	config := builderTestConfig(managedUploaderTestConfig(t.TempDir()))
	first := NewRootRuntimeBuilder()
	t.Cleanup(func() { _ = first.Close() })
	if _, err := first.Build(config); err != nil {
		t.Fatal(err)
	}
	second := NewRootRuntimeBuilder()
	t.Cleanup(func() { _ = second.Close() })
	if _, err := second.Build(config); !errors.Is(err, ErrSpoolRootAlreadyOwned) {
		t.Fatalf("second owner's Build() error = %v, want ErrSpoolRootAlreadyOwned", err)
	}
	if len(second.OwnedRuntimes()) != 0 {
		t.Fatal("rejected owner retained a runtime")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Build(config); err != nil {
		t.Fatalf("Build() after earlier owner closed: %v", err)
	}
}

func TestRootRuntimesRejectsAmbiguousServiceAndRollsBack(t *testing.T) {
	first := builderTestConfig(managedUploaderTestConfig(t.TempDir()))
	second := builderTestConfig(managedUploaderTestConfig(t.TempDir()))
	if _, err := NewRootRuntimes(first, second); !errors.Is(err, ErrSpoolServiceRootMismatch) {
		t.Fatalf("NewRootRuntimes() error = %v, want ErrSpoolServiceRootMismatch", err)
	}
	for _, config := range []ConfigProvider{first, second} {
		runtime, err := NewRootRuntime(config)
		if err != nil {
			t.Fatalf("failed construction retained ownership: %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func builderTestConfig(config objectstorage.CompressedLogFileUploadSpoolConfig) ConfigProvider {
	return NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(config), objectstorage.UploadTargetS3)
}
