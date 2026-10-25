package compressedlog

import (
	"errors"
	"maps"
	"testing"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type mutableCoordinatorConfigProvider struct {
	ConfigAdapter
	config objectstorage.CompressedLogFileUploadSpoolConfig
	calls  map[string]int
}

func (p *mutableCoordinatorConfigProvider) RunnerInterval() time.Duration {
	p.calls["runner"]++
	return p.config.RunnerInterval
}

func (p *mutableCoordinatorConfigProvider) LifecycleInterval() time.Duration {
	p.calls["lifecycle"]++
	return p.config.LifecycleInterval
}

func (p *mutableCoordinatorConfigProvider) CircuitBreakerOpenCooldown() time.Duration {
	p.calls["circuit breaker"]++
	return p.config.CircuitBreakerOpenCooldown
}

func (p *mutableCoordinatorConfigProvider) MaxRetryAfter() time.Duration {
	p.calls["max retry-after"]++
	return p.config.MaxRetryAfter
}

func (p *mutableCoordinatorConfigProvider) DonePurge() objectstorage.CompressedLogFileUploadSpoolPurgeConfigProvider {
	p.calls["done"]++
	return objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(p.config).DonePurge()
}

func (p *mutableCoordinatorConfigProvider) DeadPurge() objectstorage.CompressedLogFileUploadSpoolPurgeConfigProvider {
	p.calls["dead"]++
	return objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(p.config).DeadPurge()
}

func (p *mutableCoordinatorConfigProvider) QuarantinePurge() objectstorage.CompressedLogFileUploadSpoolPurgeConfigProvider {
	p.calls["quarantine"]++
	return objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(p.config).QuarantinePurge()
}

func TestCoordinatorConfigAdapterSnapshotsProviderValuesAtConstruction(t *testing.T) {
	want := objectstorage.CompressedLogFileUploadSpoolConfig{
		RunnerInterval:             -time.Second,
		LifecycleInterval:          -2 * time.Second,
		CircuitBreakerOpenCooldown: 37 * time.Second,
		MaxRetryAfter:              41 * time.Minute,
		Done: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 101,
		},
		Dead: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: false, MaxFileAgeDays: -2, MaxDirSize: 202,
		},
		Quarantine: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: true, MaxFileAgeDays: 3, MaxDirSize: 303,
		},
	}
	provider := &mutableCoordinatorConfigProvider{config: want, calls: make(map[string]int)}
	adapter := newRootRuntimeConfigAdapter(provider)
	callsAtConstruction := maps.Clone(provider.calls)
	provider.config = objectstorage.CompressedLogFileUploadSpoolConfig{}

	for i := 0; i < 2; i++ {
		got := objectstorage.CompressedLogFileUploadSpoolConfig{
			RunnerInterval:             adapter.Interval(),
			LifecycleInterval:          adapter.PurgeInterval(),
			CircuitBreakerOpenCooldown: adapter.CircuitBreakerOpenCooldown(),
			MaxRetryAfter:              adapter.MaxRetryAfter(),
			Done:                       statePurgeConfigValues(adapter.DonePurge()),
			Dead:                       statePurgeConfigValues(adapter.DeadPurge()),
			Quarantine:                 statePurgeConfigValues(adapter.QuarantinePurge()),
		}
		if got != want {
			t.Fatalf("adapter snapshot = %+v, want unchanged provider values %+v", got, want)
		}
		if got := adapter.CapacityPurgeMinimumFileAge(); got != want.LifecycleInterval {
			t.Fatalf("CapacityPurgeMinimumFileAge() = %v, want original lifecycle interval %v", got, want.LifecycleInterval)
		}
	}
	// 構築時に読んだ値だけを使い、以後はproviderを読み直しません。
	for _, field := range []string{"runner", "lifecycle", "circuit breaker", "max retry-after", "done", "dead", "quarantine"} {
		if callsAtConstruction[field] == 0 {
			t.Errorf("%s getter was not read at construction", field)
		}
	}
	if !maps.Equal(provider.calls, callsAtConstruction) {
		t.Errorf("provider getter calls after construction = %v, want unchanged %v", provider.calls, callsAtConstruction)
	}
}

func statePurgeConfigValues(config objectstoragespool.StatePurgeConfigProvider) objectstorage.CompressedLogFileUploadSpoolPurgeConfig {
	return objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled: config.Enabled(), MaxFileAgeDays: config.MaxFileAgeDays(), MaxDirSize: config.MaxDirSize(),
	}
}

func TestRootRuntimeRejectsIndependentOwnerRegardlessOfSettings(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	for _, test := range []struct {
		name   string
		change func(*objectstorage.CompressedLogFileUploadSpoolConfig)
	}{
		{"identical settings", func(*objectstorage.CompressedLogFileUploadSpoolConfig) {}},
		{"runner interval", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.RunnerInterval++ }},
		{"lifecycle interval", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.LifecycleInterval++ }},
		{"circuit breaker cooldown", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.CircuitBreakerOpenCooldown++ }},
		{"max retry-after", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.MaxRetryAfter++ }},
		{"disabled retention settings", func(c *objectstorage.CompressedLogFileUploadSpoolConfig) { c.Quarantine.MaxDirSize++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := cfg
			test.change(&other)
			duplicate, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(other), objectstorage.UploadTargetS3))
			if duplicate != nil {
				_ = duplicate.Close()
				t.Fatal("independent root owner unexpectedly acquired a reserved root")
			}
			if !errors.Is(err, ErrSpoolRootAlreadyOwned) {
				t.Fatalf("NewRootRuntime() error = %v, want ErrSpoolRootAlreadyOwned", err)
			}
		})
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	// An owner with no routes must release its reservation on Close.
	replacement := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	if replacement.Root().Path() != runtime.Root().Path() {
		t.Fatal("replacement runtime resolved a different root")
	}
}
