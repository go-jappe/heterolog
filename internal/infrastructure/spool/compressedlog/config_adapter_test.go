package compressedlog

import (
	"errors"
	"maps"
	"testing"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestConfigAdapterPreservesConfigSnapshot(t *testing.T) {
	service := objectstorage.UploadTarget(" custom service ")
	cfg := objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  " /spool/path ",
		Target:                     " target ",
		LoggerContract:             " custom contract ",
		RunnerInterval:             -time.Second,
		LifecycleInterval:          0,
		CircuitBreakerOpenCooldown: 37 * time.Second,
		MaxRetryAfter:              41 * time.Minute,
		Done: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 101,
		},
		Dead: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: false, MaxFileAgeDays: 2, MaxDirSize: 202,
		},
		Quarantine: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: true, MaxFileAgeDays: 3, MaxDirSize: 303,
		},
	}
	want := cfg
	adapter := NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), service)
	cfg = objectstorage.CompressedLogFileUploadSpoolConfig{}
	if got := adapter.Service(); got != service {
		t.Fatalf("adapter service = %q, want %q", got, service)
	}

	got := objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  adapter.SpoolPath(),
		Target:                     adapter.Target(),
		LoggerContract:             adapter.LoggerContract(),
		RunnerInterval:             adapter.RunnerInterval(),
		LifecycleInterval:          adapter.LifecycleInterval(),
		CircuitBreakerOpenCooldown: adapter.CircuitBreakerOpenCooldown(),
		MaxRetryAfter:              adapter.MaxRetryAfter(),
		Done:                       statePurgeConfigValues(adapter.DonePurge()),
		Dead:                       statePurgeConfigValues(adapter.DeadPurge()),
		Quarantine:                 statePurgeConfigValues(adapter.QuarantinePurge()),
	}
	if got != want {
		t.Fatalf("adapter snapshot = %+v, want unchanged values %+v", got, want)
	}
}

func TestCoordinatorConfigRejectNonpositiveProviderDurations(t *testing.T) {
	for _, duration := range []time.Duration{0, -time.Second} {
		for _, field := range []string{"runner", "lifecycle", "circuit breaker"} {
			t.Run(field+"/"+duration.String(), func(t *testing.T) {
				cfg := managedUploaderTestConfig("unused")
				switch field {
				case "runner":
					cfg.RunnerInterval = duration
				case "lifecycle":
					cfg.LifecycleInterval = duration
				case "circuit breaker":
					cfg.CircuitBreakerOpenCooldown = duration
				}
				if err := validateRootRuntimeConfig(newRootRuntimeConfigAdapter(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))); !errors.Is(err, ErrInvalidCompressedLogFileUploadSpoolConfig) {
					t.Fatalf("validateRootRuntimeConfig() error = %v, want invalid config", err)
				}
			})
		}
	}
}

func TestRootRuntimeRejectsNilConfigProvider(t *testing.T) {
	runtime, err := NewRootRuntime(nil)
	if runtime != nil {
		_ = runtime.Close()
		t.Fatal("NewRootRuntime() returned a root with a nil config provider")
	}
	if !errors.Is(err, ErrInvalidCompressedLogFileUploadSpoolConfig) {
		t.Fatalf("NewRootRuntime() error = %v, want invalid config", err)
	}
}

func TestRootRuntimeSnapshotsSettingsOnceForAllRouteRegistrations(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	cfg.LifecycleInterval = 2 * time.Hour
	cfg.MaxRetryAfter = 41 * time.Minute
	cfg.Done = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 101,
	}
	cfg.Dead = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled: true, MaxFileAgeDays: 2, MaxDirSize: 202,
	}
	cfg.Quarantine = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled: true, MaxFileAgeDays: 3, MaxDirSize: 303,
	}
	provider := &mutableCoordinatorConfigProvider{
		ConfigAdapter: NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3),
		config:        cfg,
		calls:         make(map[string]int),
	}
	runtime := newTestRootRuntime(t, provider)
	callsAtConstruction := maps.Clone(provider.calls)
	// Mutation after construction must not affect initialization or any later
	// registration; routes do not receive or re-read the root's provider.
	provider.config = objectstorage.CompressedLogFileUploadSpoolConfig{}
	for i := 0; i < 3; i++ {
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
		}, &replaytest.Uploader{})
		if err != nil {
			t.Fatalf("RegisterRoute() error = %v", err)
		}
		t.Cleanup(func() {
			if err := route.Close(); err != nil {
				t.Errorf("route Close() error = %v", err)
			}
		})
		if route.rootRuntime != runtime {
			t.Fatal("route does not directly reference its root runtime")
		}
	}
	config := runtime.runner.config.(rootRuntimeConfigProvider)
	if config.Interval() != cfg.RunnerInterval ||
		config.PurgeInterval() != cfg.LifecycleInterval ||
		config.CircuitBreakerOpenCooldown() != cfg.CircuitBreakerOpenCooldown ||
		config.MaxRetryAfter() != cfg.MaxRetryAfter ||
		config.CapacityPurgeMinimumFileAge() != cfg.LifecycleInterval ||
		statePurgeConfigValues(config.DonePurge()) != cfg.Done ||
		statePurgeConfigValues(config.DeadPurge()) != cfg.Dead ||
		statePurgeConfigValues(config.QuarantinePurge()) != cfg.Quarantine {
		t.Fatalf("coordinator config does not match original provider values: %+v", config)
	}
	for _, field := range []string{"runner", "lifecycle", "circuit breaker", "max retry-after", "done", "dead", "quarantine"} {
		if callsAtConstruction[field] == 0 {
			t.Errorf("%s getter was not read at construction", field)
		}
	}
	if !maps.Equal(provider.calls, callsAtConstruction) {
		t.Errorf("provider getter calls after route registrations = %v, want unchanged %v", provider.calls, callsAtConstruction)
	}
}

// NewRootRuntimeの設定errorは、ErrInvalidCompressedLogFileUploadSpoolConfigと共に、
// 不正な理由を表す下位のsentinelもerrors.Isで辿れるようにします。
func TestNewRootRuntimeConfigErrorsKeepValidationCause(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*objectstorage.CompressedLogFileUploadSpoolConfig)
		want   error
	}{
		{
			name:   "empty spool path",
			change: func(cfg *objectstorage.CompressedLogFileUploadSpoolConfig) { cfg.SpoolPath = "" },
			want:   objectstoragespool.ErrInvalidRoot,
		},
		{
			name:   "nonpositive circuit breaker open cooldown",
			change: func(cfg *objectstorage.CompressedLogFileUploadSpoolConfig) { cfg.CircuitBreakerOpenCooldown = 0 },
			want:   compressedlogworker.ErrInvalidCircuitBreakerOpenCooldown,
		},
		{
			name:   "negative max retry after",
			change: func(cfg *objectstorage.CompressedLogFileUploadSpoolConfig) { cfg.MaxRetryAfter = -time.Second },
			want:   compressedlogworker.ErrInvalidMaxRetryAfter,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := managedUploaderTestConfig(t.TempDir())
			tc.change(&cfg)
			runtime, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
			if runtime != nil {
				_ = runtime.Close()
				t.Fatal("NewRootRuntime() returned a root for an invalid config")
			}
			if !errors.Is(err, ErrInvalidCompressedLogFileUploadSpoolConfig) || !errors.Is(err, tc.want) {
				t.Fatalf("NewRootRuntime() error = %v, want ErrInvalidCompressedLogFileUploadSpoolConfig and %v", err, tc.want)
			}
		})
	}
}
