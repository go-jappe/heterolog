package compressedlogfailure

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestConfigAdapterSnapshotsDurableTypesAndChildConfigs(t *testing.T) {
	cfg := testFailureStoreConfig(" archive root ", []failurestore.DurableType{
		failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool,
	})
	cfg.DurableStoreConfig.FileArchiveConfig.LoggerContract = " custom contract "
	cfg.DurableStoreConfig.FileArchiveConfig.LifecycleInterval = -time.Second
	cfg.DurableStoreConfig.FileArchiveConfig.Retryable = objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 101,
	}
	cfg.DurableStoreConfig.FileArchiveConfig.Permanent = objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled: false, MaxFileAgeDays: 2, MaxDirSize: 202,
	}
	want := cfg
	want.DurableType = slices.Clone(cfg.DurableType)
	adapter := NewConfigAdapter(cfg)
	archiveAdapter := NewFileArchiveConfigAdapter(cfg.DurableStoreConfig.FileArchiveConfig)
	cfg.DurableType[0] = failurestore.DurableTypeNone
	cfg.DurableStoreConfig = objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{}
	returnedTypes := adapter.DurableType()
	returnedTypes[1] = failurestore.DurableTypeNone
	if got := adapter.DurableType(); !slices.Equal(got, want.DurableType) {
		t.Fatalf("DurableType() = %v, want independent snapshot %v", got, want.DurableType)
	}

	for _, provider := range []FileArchiveConfigProvider{archiveAdapter, adapter.FileArchiveConfig()} {
		got := objectstorage.CompressedLogFileUploadFailureArchiveConfig{
			RootPath:          provider.RootPath(),
			LoggerContract:    provider.LoggerContract(),
			LifecycleInterval: provider.LifecycleInterval(),
			Retryable:         provider.RetryablePurge(),
			Permanent:         provider.PermanentPurge(),
		}
		if got != want.DurableStoreConfig.FileArchiveConfig {
			t.Fatalf("archive snapshot = %+v, want unchanged values %+v", got, want.DurableStoreConfig.FileArchiveConfig)
		}
	}

}

type selectedConfigProvider struct {
	types   []failurestore.DurableType
	archive FileArchiveConfigProvider
}

func (p selectedConfigProvider) DurableType() []failurestore.DurableType { return p.types }

func (p selectedConfigProvider) FileArchiveConfig() FileArchiveConfigProvider {
	if !slices.Contains(p.types, failurestore.DurableTypeFileArchive) {
		panic("unselected archive config was read")
	}
	return p.archive
}

func TestManagedUploaderOptionalConfigProvidersPassThrough(t *testing.T) {
	remote := &testDestinationUploader{}
	for _, tc := range []struct {
		name     string
		provider ConfigProvider
	}{
		{name: "nil"},
		{name: "empty", provider: selectedConfigProvider{}},
		{name: "none", provider: selectedConfigProvider{types: []failurestore.DurableType{failurestore.DurableTypeNone}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewManagedUploader(ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: tc.provider,
				SaveTimeout:                         -time.Second,
			}, remote)
			if err != nil || got != remote {
				t.Fatalf("NewManagedUploader() = %T, %v; want unchanged uploader", got, err)
			}
		})
	}
}

func TestManagedUploaderReadsOnlySelectedConfigProvider(t *testing.T) {
	for _, selected := range []failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive} {
		for _, missing := range []bool{false, true} {
			name := string(selected)
			if missing {
				name += "/missing"
			}
			t.Run(name, func(t *testing.T) {
				cfg := testFailureStoreConfig(t.TempDir(), []failurestore.DurableType{selected})
				adapter := NewConfigAdapter(cfg)
				provider := selectedConfigProvider{types: cfg.DurableType}
				var spoolRuntime compressedlog.RootRuntimes
				if !missing {
					if selected == failurestore.DurableTypeSpool {
						spoolRuntime = newTestSpoolRuntimes(t, t.TempDir())
					}
					provider.archive = adapter.FileArchiveConfig()
				}
				got, err := NewManagedUploader(ManagedUploaderConfig{
					CompressedLogFileUploadFailureStore: provider,
					SpoolRuntimes:                       spoolRuntime,
					FileName:                            logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: ".log"},
				}, &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}})
				if managed, ok := got.(*ManagedUploader); ok {
					t.Cleanup(func() {
						if err := managed.Close(); err != nil {
							t.Errorf("Close() error = %v", err)
						}
					})
				}
				if missing {
					wantErr := ErrInvalidConfig
					if got != nil || !errors.Is(err, wantErr) {
						t.Fatalf("missing selected config returned %T, %v; want %v", got, err, wantErr)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				managed, ok := got.(*ManagedUploader)
				if !ok {
					t.Fatalf("NewManagedUploader() = %T, want *ManagedUploader", got)
				}
				targets := managed.failureTargets["destination-a"]
				if len(targets) != 1 || targets[0].kind() != selected {
					t.Fatalf("constructed stores do not match selected type %s", selected)
				}
			})
		}
	}
}

// このproviderは、adapterのgetter経由で保存先と周期が取得されることを確認します。
type overridingArchiveConfigProvider struct {
	FileArchiveConfigAdapter
	rootPath string
	interval time.Duration
	calls    map[string]int
}

func (p overridingArchiveConfigProvider) RootPath() string {
	p.calls["root"]++
	return p.rootPath
}

func (p overridingArchiveConfigProvider) LifecycleInterval() time.Duration {
	p.calls["interval"]++
	return p.interval
}

func TestManagedUploaderSharesArchiveLifecycleByRoot(t *testing.T) {
	cfg := testFailureStoreConfig(t.TempDir(), nil).DurableStoreConfig.FileArchiveConfig
	cfg.Retryable = objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 101,
	}
	cfg.Permanent = objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled: true, MaxFileAgeDays: 2, MaxDirSize: 202,
	}
	embeddedConfig := cfg
	embeddedConfig.RootPath = ""
	embeddedConfig.LifecycleInterval = -time.Second
	firstProvider := overridingArchiveConfigProvider{
		FileArchiveConfigAdapter: NewFileArchiveConfigAdapter(embeddedConfig),
		rootPath:                 cfg.RootPath,
		interval:                 cfg.LifecycleInterval,
		calls:                    make(map[string]int),
	}
	secondProvider := firstProvider
	secondProvider.calls = make(map[string]int)
	managedConfig := func(archive FileArchiveConfigProvider) ManagedUploaderConfig {
		return ManagedUploaderConfig{
			CompressedLogFileUploadFailureStore: selectedConfigProvider{
				types:   []failurestore.DurableType{failurestore.DurableTypeFileArchive},
				archive: archive,
			},
			FileName: logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: ".log"},
		}
	}
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
	var first *ManagedUploader
	for _, provider := range []FileArchiveConfigProvider{firstProvider, secondProvider, NewFileArchiveConfigAdapter(cfg)} {
		managed := newTestManagedUploader(t, managedConfig(provider), remote)
		if first == nil {
			first = managed
		} else if archiveRouteForTest(t, managed, "destination-a").owner.coordinator != archiveRouteForTest(t, first, "destination-a").owner.coordinator {
			t.Fatalf("same root from %T did not share the archive lifecycle", provider)
		}
	}
	if got, want := archiveRouteForTest(t, first, "destination-a").store.Root().Path(), newArchiveRoot(t, cfg.RootPath).Path(); got != want {
		t.Fatalf("archive root = %q, want provider root %q", got, want)
	}
	coordinator := archiveRouteForTest(t, first, "destination-a").owner.coordinator
	if coordinator.interval != cfg.LifecycleInterval {
		t.Fatalf("archive lifecycle interval = %v, want %v", coordinator.interval, cfg.LifecycleInterval)
	}
	for i, provider := range []overridingArchiveConfigProvider{firstProvider, secondProvider} {
		for _, field := range []string{"root", "interval"} {
			if got := provider.calls[field]; got != 1 {
				t.Errorf("provider %d %s getter calls = %d, want 1", i, field, got)
			}
		}
	}
}

// NewManagedUploaderは、durable typeの選択が不正な理由をErrInvalidConfigと共に返し、
// failurestore.DurableTypeErrorとその種類をerrors.Is・errors.Asで辿れるようにします。
func TestManagedUploaderDurableTypeErrorKeepsValidationCause(t *testing.T) {
	for _, tc := range []struct {
		name     string
		types    []failurestore.DurableType
		wantKind error
		wantType failurestore.DurableType
	}{
		{
			name:     "duplicate",
			types:    []failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeSpool},
			wantKind: failurestore.ErrDuplicateDurableType,
			wantType: failurestore.DurableTypeSpool,
		},
		{
			name:     "unsupported",
			types:    []failurestore.DurableType{failurestore.DurableTypeFileArchive, "stdout"},
			wantKind: failurestore.ErrUnsupportedDurableType,
			wantType: "stdout",
		},
		{
			name:     "none with another type",
			types:    []failurestore.DurableType{failurestore.DurableTypeNone, failurestore.DurableTypeSpool},
			wantKind: failurestore.ErrDurableTypeNoneNotAlone,
			wantType: failurestore.DurableTypeNone,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewManagedUploader(ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: selectedConfigProvider{types: tc.types},
			}, &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}})
			if got != nil {
				t.Fatalf("NewManagedUploader() = %T, want nil uploader for invalid durable types", got)
			}
			if !errors.Is(err, ErrInvalidConfig) || !errors.Is(err, tc.wantKind) {
				t.Fatalf("NewManagedUploader() error = %v, want ErrInvalidConfig and %v", err, tc.wantKind)
			}
			var durableErr *failurestore.DurableTypeError
			if !errors.As(err, &durableErr) {
				t.Fatalf("NewManagedUploader() error = %v, want *failurestore.DurableTypeError as its cause", err)
			}
			if durableErr.Type != tc.wantType {
				t.Fatalf("DurableTypeError.Type = %q, want %q", durableErr.Type, tc.wantType)
			}
		})
	}
}
