package compressedlogarchive

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestPurgeConfigForFailureCategory(t *testing.T) {
	t.Parallel()

	config := PurgeConfig{
		Retryable: enabledArchivePurgeConfig(3, 123),
		Permanent: enabledArchivePurgeConfig(7, 456),
	}
	for _, test := range []struct {
		name     string
		category uploader.FailureCategory
		config   PurgeConfig
		want     pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
		wantErr  bool
	}{
		{name: "retryable", category: uploader.FailureCategoryRetryable, config: config, want: config.Retryable},
		{name: "permanent", category: uploader.FailureCategoryPermanent, config: config, want: config.Permanent},
		{name: "disabled retryable", category: uploader.FailureCategoryRetryable},
		{name: "disabled permanent", category: uploader.FailureCategoryPermanent},
		{name: "empty category", config: config, wantErr: true},
		{name: "unknown category", category: "unknown", config: config, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := purgeConfigForFailureCategory(NewPurgeConfigAdapter(test.config), test.category)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidFailureCategory) {
					t.Fatalf("error = %v, want ErrInvalidFailureCategory", err)
				}
				if got != nil {
					t.Fatalf("config = %#v, want nil on error", got)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if failureCategoryPurgeConfigValues(got) != test.want {
				t.Fatalf("config = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestPurgeConfigAdapterPreservesValues(t *testing.T) {
	t.Parallel()

	for _, config := range []PurgeConfig{
		{},
		{
			Retryable: enabledArchivePurgeConfig(3, 123),
			Permanent: enabledArchivePurgeConfig(7, 456),
		},
		{
			Retryable: pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{MaxFileAgeDays: -1},
			Permanent: pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{Enabled: true},
		},
	} {
		want := config
		adapter := NewPurgeConfigAdapter(config)
		config.Retryable = enabledArchivePurgeConfig(99, 999)
		config.Permanent = config.Retryable
		if failureCategoryPurgeConfigValues(adapter.RetryablePurge()) != want.Retryable ||
			failureCategoryPurgeConfigValues(adapter.PermanentPurge()) != want.Permanent {
			t.Fatalf("adapter changed construction values: got %#v, want %#v", adapter, want)
		}
	}
}

type testPurgeConfigProvider struct {
	nilCategory uploader.FailureCategory
	retryable   pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
	permanent   pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
}

func (p testPurgeConfigProvider) RetryablePurge() FailureCategoryPurgeConfigProvider {
	if p.nilCategory == uploader.FailureCategoryRetryable {
		return nil
	}
	return testFailureCategoryPurgeConfigProvider{values: p.retryable}
}

func (p testPurgeConfigProvider) PermanentPurge() FailureCategoryPurgeConfigProvider {
	if p.nilCategory == uploader.FailureCategoryPermanent {
		return nil
	}
	return testFailureCategoryPurgeConfigProvider{values: p.permanent}
}

func TestPurgeConfigValidation(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	for _, test := range []struct {
		name    string
		config  PurgeConfigProvider
		wantErr *FailureCategoryPurgeConfigError
	}{
		{name: "nil"},
		{
			name:    "retryable nil provider",
			config:  testPurgeConfigProvider{nilCategory: uploader.FailureCategoryRetryable},
			wantErr: &FailureCategoryPurgeConfigError{FailureCategory: uploader.FailureCategoryRetryable, Err: ErrNilFailureCategoryPurgeConfig},
		},
		{
			name:    "permanent nil provider",
			config:  testPurgeConfigProvider{nilCategory: uploader.FailureCategoryPermanent},
			wantErr: &FailureCategoryPurgeConfigError{FailureCategory: uploader.FailureCategoryPermanent, Err: ErrNilFailureCategoryPurgeConfig},
		},
		{
			name:    "retryable age",
			config:  testPurgeConfigProvider{retryable: enabledArchivePurgeConfig(0, 1)},
			wantErr: &FailureCategoryPurgeConfigError{FailureCategory: uploader.FailureCategoryRetryable, Err: ErrInvalidPurgeMaxFileAgeDays},
		},
		{
			name:    "retryable capacity",
			config:  testPurgeConfigProvider{retryable: enabledArchivePurgeConfig(1, 0)},
			wantErr: &FailureCategoryPurgeConfigError{FailureCategory: uploader.FailureCategoryRetryable, Err: ErrInvalidPurgeMaxDirSize},
		},
		{
			name:    "permanent age",
			config:  testPurgeConfigProvider{permanent: enabledArchivePurgeConfig(-1, 1)},
			wantErr: &FailureCategoryPurgeConfigError{FailureCategory: uploader.FailureCategoryPermanent, Err: ErrInvalidPurgeMaxFileAgeDays, MaxFileAgeDays: -1},
		},
		{
			name:    "permanent capacity",
			config:  testPurgeConfigProvider{permanent: enabledArchivePurgeConfig(1, 0)},
			wantErr: &FailureCategoryPurgeConfigError{FailureCategory: uploader.FailureCategoryPermanent, Err: ErrInvalidPurgeMaxDirSize},
		},
		{
			name: "disabled categories",
			config: testPurgeConfigProvider{
				retryable: pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{MaxFileAgeDays: -1},
				permanent: pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{MaxFileAgeDays: -1},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, constructorErr := NewPurger(store, test.config, clock.NewUTCClock())
			for name, err := range map[string]error{
				"NewPurger":           constructorErr,
				"ValidatePurgeConfig": ValidatePurgeConfig(test.config),
			} {
				switch {
				case test.config == nil:
					if !errors.Is(err, ErrNilPurgeConfig) {
						t.Errorf("%s() error = %v, want ErrNilPurgeConfig", name, err)
					}
				case test.wantErr == nil:
					if err != nil {
						t.Errorf("%s() error = %v", name, err)
					}
				default:
					if _, found := errortree.Find(err, func(got *FailureCategoryPurgeConfigError) bool {
						return *got == *test.wantErr
					}); !found {
						t.Errorf("%s() error = %v, want %+v", name, err, *test.wantErr)
					}
				}
			}
		})
	}
}

func TestPurgerUsesCustomConfigProvider(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	retryable := publishPurgeTestCompressedLog(t, store, "custom-retryable_20000101_1", uploader.FailureCategoryRetryable, "old retryable")
	permanent := publishPurgeTestCompressedLog(t, store, "custom-permanent_"+time.Now().UTC().Format("20060102")+"_1", uploader.FailureCategoryPermanent, "over capacity")
	setFileTime(t, retryable.Path, time.Now().UTC().Add(-72*time.Hour))
	purger, err := NewPurger(store, testPurgeConfigProvider{
		retryable: enabledArchivePurgeConfig(1, math.MaxUint64),
		permanent: enabledArchivePurgeConfig(365, 1),
	}, clock.NewUTCClock())
	if err != nil {
		t.Fatalf("NewPurger() error = %v", err)
	}
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.ByFailureCategory[uploader.FailureCategoryRetryable].DeletedFileCount != 1 {
		t.Fatalf("retryable report = %#v", report.ByFailureCategory[uploader.FailureCategoryRetryable])
	}
	if _, err := os.Lstat(retryable.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired retryable file remains: %v", err)
	}
	permanentReport := report.ByFailureCategory[uploader.FailureCategoryPermanent]
	if permanentReport.DeletedFileCount != 1 || permanentReport.CapacityLimitExceeded {
		t.Fatalf("permanent report = %#v", permanentReport)
	}
	if _, err := os.Lstat(permanent.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("over-capacity permanent file remains: %v", err)
	}
}

func failureCategoryPurgeConfigValues(config FailureCategoryPurgeConfigProvider) pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig {
	return pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled: config.Enabled(), MaxFileAgeDays: config.MaxFileAgeDays(), MaxDirSize: config.MaxDirSize(),
	}
}

// 独自providerでもpurgerが具体型に依存せずメソッドから条件を取得することを確認します。
type testFailureCategoryPurgeConfigProvider struct {
	values pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
}

func (p testFailureCategoryPurgeConfigProvider) Enabled() bool {
	return p.values.Enabled
}
func (p testFailureCategoryPurgeConfigProvider) MaxFileAgeDays() int {
	return p.values.MaxFileAgeDays
}
func (p testFailureCategoryPurgeConfigProvider) MaxDirSize() uint64 {
	return p.values.MaxDirSize
}
