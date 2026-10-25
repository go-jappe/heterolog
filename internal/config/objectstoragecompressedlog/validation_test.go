package objectstoragecompressedlog

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/failurestore"
)

func TestValidateConfigAcceptsDefaultFailureStores(t *testing.T) {
	cfg := Config{
		Standard: DefaultStandardCompressedLogFileUploadFailureStoreConfig(),
		Delivery: DefaultDeliveryCompressedLogFileUploadFailureStoreConfig(),
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatalf("ValidateConfig() error = %v", err)
	}
}

func TestValidateConfigIgnoresUnusedContractZeroValue(t *testing.T) {
	if err := ValidateConfig(Config{}); err != nil {
		t.Fatalf("ValidateConfig() error = %v", err)
	}
}

func TestValidateConfigRejectsInvalidDurableTypeSelection(t *testing.T) {
	tests := []struct {
		name     string
		types    []failurestore.DurableType
		wantKind error
		wantType failurestore.DurableType
	}{
		{
			name: "none mixed with store",
			types: []failurestore.DurableType{
				failurestore.DurableTypeNone,
				failurestore.DurableTypeSpool,
			},
			wantKind: failurestore.ErrDurableTypeNoneNotAlone,
			wantType: failurestore.DurableTypeNone,
		},
		{
			name: "duplicate store",
			types: []failurestore.DurableType{
				failurestore.DurableTypeFileArchive,
				failurestore.DurableTypeFileArchive,
			},
			wantKind: failurestore.ErrDuplicateDurableType,
			wantType: failurestore.DurableTypeFileArchive,
		},
		{
			name:     "unsupported store",
			types:    []failurestore.DurableType{"unknown"},
			wantKind: failurestore.ErrUnsupportedDurableType,
			wantType: "unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{Standard: DefaultStandardCompressedLogFileUploadFailureStoreConfig()}
			cfg.Standard.DurableType = test.types
			err := ValidateConfig(cfg)
			var typeErr *failurestore.DurableTypeError
			if !errors.As(err, &typeErr) || !errors.Is(typeErr, test.wantKind) || typeErr.Type != test.wantType ||
				typeErr.Scope != "standard compressed log file upload failure" {
				t.Fatalf("ValidateConfig() error = %v, want standard %v for %q", err, test.wantKind, test.wantType)
			}
		})
	}
}

func TestValidateConfigValidatesOnlySelectedDurableSubconfigs(t *testing.T) {
	t.Run("file archive", func(t *testing.T) {
		cfg := DefaultStandardCompressedLogFileUploadFailureStoreConfig()
		cfg.DurableType = []failurestore.DurableType{failurestore.DurableTypeFileArchive}
		cfg.DurableStoreConfig.SpoolConfig = SpoolConfig{}
		if err := ValidateConfig(Config{Standard: cfg}); err != nil {
			t.Fatalf("unselected spool config produced error: %v", err)
		}

		cfg.DurableStoreConfig.FileArchiveConfig.LifecycleInterval = 0
		err := ValidateConfig(Config{Standard: cfg})
		requireSettingError(t, err, "standard", "", SettingFileArchiveLifecycleInterval, ErrNonPositiveSetting, time.Duration(0))
	})

	t.Run("spool", func(t *testing.T) {
		cfg := DefaultStandardCompressedLogFileUploadFailureStoreConfig()
		cfg.DurableType = []failurestore.DurableType{failurestore.DurableTypeSpool}
		cfg.DurableStoreConfig.FileArchiveConfig = FileArchiveConfig{}
		if err := ValidateConfig(Config{Standard: cfg}); err != nil {
			t.Fatalf("unselected file archive config produced error: %v", err)
		}

		cfg.DurableStoreConfig.SpoolConfig.CircuitBreaker.OpenCooldown = 0
		err := ValidateConfig(Config{Standard: cfg})
		requireSettingError(t, err, "standard", "", SettingSpoolCircuitBreakerOpenCooldown, ErrNonPositiveSetting, time.Duration(0))
	})

	t.Run("none", func(t *testing.T) {
		cfg := FailureStoreConfig{
			DurableType: []failurestore.DurableType{failurestore.DurableTypeNone},
		}
		if err := ValidateConfig(Config{Standard: cfg}); err != nil {
			t.Fatalf("none selected with zero subconfigs: %v", err)
		}
	})
}

func TestValidateConfigAllowsDisabledPurgeWithoutLimits(t *testing.T) {
	cfg := DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	cfg.DurableStoreConfig.FileArchiveConfig.Retryable = PurgeConfig{}
	cfg.DurableStoreConfig.SpoolConfig.Lifecycle.Done = PurgeConfig{}
	if err := ValidateConfig(Config{Standard: cfg}); err != nil {
		t.Fatalf("ValidateConfig() error = %v", err)
	}
}

func TestValidateConfigRejectsUnsafeFileArchiveDirectoryName(t *testing.T) {
	tests := []string{" archive", "archive ", "nested/archive", `nested\archive`, ".", ".."}
	for _, directoryName := range tests {
		t.Run(directoryName, func(t *testing.T) {
			cfg := DefaultStandardCompressedLogFileUploadFailureStoreConfig()
			cfg.DurableType = []failurestore.DurableType{failurestore.DurableTypeFileArchive}
			cfg.DurableStoreConfig.FileArchiveConfig.DirName = directoryName
			err := ValidateConfig(Config{Standard: cfg})
			requireSettingError(t, err, "standard", "", SettingFileArchiveDirName, ErrInvalidDirectoryName, directoryName)
		})
	}
}

// Retry-Afterの待機上限は、このfieldを持たない既存設定との互換のため0を既定値として受け入れ、
// 負値だけを設定errorにする。
func TestValidateConfigRejectsNegativeSpoolMaxRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		name          string
		maxRetryAfter time.Duration
		wantError     bool
	}{
		{name: "default", maxRetryAfter: DefaultMaxRetryAfter},
		{name: "unset field", maxRetryAfter: 0},
		{name: "negative", maxRetryAfter: -time.Second, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Standard: DefaultStandardCompressedLogFileUploadFailureStoreConfig(),
				Delivery: DefaultDeliveryCompressedLogFileUploadFailureStoreConfig(),
			}
			cfg.Delivery.DurableStoreConfig.SpoolConfig.Runner.MaxRetryAfter = tt.maxRetryAfter
			err := ValidateConfig(cfg)
			if tt.wantError {
				requireSettingError(t, err, "delivery", "", SettingSpoolMaxRetryAfter, ErrNegativeSetting, tt.maxRetryAfter)
				return
			}
			if err != nil {
				t.Fatalf("ValidateConfig() error = %v", err)
			}
		})
	}
}

// requireSettingErrorは、errがcontractの設定でdestinationのsettingを値valueについてkindの理由で
// 拒否したSettingErrorを含むことを確認します。
func requireSettingError(
	t *testing.T,
	err error,
	contract string,
	destination string,
	setting Setting,
	kind error,
	value any,
) {
	t.Helper()
	if _, ok := errortree.Find(err, func(settingErr *SettingError) bool {
		return settingErr.Contract == contract && settingErr.Destination == destination &&
			settingErr.Setting == setting && settingErr.Value == value && errors.Is(settingErr, kind)
	}); !ok {
		t.Fatalf("ValidateConfig() error = %v, want %s %s %v for value %v", err, contract, setting, kind, value)
	}
}
