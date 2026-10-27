package config

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	"github.com/go-jappe/heterolog/pkg/env"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
)

// 送信先が指定したRetry-Afterの待機上限は、backendごとのspool runnerと圧縮ログspoolの
// 両方で秒単位に読み込み、未設定なら1時間とする。明示した0以下の値は無制限や既定値として
// 扱わず設定errorにする。
func TestMaxRetryAfterEnvironmentDefaultsToOneHourAndRejectsNonPositiveValues(t *testing.T) {
	standardDefaults := objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig().DurableStoreConfig.SpoolConfig
	deliveryDefaults := objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig().DurableStoreConfig.SpoolConfig
	tests := []struct {
		key  string
		load func() (time.Duration, error)
	}{
		{"HETEROLOG_OPENSEARCH_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", func() (time.Duration, error) {
			cfg, err := loadOpensearchSpoolRunnerConfig(processEnvironmentForTest())
			return cfg.Worker.MaxRetryAfter, err
		}},
		{"HETEROLOG_LOKI_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", func() (time.Duration, error) {
			cfg, err := loadLokiSpoolRunnerConfig(processEnvironmentForTest())
			return cfg.Worker.MaxRetryAfter, err
		}},
		{"HETEROLOG_CLOUDWATCH_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", func() (time.Duration, error) {
			cfg, err := loadCloudWatchLogsSpoolRunnerConfig(processEnvironmentForTest())
			return cfg.Worker.MaxRetryAfter, err
		}},
		{"HETEROLOG_CLICKHOUSE_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", func() (time.Duration, error) {
			cfg, err := loadClickHouseSpoolRunnerConfig(processEnvironmentForTest())
			return cfg.Worker.MaxRetryAfter, err
		}},
		{"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_MAX_RETRY_AFTER_SECOND", func() (time.Duration, error) {
			cfg, err := loadStandardObjectStorageCompressedLogFileUploadSpoolConfig(processEnvironmentForTest(), standardDefaults)
			return cfg.Runner.MaxRetryAfter, err
		}},
		{"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_MAX_RETRY_AFTER_SECOND", func() (time.Duration, error) {
			cfg, err := loadDeliveryObjectStorageCompressedLogFileUploadSpoolConfig(processEnvironmentForTest(), deliveryDefaults)
			return cfg.Runner.MaxRetryAfter, err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Setenv(tt.key, "")
			got, err := tt.load()
			if err != nil || got != time.Hour {
				t.Fatalf("default = %s, %v; want 1h", got, err)
			}

			t.Setenv(tt.key, "120")
			got, err = tt.load()
			if err != nil || got != 2*time.Minute {
				t.Fatalf("explicit 120 = %s, %v; want 2m", got, err)
			}

			for _, invalid := range []string{"0", "-1"} {
				t.Setenv(tt.key, invalid)
				_, err := tt.load()
				requireEnvironmentKeyError(t, err, tt.key, env.ErrNotPositive)
			}
		})
	}
}

func TestMaxRetryAfterDefaultsAreOneHour(t *testing.T) {
	if spoolrunner.DefaultMaxRetryAfter != time.Hour {
		t.Fatalf("spool runner default = %s, want 1h", spoolrunner.DefaultMaxRetryAfter)
	}
	for name, cfg := range map[string]objectstoragecompressedlog.FailureStoreConfig{
		"standard": objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig(),
		"delivery": objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig(),
	} {
		if got := cfg.DurableStoreConfig.SpoolConfig.Runner.MaxRetryAfter; got != time.Hour {
			t.Fatalf("%s compressed log spool default = %s, want 1h", name, got)
		}
	}
}

// mapperは各backendのspool runner設定と圧縮ログspool設定の待機上限を、そのまま実行時設定へ渡す。
func TestMaxRetryAfterIsMappedToRuntimeConfigs(t *testing.T) {
	mapped := mapBackendSpoolRunnerConfig(pkgspoolrunner.BackendSpoolRunnerConfig{
		Worker: pkgspoolrunner.WorkerConfig{MaxRetryAfter: 17 * time.Minute},
	})
	if mapped.Worker.MaxRetryAfter != 17*time.Minute {
		t.Fatalf("mapped spool runner max retry-after = %s, want 17m", mapped.Worker.MaxRetryAfter)
	}
}
