package config

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// backendDisplayTimeZoneは、既定のUTCと異なる表示用timezoneです。
const backendDisplayTimeZone = "America/New_York"

// TestBackendDisplayTimeConfigurationSurvivesEnvironmentAndMappingは、共通の時刻環境設定と
// log本文形式がsync・async・deliveryの各backend DTOまで欠落せず届くことを検証します。
// 時刻設定はcode既定値（ローカル時刻無効・UTC）と異なる値を与え、keyを読まない
// loaderでは失敗するようにします。
func TestBackendDisplayTimeConfigurationSurvivesEnvironmentAndMapping(t *testing.T) {
	root := t.TempDir()
	appCfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=backend-display-time-contract",
		"HETEROLOG_TARGET=backend-display-time-target",
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch,loki,clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch,loki,clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch,loki,clickhouse",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ENABLE_LOCAL_TIME_LOG=true",
		"HETEROLOG_TIMEZONE_LOG=" + backendDisplayTimeZone,
	})

	syncCfg := ToLoggerConfig(appCfg)
	asyncCfg := ToAsyncBestEffortMultiLoggerConfig(appCfg).ToLoggerConfig()
	deliveryCfg := ToDeliveryLoggerConfig(appCfg)

	assertBackendDisplayTimeConfig := func(
		name string,
		timeCfg pkglogger.TimeConfig,
		bodyCfg pkglogger.LogBodyConfig,
	) {
		t.Helper()
		if !timeCfg.IsLocalTimeEnabled || timeCfg.TimeZone != backendDisplayTimeZone {
			t.Fatalf("%s TimeConfig = %#v, want enabled %s", name, timeCfg, backendDisplayTimeZone)
		}
		if bodyCfg.LogLineDateFormat != logconfig.LogLineDateFormat {
			t.Fatalf(
				"%s LogLineDateFormat = %q, want %q",
				name,
				bodyCfg.LogLineDateFormat,
				logconfig.LogLineDateFormat,
			)
		}
	}

	assertBackendDisplayTimeConfig(
		"sync OpenSearch",
		syncCfg.OpenSearch.TimeConfig,
		syncCfg.OpenSearch.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"async OpenSearch",
		asyncCfg.OpenSearch.TimeConfig,
		asyncCfg.OpenSearch.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"delivery OpenSearch",
		deliveryCfg.OpenSearch.TimeConfig,
		deliveryCfg.OpenSearch.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"sync Loki",
		syncCfg.Loki.TimeConfig,
		syncCfg.Loki.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"async Loki",
		asyncCfg.Loki.TimeConfig,
		asyncCfg.Loki.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"delivery Loki",
		deliveryCfg.Loki.TimeConfig,
		deliveryCfg.Loki.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"sync ClickHouse",
		syncCfg.ClickHouse.TimeConfig,
		syncCfg.ClickHouse.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"async ClickHouse",
		asyncCfg.ClickHouse.TimeConfig,
		asyncCfg.ClickHouse.LogBodyConfig,
	)
	assertBackendDisplayTimeConfig(
		"delivery ClickHouse",
		deliveryCfg.ClickHouse.TimeConfig,
		deliveryCfg.ClickHouse.LogBodyConfig,
	)
}
