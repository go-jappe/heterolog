package config

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type retryClientEnvironmentRoute struct {
	name          string
	destination   string
	enableKey     string
	logTypeKey    string
	policyKey     string
	handlerKey    string
	modeKey       string
	maxRetriesKey string
}

// TestMappedConfigRejectsNegativeRetryCountForEveryLoggerRouteは、全logger経路で、
// 再試行の回数の負数が.envの読み込みでは整数として読まれ、pkg/loggerの設定へ変換した後の
// 検証でerrorになることを検証します。値の範囲は、設定を使う側の検証が受け持ちます。
func TestMappedConfigRejectsNegativeRetryCountForEveryLoggerRoute(t *testing.T) {
	for _, route := range retryClientEnvironmentRoutes() {
		route := route
		for _, handlerOnly := range []bool{false, true} {
			handlerOnly := handlerOnly
			name := "selected_destination"
			if handlerOnly {
				name = "handler_only_destination"
			}

			t.Run(route.name+"/"+name, func(t *testing.T) {
				lines := retryClientRouteEnv(route, handlerOnly)
				lines = append(lines,
					route.modeKey+"=retry_policy",
					route.maxRetriesKey+"=-1",
				)

				cfg, err := loadConfigErrorFromTempEnv(t, lines)
				if err != nil {
					t.Fatalf("LoadConfig() error = %v, want the negative retry count to be read as an integer", err)
				}
				requireNegativeRetryCountError(t, mapRetryClientRouteConfig(route, cfg), route.destination)
			})
		}
	}
}

// TestLoadConfigAcceptsZeroRetryCountは、再試行の回数の0が不正な値ではなく、
// 再試行なしとして読み込まれ、変換後の検証も通ることを検証します。
func TestLoadConfigAcceptsZeroRetryCount(t *testing.T) {
	for _, route := range retryClientEnvironmentRoutes() {
		route := route
		t.Run(route.name, func(t *testing.T) {
			lines := retryClientRouteEnv(route, false)
			lines = append(lines,
				"HETEROLOG_SYSTEM_LIB_DIR="+filepath.Join(t.TempDir(), "spool"),
				route.modeKey+"=retry_policy",
				route.maxRetriesKey+"=0",
			)

			cfg, err := loadConfigErrorFromTempEnv(t, lines)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v, want zero retries to be valid", err)
			}
			// 最小限の設定では認証情報などの別の検証が失敗することがあるため、再試行の回数を
			// 拒否するerrorがないことだけを確かめます。
			for _, fieldErr := range pkglogger.ConfigFieldErrors(mapRetryClientRouteConfig(route, cfg)) {
				if fieldErr.Field == pkglogger.ConfigFieldRetryMaxRetries {
					t.Fatalf("mapped config error = %v, want zero retries to be valid", fieldErr)
				}
			}
		})
	}
}

// TestLoadConfigRejectsInvalidRetryModeForEveryLoggerRouteは、sync / async /
// deliveryと4 backendの全組み合わせについて、通常出力先とhandler専用出力先の
// どちらから読み込まれた設定でも、不正なmodeが起動時エラーになることを検証します。
func TestLoadConfigRejectsInvalidRetryModeForEveryLoggerRoute(t *testing.T) {
	for _, route := range retryClientEnvironmentRoutes() {
		route := route
		for _, handlerOnly := range []bool{false, true} {
			handlerOnly := handlerOnly
			name := "selected_destination"
			if handlerOnly {
				name = "handler_only_destination"
			}

			t.Run(route.name+"/"+name, func(t *testing.T) {
				lines := retryClientRouteEnv(route, handlerOnly)
				lines = append(lines, route.modeKey+"=not_a_retry_mode")

				_, err := loadConfigErrorFromTempEnv(t, lines)
				if err == nil {
					t.Fatal("LoadConfig() error = nil, want invalid retry mode error")
				}
				requireEnvironmentKeyError(t, err, route.modeKey, ErrUnsupportedRetryClientMode)
			})
		}
	}
}

// TestLoadConfigRejectsExplicitInvalidRetryNumericValuesは、retry modeで値を使用するかに
// かかわらず、.envへ明示した再試行の回数と待機時間が整数として読めない場合に、読み込みの
// エラーになることを検証します。値の範囲（負の回数、0以下の待機時間）は、pkg/loggerの設定へ
// 変換した後の検証が受け持ちます。
func TestLoadConfigRejectsExplicitInvalidRetryNumericValues(t *testing.T) {
	tests := []struct {
		name  string
		mode  string
		key   string
		value string
		kind  error
	}{
		{
			name:  "malformed max retries",
			mode:  "retry_policy",
			key:   "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES",
			value: "three",
			kind:  env.ErrNotInteger,
		},
		{
			name:  "malformed max delay",
			mode:  "retry_policy_with_breaker",
			key:   "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND",
			value: "one_second",
			kind:  env.ErrNotInteger,
		},
		{
			name:  "max delay overflows duration",
			mode:  "retry_policy",
			key:   "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND",
			value: "9223372036855",
			kind:  env.ErrOutOfRange,
		},
	}

	route := retryClientEnvironmentRoute{
		name:        "sync/loki",
		destination: "loki",
		enableKey:   "HETEROLOG_ENABLE_LOGGING",
		logTypeKey:  "HETEROLOG_LOG_TYPE",
		policyKey:   "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
		handlerKey:  "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
		modeKey:     "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			lines := retryClientRouteEnv(route, false)
			lines = append(lines,
				"HETEROLOG_LOKI_RETRY_CLIENT_MODE="+tt.mode,
				tt.key+"="+tt.value,
			)

			_, err := loadConfigErrorFromTempEnv(t, lines)
			if err == nil {
				t.Fatal("LoadConfig() error = nil, want invalid explicit retry numeric error")
			}
			requireEnvironmentKeyError(t, err, tt.key, tt.kind)
		})
	}
}

// TestMappedConfigRejectsNonPositiveRetryDelaysは、再試行するmodeで0以下の待機時間が、
// .envの読み込みでは整数として読まれ、pkg/loggerの設定へ変換した後の検証でerrorになることを
// 検証します。
func TestMappedConfigRejectsNonPositiveRetryDelays(t *testing.T) {
	route := lokiRetryClientEnvironmentRoute()
	for _, tc := range []struct {
		name  string
		key   string
		value string
		field pkglogger.ConfigField
	}{
		{name: "negative base delay", key: "HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND", value: "-1", field: pkglogger.ConfigFieldRetryBaseDelay},
		{name: "zero max delay", key: "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND", value: "0", field: pkglogger.ConfigFieldRetryMaxDelay},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := retryClientRouteEnv(route, false)
			lines = append(lines, "HETEROLOG_LOKI_RETRY_CLIENT_MODE=retry_with_breaker", tc.key+"="+tc.value)

			cfg, err := loadConfigErrorFromTempEnv(t, lines)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v, want the delay to be read as an integer", err)
			}
			err = mapRetryClientRouteConfig(route, cfg)
			for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
				if fieldErr.Backend == pkglogger.LogOutputLoki && fieldErr.Field == tc.field &&
					errors.Is(fieldErr, pkglogger.ErrNonPositiveSetting) {
					return
				}
			}
			t.Fatalf("mapped config error = %v, want a non-positive Loki %s error", err, tc.field)
		})
	}
}

// TestLoadConfigAcceptsUnusedRetryValuesInModesWithoutRetryは、再試行しないmodeでは、
// 使わない再試行の回数と待機時間が範囲外でもerrorにしないことを検証します。
func TestLoadConfigAcceptsUnusedRetryValuesInModesWithoutRetry(t *testing.T) {
	route := lokiRetryClientEnvironmentRoute()
	for _, mode := range []string{"none", "circuit_breaker"} {
		t.Run(mode, func(t *testing.T) {
			lines := retryClientRouteEnv(route, false)
			lines = append(lines,
				"HETEROLOG_LOKI_RETRY_CLIENT_MODE="+mode,
				"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES=-1",
				"HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND=0",
				"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND=-1",
			)

			cfg, err := loadConfigErrorFromTempEnv(t, lines)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v, want unused retry values to be accepted", err)
			}
			for _, fieldErr := range pkglogger.ConfigFieldErrors(mapRetryClientRouteConfig(route, cfg)) {
				switch fieldErr.Field {
				case pkglogger.ConfigFieldRetryMaxRetries, pkglogger.ConfigFieldRetryBaseDelay, pkglogger.ConfigFieldRetryMaxDelay:
					t.Fatalf("mapped config error = %v, want unused retry values to be accepted", fieldErr)
				}
			}
		})
	}
}

// lokiRetryClientEnvironmentRouteは、同期loggerの通常出力先をLokiにする経路です。
func lokiRetryClientEnvironmentRoute() retryClientEnvironmentRoute {
	return retryClientEnvironmentRoute{
		name:        "sync/loki",
		destination: "loki",
		enableKey:   "HETEROLOG_ENABLE_LOGGING",
		logTypeKey:  "HETEROLOG_LOG_TYPE",
		policyKey:   "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
		handlerKey:  "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
		modeKey:     "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
	}
}

func TestLoadConfigRetryNumericValuesUseDefaultsWhenMissingOrEmpty(t *testing.T) {
	root := t.TempDir()
	route := retryClientEnvironmentRoute{
		name:        "sync/loki",
		destination: "loki",
		enableKey:   "HETEROLOG_ENABLE_LOGGING",
		logTypeKey:  "HETEROLOG_LOG_TYPE",
		policyKey:   "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
		handlerKey:  "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
		modeKey:     "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
	}
	lines := retryClientRouteEnv(route, false)
	lines = append(lines,
		"HETEROLOG_SYSTEM_LIB_DIR="+filepath.Join(root, "spool"),
		"HETEROLOG_LOKI_RETRY_CLIENT_MODE=retry_policy",
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES=",
		"HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND=",
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND=",
	)

	if _, err := loadConfigErrorFromTempEnv(t, lines); err != nil {
		t.Fatalf("LoadConfig() with empty retry numeric values: %v", err)
	}
}

func TestLoadConfigIgnoresInvalidRetrySettingsForUnselectedBackend(t *testing.T) {
	lines := []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=stdout",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_LOKI_RETRY_CLIENT_MODE=not_a_retry_mode",
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES=-1",
	}

	if _, err := loadConfigErrorFromTempEnv(t, lines); err != nil {
		t.Fatalf("LoadConfig() validated unselected Loki retry settings: %v", err)
	}
}

func retryClientEnvironmentRoutes() []retryClientEnvironmentRoute {
	return []retryClientEnvironmentRoute{
		{
			name: "sync/opensearch", destination: "opensearch",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:     "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_OPENSEARCH_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "sync/loki", destination: "loki",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:     "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "sync/cloudwatchlogs", destination: "cloudwatchlogs",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:     "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_CLOUDWATCH_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "sync/clickhouse", destination: "clickhouse",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:     "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "async/opensearch", destination: "opensearch",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:     "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "async/loki", destination: "loki",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:     "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_ASYNC_LOKI_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "async/cloudwatchlogs", destination: "cloudwatchlogs",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:     "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "async/clickhouse", destination: "clickhouse",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:     "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "delivery/opensearch", destination: "opensearch",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:     "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "delivery/loki", destination: "loki",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:     "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "delivery/cloudwatchlogs", destination: "cloudwatchlogs",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:     "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_RETRIES",
		},
		{
			name: "delivery/clickhouse", destination: "clickhouse",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:     "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:    "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:       "HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE",
			maxRetriesKey: "HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES",
		},
	}
}

func retryClientRouteEnv(route retryClientEnvironmentRoute, handlerOnly bool) []string {
	lines := []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		route.enableKey + "=true",
		route.policyKey + "=unbounded",
	}
	if handlerOnly {
		return append(lines,
			route.logTypeKey+"=stdout",
			route.handlerKey+"="+route.destination,
		)
	}
	return append(lines, route.logTypeKey+"="+route.destination)
}

// mapRetryClientRouteConfigは、routeのloggerの設定をpkg/loggerの設定へ変換し、その検証の
// errorを返します。
func mapRetryClientRouteConfig(route retryClientEnvironmentRoute, cfg AppConfig) error {
	switch {
	case strings.HasPrefix(route.name, "async/"):
		_, err := MapAsyncLoggerConfig(cfg)
		return err
	case strings.HasPrefix(route.name, "delivery/"):
		_, err := MapDeliveryLoggerConfig(cfg)
		return err
	default:
		_, err := MapLoggerConfig(cfg)
		return err
	}
}

// requireNegativeRetryCountErrorは、errがdestinationの再試行の回数を負の値として拒否した
// pkg/loggerのConfigFieldErrorを含むことを確認します。
func requireNegativeRetryCountError(t *testing.T, err error, destination string) {
	t.Helper()
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Backend == pkglogger.LogOutputDestination(destination) &&
			fieldErr.Field == pkglogger.ConfigFieldRetryMaxRetries &&
			errors.Is(fieldErr, pkglogger.ErrNegativeSetting) {
			return
		}
	}
	t.Fatalf("mapped config error = %v, want a negative %s retry count error", err, destination)
}
