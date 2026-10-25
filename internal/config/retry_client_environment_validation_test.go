package config

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/env"
)

type retryClientEnvironmentRoute struct {
	name           string
	destination    string
	enableKey      string
	logTypeKey     string
	policyKey      string
	handlerKey     string
	modeKey        string
	maxAttemptsKey string
}

// TestLoadConfigRejectsNegativeRetryValueForEveryLoggerRouteは、全logger経路で
// retry数値の明示的な負数がdefaultへ置き換えられず、起動時エラーになることを
// 検証します。
func TestLoadConfigRejectsNegativeRetryValueForEveryLoggerRoute(t *testing.T) {
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
					route.maxAttemptsKey+"=-1",
				)

				_, err := loadConfigErrorFromTempEnv(t, lines)
				if err == nil {
					t.Fatal("LoadConfig() error = nil, want negative retry value error")
				}
				requireEnvironmentKeyError(t, err, route.maxAttemptsKey, env.ErrNotPositive)
			})
		}
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
// かかわらず、.envへ明示したretry数値が正の整数でなければエラーになることを検証します。
// 未使用値を許容するtyped configの契約とは異なり、environment境界では設定ミスを
// 握り潰しません。
func TestLoadConfigRejectsExplicitInvalidRetryNumericValues(t *testing.T) {
	tests := []struct {
		name  string
		mode  string
		key   string
		value string
		kind  error
	}{
		{
			name:  "malformed max attempts",
			mode:  "retry_policy",
			key:   "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEMPTS",
			value: "three",
			kind:  env.ErrNotInteger,
		},
		{
			name:  "zero max attempts",
			mode:  "retry",
			key:   "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEMPTS",
			value: "0",
			kind:  env.ErrNotPositive,
		},
		{
			name:  "negative base delay",
			mode:  "retry_with_breaker",
			key:   "HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND",
			value: "-1",
			kind:  env.ErrNotPositive,
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
		{
			name:  "unused zero value in none mode",
			mode:  "none",
			key:   "HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND",
			value: "0",
			kind:  env.ErrNotPositive,
		},
		{
			name:  "unused negative value in circuit breaker mode",
			mode:  "circuit_breaker",
			key:   "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND",
			value: "-1",
			kind:  env.ErrNotPositive,
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
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEMPTS=",
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
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEMPTS=-1",
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
			policyKey:      "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_OPENSEARCH_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "sync/loki", destination: "loki",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:      "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "sync/cloudwatchlogs", destination: "cloudwatchlogs",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:      "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_CLOUDWATCH_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "sync/clickhouse", destination: "clickhouse",
			enableKey: "HETEROLOG_ENABLE_LOGGING", logTypeKey: "HETEROLOG_LOG_TYPE",
			policyKey:      "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "async/opensearch", destination: "opensearch",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:      "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "async/loki", destination: "loki",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:      "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_ASYNC_LOKI_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "async/cloudwatchlogs", destination: "cloudwatchlogs",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:      "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "async/clickhouse", destination: "clickhouse",
			enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", logTypeKey: "HETEROLOG_ASYNC_LOG_TYPE",
			policyKey:      "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "delivery/opensearch", destination: "opensearch",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:      "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "delivery/loki", destination: "loki",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:      "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "delivery/cloudwatchlogs", destination: "cloudwatchlogs",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:      "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_ATTEMPTS",
		},
		{
			name: "delivery/clickhouse", destination: "clickhouse",
			enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING", logTypeKey: "HETEROLOG_DELIVERY_LOG_TYPE",
			policyKey:      "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
			handlerKey:     "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			modeKey:        "HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE",
			maxAttemptsKey: "HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS",
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
