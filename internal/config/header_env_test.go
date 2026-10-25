package config

import (
	"testing"

	"github.com/go-jappe/heterolog/pkg/env"
)

// TestLoadConfigRejectsInvalidHeadersWithoutExposingValuesは、HTTP headerのJSON（*_HEADERSと
// HETEROLOG_OTEL_SPAN_HEADERS）が不正な場合に、LoadConfigが拒否したkeyとenv.ErrInvalidJSONを
// errors.Asとerrors.Isで判別できるerrorを返し、値を表示文にも型付きerrorのValueにも、包んだ
// 原因にも残さないことを検証します。headerにはAuthorizationなどの認証情報が含まれ得るためです。
// JSON parserの診断は値の一部（header名など）を含み得るため、header名に秘密を置く場合も確認します。
func TestLoadConfigRejectsInvalidHeadersWithoutExposingValues(t *testing.T) {
	const secret = "header-secret-token-value"
	routes := []struct {
		key   string
		setup []string
	}{
		{key: "HETEROLOG_OTEL_SPAN_HEADERS", setup: []string{"HETEROLOG_ENABLE_OBSERVABILITY=true"}},
	}
	for _, contract := range []struct{ prefix, enableKey string }{
		{"", "HETEROLOG_ENABLE_LOGGING"},
		{"ASYNC_", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING"},
		{"DELIVERY_", "HETEROLOG_ENABLE_DELIVERY_LOGGING"},
	} {
		for _, backend := range []struct{ destination, keyName string }{
			{"loki", "LOKI"},
			{"cloudwatchlogs", "CLOUDWATCH"},
			{"clickhouse", "CLICKHOUSE"},
		} {
			routes = append(routes, struct {
				key   string
				setup []string
			}{
				key:   "HETEROLOG_" + contract.prefix + backend.keyName + "_HEADERS",
				setup: []string{contract.enableKey + "=true", "HETEROLOG_" + contract.prefix + "LOG_TYPE=" + backend.destination},
			})
		}
	}
	for _, route := range routes {
		for _, invalid := range []struct{ name, raw string }{
			{name: "syntax", raw: `{"Authorization":"Bearer ` + secret + `",}`},
			{name: "non-string value", raw: `{"Authorization":"Bearer ` + secret + `","X-Retry":1}`},
			{name: "not an object", raw: `["Bearer ` + secret + `"]`},
			{name: "secret in header name", raw: `{"` + secret + `":1}`},
		} {
			t.Run(route.key+"/"+invalid.name, func(t *testing.T) {
				lines := append(append([]string(nil), route.setup...), route.key+"='"+invalid.raw+"'")
				_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, lines...))
				requireEnvironmentKeyError(t, err, route.key, env.ErrInvalidJSON)
				requireNoValueInErrorTree(t, err, secret)
			})
		}
	}
}
