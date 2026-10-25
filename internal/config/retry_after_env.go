package config

import (
	"fmt"
	"time"

	pkgenv "github.com/go-jappe/heterolog/pkg/env"
)

// getEnvMaxRetryAfterは、送信先が指定したRetry-Afterによる再送待機の上限を秒で読み込みます。
// 未設定または空ならdefaultValueを返します。明示した0以下の値は無制限や既定値として扱わず、
// 設定errorにします。
func getEnvMaxRetryAfter(env pkgenv.Lookup, key string, defaultValue time.Duration) (time.Duration, error) {
	value, err := env.GetEnvDuration(key, defaultValue)
	if err != nil {
		return value, err
	}
	if value <= 0 {
		raw := fmt.Sprint(int64(value / time.Second))
		return 0, newInvalidEnvironmentValueError(
			key, raw, pkgenv.ErrNotPositive,
			"environment variable %q must be greater than zero: %s",
			key,
			raw,
		)
	}
	return value, nil
}
