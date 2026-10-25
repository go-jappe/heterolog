package clickhouse

import (
	"strings"
)

// ValidateConfig はClickHouse clientの生成に必要な設定を検証します。
// HTTP通信は行いません。Timeout==0はNewがdefault timeoutを適用する
// 未指定sentinelですが、負値は設定エラーです。不正の種類はErrNegativeTimeout、
// ErrEndpointRequired、ErrInvalidEndpoint、ErrTableRequiredとerrors.Isで判別できます。
func ValidateConfig(cfg Config) error {
	if cfg.Timeout < 0 {
		return newValidationError(ErrNegativeTimeout, nil, "clickhouse timeout must not be negative: %s", cfg.Timeout)
	}
	if _, err := normalizeEndpoint(cfg.Endpoint); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Table) == "" {
		return ErrTableRequired
	}
	return nil
}
