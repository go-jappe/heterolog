package loki

// ValidateConfig はLoki clientの生成に必要な設定を検証します。
// HTTP通信や認証処理は行いません。不正の種類はErrNegativeTimeout、ErrConflictingAuth、
// ErrInvalidTenantID、ErrEndpointRequired、ErrInvalidEndpointとerrors.Isで判別できます。
func ValidateConfig(cfg Config) error {
	if cfg.Timeout < 0 {
		return newValidationError(ErrNegativeTimeout, nil, "loki timeout must not be negative: %s", cfg.Timeout)
	}
	if cfg.BasicAuth != nil && cfg.BearerToken != "" {
		return newValidationError(ErrConflictingAuth, nil, "basic auth and bearer token cannot be used together")
	}
	if err := validateTenantID(cfg.TenantID); err != nil {
		return err
	}
	_, err := normalizePushURL(cfg.Endpoint)
	return err
}
