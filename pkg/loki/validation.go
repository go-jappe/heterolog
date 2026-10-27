package loki

// ValidateConfig validates the settings required to create a Loki client. It
// does not make HTTP requests or perform authentication. Use errors.Is with
// ErrNegativeTimeout, ErrConflictingAuth, ErrInvalidTenantID,
// ErrEndpointRequired or ErrInvalidEndpoint to identify the kind of problem.
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
