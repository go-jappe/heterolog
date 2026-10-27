package clickhouse

import (
	"strings"
)

// ValidateConfig validates the settings required to create a ClickHouse client.
// It does not make HTTP requests. Timeout==0 is the unset sentinel for which
// New applies the default timeout, but a negative value is a configuration
// error. Use errors.Is with ErrNegativeTimeout, ErrEndpointRequired,
// ErrInvalidEndpoint or ErrTableRequired to identify the kind of problem.
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
