package config

import (
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
)

func TestDurationLoadersPreserveTypedDefaultsAndExplicitUnits(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue time.Duration
		explicit     string
		explicitWant time.Duration
		load         func() (time.Duration, error)
	}{
		{
			name:         "slack max rest threshold minutes",
			key:          "HETEROLOG_MAX_SLACK_REST_THRESHOLD_MINUTE",
			defaultValue: logconfig.DefaultMaxRestThreshold,
			explicit:     "7",
			explicitWant: 7 * time.Minute,
			load: func() (time.Duration, error) {
				cfg, err := loadSlackConfig(processEnvironmentForTest())
				return cfg.MaxRestThreshold, err
			},
		},
		{
			name:         "async slack max rest threshold minutes",
			key:          "HETEROLOG_MAX_ASYNC_SLACK_REST_THRESHOLD_MINUTE",
			defaultValue: logconfig.DefaultMaxRestThreshold,
			explicit:     "11",
			explicitWant: 11 * time.Minute,
			load: func() (time.Duration, error) {
				cfg, err := loadAsyncSlackConfig(processEnvironmentForTest())
				return cfg.MaxRestThreshold, err
			},
		},
		{
			name:         "opensearch flush interval seconds",
			key:          "HETEROLOG_OPENSEARCH_FLUSH_INTERVALS",
			defaultValue: logconfig.DefaultOpenSearchFlushInterval,
			explicit:     "7",
			explicitWant: 7 * time.Second,
			load: func() (time.Duration, error) {
				cfg, err := loadOpenSearchConfig(processEnvironmentForTest())
				return cfg.FlushInterval, err
			},
		},
		{
			name:         "async opensearch flush interval seconds",
			key:          "HETEROLOG_ASYNC_OPENSEARCH_FLUSH_INTERVALS",
			defaultValue: logconfig.DefaultOpenSearchFlushInterval,
			explicit:     "11",
			explicitWant: 11 * time.Second,
			load: func() (time.Duration, error) {
				cfg, err := loadAsyncOpenSearchConfig(processEnvironmentForTest())
				return cfg.FlushInterval, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, value := range []struct {
				name    string
				value   string
				missing bool
				want    time.Duration
			}{
				{
					name:    "missing",
					missing: true,
					want:    tt.defaultValue,
				},
				{
					name: "empty",
					want: tt.defaultValue,
				},
				{
					name:  "explicit",
					value: tt.explicit,
					want:  tt.explicitWant,
				},
			} {
				t.Run(value.name, func(t *testing.T) {
					t.Setenv(tt.key, value.value)
					if value.missing {
						if err := os.Unsetenv(tt.key); err != nil {
							t.Fatalf("os.Unsetenv(%q): %v", tt.key, err)
						}
					}

					got, err := tt.load()
					if got != value.want {
						t.Fatalf("loaded duration = %s, want %s", got, value.want)
					}
					if err != nil {
						t.Fatalf("environment errors = %v, want nil", err)
					}
				})
			}
		})
	}
}
