package config

import (
	"errors"
	"math"
	"testing"

	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
)

func TestValidateApplicationConfigRejectsInvalidRuntimeMemoryHealthConfig(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*configruntime.MemoryHealthConfig)
		want      error
	}{
		{
			name: "zero critical ratio",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.CriticalRatio = 0
			},
			want: ErrInvalidMemoryCriticalRatio,
		},
		{
			name: "negative critical ratio",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.CriticalRatio = -0.1
			},
			want: ErrInvalidMemoryCriticalRatio,
		},
		{
			name: "critical ratio above one",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.CriticalRatio = 1.01
			},
			want: ErrInvalidMemoryCriticalRatio,
		},
		{
			name: "NaN critical ratio",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.CriticalRatio = math.NaN()
			},
			want: ErrInvalidMemoryCriticalRatio,
		},
		{
			name: "zero minimum available bytes",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.MinAvailableBytes = 0
			},
			want: ErrInvalidMemoryMinAvailableBytes,
		},
		{
			name: "zero critical consecutive hits",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.CriticalConsecutiveHits = 0
			},
			want: ErrInvalidMemoryCriticalConsecutiveHits,
		},
		{
			name: "negative critical consecutive hits",
			configure: func(cfg *configruntime.MemoryHealthConfig) {
				cfg.CriticalConsecutiveHits = -1
			},
			want: ErrInvalidMemoryCriticalConsecutiveHits,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validRuntimeMemoryHealthApplicationConfig()
			tt.configure(&cfg.RuntimeConfig.Health.Memory)

			err := ValidateApplicationConfig(cfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("ValidateApplicationConfig() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestValidateApplicationConfigAcceptsRuntimeMemoryHealthBoundaryValues(t *testing.T) {
	cfg := validRuntimeMemoryHealthApplicationConfig()
	cfg.RuntimeConfig.Health.Memory = configruntime.MemoryHealthConfig{
		CriticalRatio:           1,
		MinAvailableBytes:       1,
		CriticalConsecutiveHits: 1,
	}

	if err := ValidateApplicationConfig(cfg); err != nil {
		t.Fatalf("ValidateApplicationConfig() error = %v, want nil", err)
	}
}

func TestValidateApplicationConfigAggregatesRuntimeMemoryHealthErrors(t *testing.T) {
	cfg := validRuntimeMemoryHealthApplicationConfig()
	cfg.RuntimeConfig.Health.Memory = configruntime.MemoryHealthConfig{}

	err := ValidateApplicationConfig(cfg)
	for _, want := range []error{
		ErrInvalidMemoryCriticalRatio,
		ErrInvalidMemoryMinAvailableBytes,
		ErrInvalidMemoryCriticalConsecutiveHits,
	} {
		if !errors.Is(err, want) {
			t.Fatalf("ValidateApplicationConfig() error = %v, want %v", err, want)
		}
	}
}

func validRuntimeMemoryHealthApplicationConfig() AppConfig {
	var cfg AppConfig
	cfg.RuntimeConfig.Health.Memory = configruntime.MemoryHealthConfig{
		CriticalRatio:           0.9,
		MinAvailableBytes:       1,
		CriticalConsecutiveHits: 1,
	}
	return cfg
}
