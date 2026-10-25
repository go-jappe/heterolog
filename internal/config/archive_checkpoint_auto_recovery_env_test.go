package config

import (
	"testing"

	"github.com/go-jappe/heterolog/pkg/env"
)

func TestLoadConfigArchiveCheckpointAutoRecovery(t *testing.T) {
	const key = "HETEROLOG_ENABLE_ARCHIVE_CHECKPOINT_AUTO_RECOVERY"
	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "absent defaults enabled", want: true},
		{name: "empty defaults enabled", line: key + "=", want: true},
		{name: "explicit enabled", line: key + "=true", want: true},
		{name: "explicit disabled", line: key + "=false", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				tt.line,
			})
			if got := cfg.ArchiveCheckpointAutoRecovery; got != tt.want {
				t.Fatalf("ArchiveCheckpointAutoRecovery = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestLoadConfigRejectsInvalidArchiveCheckpointAutoRecovery(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_ENABLE_ARCHIVE_CHECKPOINT_AUTO_RECOVERY=perhaps",
	})
	requireEnvironmentKeyError(t, err, "HETEROLOG_ENABLE_ARCHIVE_CHECKPOINT_AUTO_RECOVERY", env.ErrNotBoolean)
}

func TestPublicEnvExampleEnablesArchiveCheckpointAutoRecovery(t *testing.T) {
	const key = "HETEROLOG_ENABLE_ARCHIVE_CHECKPOINT_AUTO_RECOVERY"
	values := readPublicEnvExample(t)
	got, exists := values[key]
	if !exists {
		t.Fatalf(".env.example is missing %s", key)
	}
	if got != "true" {
		t.Fatalf(".env.example %s = %q, want true", key, got)
	}
}
