package config

import (
	"testing"

	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLoadLogLevelRecordsUnknownValueAsConfigurationError(t *testing.T) {
	t.Setenv("TEST_LOG_LEVEL", "verbose")

	got, err := loadLogLevel(env.GetEnvString("TEST_LOG_LEVEL", pkglogger.LogLevelInfo.String()), "TEST_LOG_LEVEL", pkglogger.LogLevelInfo)
	if got != pkglogger.LogLevelInfo {
		t.Fatalf("loadLogLevel() = %q, want fallback value %q", got, pkglogger.LogLevelInfo)
	}
	requireEnvironmentKeyError(t, err, "TEST_LOG_LEVEL", nil)
	if valueErr := environmentKeyError(err, "TEST_LOG_LEVEL").(*InvalidEnvironmentValueError); valueErr.Value != "verbose" {
		t.Fatalf("TEST_LOG_LEVEL error value = %q, want verbose", valueErr.Value)
	}
}
