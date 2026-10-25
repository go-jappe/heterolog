package mappedvalidation

import (
	"errors"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestValidateMappedAppendLogRunnerConfigRequiresSupervisorForStopOnError(t *testing.T) {
	err := validateMappedAppendLogRunnerConfig(
		"logger file",
		pkgfsstore.FileLifecycleModeRunner,
		pkgfsstore.AppendLogRunnerConfig{
			Interval:    time.Second,
			StopOnError: true,
		},
	)
	if !errors.Is(err, ErrAppendLogRunnerStopOnErrorRequiresSupervisor) {
		t.Fatalf(
			"validateMappedAppendLogRunnerConfig() error = %v, want %v",
			err,
			ErrAppendLogRunnerStopOnErrorRequiresSupervisor,
		)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) ||
		configErr.Source != "logger file" ||
		configErr.Section != ConfigSectionAppendLogRunner {
		t.Fatalf("validateMappedAppendLogRunnerConfig() error = %#v, want logger file appendlog runner ConfigError", err)
	}
}
