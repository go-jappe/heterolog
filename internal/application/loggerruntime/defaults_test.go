package loggerruntime_test

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
)

// 既定のspool設定は、全backendでRetry-Afterの待機上限を1時間にします。
func TestDefaultSpoolConfigCapsRetryAfterAtOneHour(t *testing.T) {
	cfg := loggerruntime.DefaultSpoolConfig()
	for name, backend := range map[string]pkgspoolrunner.BackendSpoolRunnerConfig{
		"opensearch":     cfg.RunnerConfig.OpensearchConfig,
		"loki":           cfg.RunnerConfig.LokiConfig,
		"cloudwatchlogs": cfg.RunnerConfig.CloudWatchLogsConfig,
		"clickhouse":     cfg.RunnerConfig.ClickHouseConfig,
	} {
		if got := backend.Worker.MaxRetryAfter; got != time.Hour {
			t.Errorf("%s MaxRetryAfter = %s, want 1h", name, got)
		}
	}
}
