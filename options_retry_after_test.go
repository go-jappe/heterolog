package heterolog_test

import (
	"testing"
	"time"

	heterolog "github.com/go-jappe/heterolog"
)

// 公開の既定spool設定は、全backendでRetry-Afterの待機上限を1時間にする。
func TestDefaultSpoolConfigCapsRetryAfterAtOneHour(t *testing.T) {
	cfg := heterolog.DefaultSpoolConfig()
	for name, backend := range map[string]heterolog.BackendSpoolRunnerConfig{
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
