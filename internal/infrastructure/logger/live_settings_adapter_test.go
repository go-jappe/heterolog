package logger

import (
	"testing"

	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// TestBackendConfigAdaptersFollowLiveSettingsは、RuntimeConfig.Liveを設定した出力先が、
// 固定のLogLevelとSamplingConfigの代わりにLiveの現在の値を読むことを、全backendで検証します。
// 稼働中の変更は、各backendがlogごとに読むこの値を通じて反映します。
func TestBackendConfigAdaptersFollowLiveSettings(t *testing.T) {
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// 固定の値はLiveと違う値にし、どちらを読んだかを区別します。
	runtime := pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelWarn, Live: live}
	fixedSampling := pkglogger.SamplingConfig{IsSamplingEnabled: true, SamplingDebugRate: 9, SamplingInfoRate: 9, SamplingWarnRate: 9, SamplingErrorRate: 9}
	type accessors struct {
		level    func() pkglogger.LogLevel
		sampling func() sampling.SamplingConfigProvider
	}
	adapters := map[string]accessors{}
	{
		a := stdout.NewConfigAdapter(pkglogger.StdoutLoggerConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["stdout"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}
	{
		a := file.NewConfigAdapter(pkglogger.FileLoggerConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["file"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}
	{
		a := slack.NewConfigAdapter(pkglogger.SlackLoggerConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["slack"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}
	{
		a := opensearchlogger.NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["opensearch"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}
	{
		a := lokilogger.NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["loki"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}
	{
		a := cloudwatchlogslogger.NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["cloudwatchlogs"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}
	{
		a := clickhouselogger.NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{RuntimeConfig: runtime, SamplingConfig: fixedSampling})
		adapters["clickhouse"] = accessors{a.Runtime().LogLevel, a.Sampling}
	}

	for name, a := range adapters {
		if got := a.level(); got != pkglogger.LogLevelInfo {
			t.Errorf("%s level = %v, want INFO from the live settings", name, got)
		}
		if a.sampling().IsSamplingEnabled() {
			t.Errorf("%s sampling is enabled, want the live settings without sampling", name)
		}
	}
	changed := pkglogger.SamplingConfig{IsSamplingEnabled: true, SamplingDebugRate: 2, SamplingInfoRate: 3, SamplingWarnRate: 4, SamplingErrorRate: 5}
	if err := live.Set(pkglogger.LogLevelError, changed); err != nil {
		t.Fatal(err)
	}
	for name, a := range adapters {
		if got := a.level(); got != pkglogger.LogLevelError {
			t.Errorf("%s level = %v after the change, want ERROR", name, got)
		}
		if got := a.sampling(); !got.IsSamplingEnabled() || got.SamplingInfoRate() != 3 || got.SamplingErrorRate() != 5 {
			t.Errorf("%s sampling did not follow the change: enabled=%t info=%d error=%d", name, got.IsSamplingEnabled(), got.SamplingInfoRate(), got.SamplingErrorRate())
		}
	}
}
