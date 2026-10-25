package slack

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"

	"github.com/stretchr/testify/require"
)

func TestConfigAdapterOwnsNotificationIntervals(t *testing.T) {
	t.Parallel()
	cfg := pkglogger.SlackLoggerConfig{Intervals: []time.Duration{time.Second, time.Minute}}
	adapter := NewConfigAdapter(cfg)
	cfg.Intervals[0] = 0
	intervals := adapter.Intervals()
	require.Equal(t, []time.Duration{time.Second, time.Minute}, intervals)
	intervals[1] = 0
	require.Equal(t, []time.Duration{time.Second, time.Minute}, adapter.Intervals())
	// コピーによって未指定が有効な設定へ変わることはありません。
	require.Nil(t, NewConfigAdapter(pkglogger.SlackLoggerConfig{}).Intervals())
}

// adapterは構築時点のHTTP client設定をcopyして所有します。呼び出し元のclientや戻り値を
// 後から変更しても、遅延生成するfallback loggerを含む以後のSlack API clientへ反映しません。
func TestConfigAdaptersOwnConfiguredHTTPClient(t *testing.T) {
	t.Parallel()
	type httpClientProvider interface{ HTTPClient() *http.Client }
	for _, test := range []struct {
		name  string
		adapt func(*http.Client) httpClientProvider
	}{
		{
			name: "buffered",
			adapt: func(client *http.Client) httpClientProvider {
				return NewConfigAdapter(pkglogger.SlackLoggerConfig{HTTPClient: client})
			},
		},
		{
			name: "delivery",
			adapt: func(client *http.Client) httpClientProvider {
				return NewDeliveryConfigAdapter(pkglogger.DeliverySlackLoggerConfig{HTTPClient: client})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			configured := &slackAPITransport{}
			replacement := &slackAPITransport{}
			original := &http.Client{Transport: configured}
			adapter := test.adapt(original)

			original.Transport = replacement
			got := adapter.HTTPClient()
			require.NotSame(t, original, got)
			require.Same(t, configured, got.Transport)

			got.Transport = replacement
			require.Same(t, configured, adapter.HTTPClient().Transport)

			// 未指定はnet/httpの既定transportを使う指定として、nilのまま保持します。
			require.Nil(t, test.adapt(nil).HTTPClient())
		})
	}
}

func TestDeliveryFallbackConfigAdapterPreservesSettings(t *testing.T) {
	t.Parallel()
	cfg := deliveryFallbackSlackTestConfig()
	cfg.Timeout = 1750 * time.Millisecond
	cfg.RuntimeConfig.LogLevel = pkglogger.LogLevelWarn
	cfg.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"stdout"}}
	cfg.SlackPostWarnThresholdDuration = 3 * time.Second
	cfg.SlackPostErrorThresholdDuration = 5 * time.Second
	transport := &slackAPITransport{}
	cfg.HTTPClient = &http.Client{Transport: transport}
	batchCfg := pkglogger.DeliveryInternalLogSlackBatchConfig{
		Intervals:              []time.Duration{2 * time.Second, 7 * time.Second},
		MaxLogsPerNotification: 13,
		MaxRestThreshold:       11 * time.Minute,
	}
	adapter := NewDeliveryFallbackConfigAdapter(cfg, batchCfg)
	require.Equal(t, cfg.TimeConfig.TimeZone, adapter.Time().TimeZone())
	require.Equal(t, cfg.RuntimeConfig.AppName, adapter.Runtime().AppName())
	require.Equal(t, cfg.RuntimeConfig.LogLevel, adapter.Runtime().LogLevel())
	require.Equal(t, cfg.LogBodyConfig.LogLineDateFormat, adapter.LogBody().LogLineDateFormat())
	require.Equal(t, cfg.Timeout, adapter.Timeout())
	require.Same(t, transport, adapter.HTTPClient().Transport)
	require.Equal(t, cfg.Token, adapter.Token())
	require.Equal(t, cfg.Channel, adapter.Channel())
	require.Equal(t, cfg.ErrorChannel, adapter.ErrorChannel())
	require.Equal(t, cfg.WarnChannel, adapter.WarnChannel())
	require.Equal(t, cfg.InfoChannel, adapter.InfoChannel())
	require.Equal(t, cfg.DebugChannel, adapter.DebugChannel())
	require.Equal(t, cfg.MaxMessageLength, adapter.MaxMessageLength())
	require.Equal(t, cfg.SlackPostWarnThresholdDuration, adapter.SlackPostWarnThresholdDuration())
	require.Equal(t, cfg.SlackPostErrorThresholdDuration, adapter.SlackPostErrorThresholdDuration())
	require.Equal(t, cfg.HandlerConfig.OnErrorConfig, adapter.Handler().OnError())
	require.False(t, adapter.Sampling().IsSamplingEnabled())
	require.False(t, adapter.Sampling().EnableLoggingByRequestID())
	require.Equal(t, batchCfg.MaxLogsPerNotification, adapter.MaxLogsPerNotification())
	require.Equal(t, batchCfg.MaxRestThreshold, adapter.MaxRestThreshold())
	batchCfg.Intervals[0] = time.Hour
	intervals := adapter.Intervals()
	require.Equal(t, []time.Duration{2 * time.Second, 7 * time.Second}, intervals)
	intervals[1] = time.Hour
	require.Equal(t, []time.Duration{2 * time.Second, 7 * time.Second}, adapter.Intervals())

	// 通知先設定を保持していてもfallback自身は別のhandlerへ再通知しません。
	fallback, err := newHandleSlackLogger(context.Background(), adapter, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fallback.Close()) })
	require.Nil(t, fallback.OnError)
	require.Nil(t, fallback.OnWarn)
	require.Nil(t, fallback.OnInfo)
	require.Nil(t, fallback.OnDebug)
}
