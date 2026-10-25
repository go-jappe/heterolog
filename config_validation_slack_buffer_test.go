package heterolog_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
)

// slackBufferNoNetworkTransportは、試験のSlack loggerがSlack APIへ接続しないよう、すべての
// requestを送らずに失敗させます。
type slackBufferNoNetworkTransport struct{}

var errSlackBufferTestNoNetwork = errors.New("slack buffer validation test: network disabled")

func (slackBufferNoNetworkTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errSlackBufferTestNoNetwork
}

// slackBufferTestConfigは、通知の待機時間と休止時間だけを指定した、それ以外は有効な
// Slack logger設定です。
func slackBufferTestConfig(intervals []time.Duration, maxRestThreshold time.Duration) *pkglogger.SlackLoggerConfig {
	return &pkglogger.SlackLoggerConfig{
		TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
		RuntimeConfig: pkglogger.RuntimeConfig{AppName: "slack-buffer-validation", LogLevel: pkglogger.LogLevelInfo},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Timeout:                         time.Second,
		Token:                           "dummy-token",
		Channel:                         "logs",
		MaxLogsPerNotification:          10,
		Intervals:                       intervals,
		MaxRestThreshold:                maxRestThreshold,
		MaxMessageLength:                10_000,
		SlackPostWarnThresholdDuration:  time.Hour,
		SlackPostErrorThresholdDuration: time.Hour,
		HTTPClient:                      &http.Client{Transport: slackBufferNoNetworkTransport{}},
	}
}

// slackBufferConstructorは、Slackのbuffer設定を一つの経路へ置いて公開constructorを呼びます。
// sourceは、その経路の不正としてConfigFieldErrorが報告するSourceです。
type slackBufferConstructor struct {
	name      string
	source    string
	construct func(intervals []time.Duration, maxRestThreshold time.Duration) (interface{ Close() error }, error)
}

func slackBufferConstructors() []slackBufferConstructor {
	ctx := context.Background()
	slackDestinations := pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout, pkglogger.LogOutputSlack}
	return []slackBufferConstructor{
		{
			name:   "New output",
			source: "logger",
			construct: func(intervals []time.Duration, rest time.Duration) (interface{ Close() error }, error) {
				cfg := heterolog.DefaultConfig()
				cfg.LogOutputDestinations = slackDestinations
				cfg.Slack = slackBufferTestConfig(intervals, rest)
				return heterolog.New(ctx, cfg)
			},
		},
		{
			name:   "New internal log destination",
			source: "logger internal log destination",
			construct: func(intervals []time.Duration, rest time.Duration) (interface{ Close() error }, error) {
				cfg := heterolog.DefaultConfig()
				cfg.Stdout.HandlerConfig = handler.HandlerConfig{OnErrorConfig: []string{"slack"}}
				cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{
					Slack: slackBufferTestConfig(intervals, rest),
				}
				return heterolog.New(ctx, cfg)
			},
		},
		{
			name:   "NewAsync output",
			source: "async logger",
			construct: func(intervals []time.Duration, rest time.Duration) (interface{ Close() error }, error) {
				cfg := heterolog.DefaultAsyncConfig()
				cfg.LogOutputDestinations = slackDestinations
				cfg.Slack = slackBufferTestConfig(intervals, rest)
				return heterolog.NewAsync(ctx, cfg)
			},
		},
		{
			name:   "NewDelivery internal log fallback",
			source: "delivery logger internal log fallback",
			construct: func(intervals []time.Duration, rest time.Duration) (interface{ Close() error }, error) {
				cfg := heterolog.DefaultDeliveryConfig()
				cfg.Stdout.HandlerConfig = handler.HandlerConfig{OnErrorConfig: []string{"slack"}}
				cfg.Slack = &pkglogger.DeliverySlackLoggerConfig{
					TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
					RuntimeConfig: pkglogger.RuntimeConfig{AppName: "slack-buffer-validation", LogLevel: pkglogger.LogLevelInfo},
					LogBodyConfig: pkglogger.LogBodyConfig{
						LogLineDateFormat:        time.RFC3339Nano,
						ErrorTraceDepth:          1,
						ErrorTraceDepthDebugMode: 1,
					},
					Timeout:                         time.Second,
					Token:                           "dummy-token",
					Channel:                         "logs",
					MaxMessageLength:                10_000,
					SlackPostWarnThresholdDuration:  time.Hour,
					SlackPostErrorThresholdDuration: time.Hour,
					HTTPClient:                      &http.Client{Transport: slackBufferNoNetworkTransport{}},
				}
				cfg.InternalLogBatchConfig.Slack = pkglogger.DeliveryInternalLogSlackBatchConfig{
					Intervals:              intervals,
					MaxLogsPerNotification: 10,
					MaxRestThreshold:       rest,
				}
				return heterolog.NewDelivery(ctx, cfg)
			},
		},
	}
}

// Slack loggerの通知の待機時間（Intervals）と休止時間（MaxRestThreshold）の不正は、他のbackend
// 設定と同じく構築前の設定検証で、Source（設定の出所）とBackend（slack）を持つConfigFieldErrorと
// して報告します。不正な待機時間の位置はSlackIntervalErrorのIndexで特定できます。
func TestPublicConstructorsReportSlackBufferSettingsWithSourceAndBackend(t *testing.T) {
	tests := []struct {
		name             string
		intervals        []time.Duration
		maxRestThreshold time.Duration
		wantField        pkglogger.ConfigField
		wantKind         error
		// wantIndexは、SlackIntervalErrorが示す不正な待機時間の位置です。-1は位置を持たない不正です。
		wantIndex int
	}{
		{
			name:             "empty intervals",
			maxRestThreshold: time.Hour,
			wantField:        pkglogger.ConfigFieldIntervals,
			wantKind:         pkglogger.ErrEmptySetting,
			wantIndex:        -1,
		},
		{
			name:             "non-positive later interval",
			intervals:        []time.Duration{time.Minute, 2 * time.Minute, 0},
			maxRestThreshold: time.Hour,
			wantField:        pkglogger.ConfigFieldIntervals,
			wantKind:         pkglogger.ErrNonPositiveSetting,
			wantIndex:        2,
		},
		{
			name:             "non-positive max rest threshold",
			intervals:        []time.Duration{time.Minute},
			maxRestThreshold: -time.Second,
			wantField:        pkglogger.ConfigFieldMaxRestThreshold,
			wantKind:         pkglogger.ErrNonPositiveSetting,
			wantIndex:        -1,
		},
	}
	for _, constructor := range slackBufferConstructors() {
		for _, tt := range tests {
			t.Run(constructor.name+"/"+tt.name, func(t *testing.T) {
				created, err := constructor.construct(tt.intervals, tt.maxRestThreshold)
				if err == nil {
					_ = created.Close()
					t.Fatalf("constructor accepted invalid Slack %s", tt.wantField)
				}
				var found *pkglogger.ConfigFieldError
				for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
					if fieldErr.Source == constructor.source && fieldErr.Backend == pkglogger.LogOutputSlack &&
						fieldErr.Field == tt.wantField && errors.Is(fieldErr, tt.wantKind) {
						found = fieldErr
						break
					}
				}
				if found == nil {
					t.Fatalf("constructor error = %v, want %v of %s %s %s", err, tt.wantKind, constructor.source, pkglogger.LogOutputSlack, tt.wantField)
				}
				var intervalErr *pkglogger.SlackIntervalError
				if hasIndex := errors.As(found, &intervalErr); hasIndex != (tt.wantIndex >= 0) ||
					(hasIndex && intervalErr.Index != tt.wantIndex) {
					t.Fatalf("Slack interval position in %#v = %#v, want index %d", found, intervalErr, tt.wantIndex)
				}
			})
		}
	}
}

// 有効なSlackのbuffer設定は、どの経路でも構築前の設定検証を通ります。
func TestPublicConstructorsAcceptValidSlackBufferSettings(t *testing.T) {
	for _, constructor := range slackBufferConstructors() {
		t.Run(constructor.name, func(t *testing.T) {
			created, err := constructor.construct([]time.Duration{time.Minute, time.Hour}, time.Hour)
			if err != nil {
				t.Fatalf("constructor error = %v", err)
			}
			if err := created.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}
}
