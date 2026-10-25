package slack

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

func TestSlackLoggerConstructorsUseSharedDestinationValidation(t *testing.T) {
	tests := []struct {
		name      string
		construct func(token, channel string) error
	}{
		{
			name: "normal",
			construct: func(token, channel string) error {
				cfg := NewConfigAdapter(pkglogger.SlackLoggerConfig{
					Token: token, Channel: channel, Timeout: time.Second,
				})
				_, err := NewSlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
		{
			name: "delivery",
			construct: func(token, channel string) error {
				cfg := NewDeliveryConfigAdapter(pkglogger.DeliverySlackLoggerConfig{
					Token: token, Channel: channel, Timeout: time.Second,
				})
				_, err := NewDeliverySlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
		{
			name: "fallback",
			construct: func(token, channel string) error {
				cfg := NewConfigAdapter(pkglogger.SlackLoggerConfig{
					Token: token, Channel: channel, Timeout: time.Second,
				})
				_, err := newHandleSlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
	}

	invalidValues := []struct {
		name    string
		token   string
		channel string
		want    error
	}{
		{
			name:    "whitespace token",
			token:   " \t\n",
			channel: "test-channel",
			want:    pkgslack.ErrTokenRequired,
		},
		{
			name:    "whitespace channel",
			token:   "xoxb-test-token",
			channel: " \t\n",
			want:    pkgslack.ErrChannelRequired,
		},
	}

	for _, constructor := range tests {
		t.Run(constructor.name, func(t *testing.T) {
			for _, invalid := range invalidValues {
				t.Run(invalid.name, func(t *testing.T) {
					err := constructor.construct(invalid.token, invalid.channel)
					if !errors.Is(err, invalid.want) {
						t.Fatalf("constructor error = %v, want %v", err, invalid.want)
					}
				})
			}
		})
	}
}

func TestSlackLoggerConstructorsRejectNonPositiveTimeout(t *testing.T) {
	constructors := map[string]func(time.Duration, *http.Client) error{
		"normal": func(timeout time.Duration, httpClient *http.Client) error {
			cfg := NewConfigAdapter(pkglogger.SlackLoggerConfig{
				Token: "xoxb-test-token", Channel: "test-channel",
				Timeout: timeout, HTTPClient: httpClient, MaxLogsPerNotification: 1,
			})
			_, err := NewSlackLogger(context.Background(), cfg, observer.NopTracer{}, observer.NopMeter{})
			return err
		},
		"delivery": func(timeout time.Duration, httpClient *http.Client) error {
			cfg := NewDeliveryConfigAdapter(pkglogger.DeliverySlackLoggerConfig{
				Token: "xoxb-test-token", Channel: "test-channel", Timeout: timeout, HTTPClient: httpClient,
			})
			_, err := NewDeliverySlackLogger(context.Background(), cfg, observer.NopTracer{}, observer.NopMeter{})
			return err
		},
		"fallback": func(timeout time.Duration, httpClient *http.Client) error {
			cfg := NewConfigAdapter(pkglogger.SlackLoggerConfig{
				Token: "xoxb-test-token", Channel: "test-channel",
				Timeout: timeout, HTTPClient: httpClient, MaxLogsPerNotification: 1,
			})
			_, err := newHandleSlackLogger(context.Background(), cfg, observer.NopTracer{}, observer.NopMeter{})
			return err
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			// HTTPClientのTimeoutは、必須である設定のTimeoutの代わりになりません。
			for _, httpClient := range []*http.Client{nil, {Timeout: time.Second}} {
				for _, timeout := range []time.Duration{0, -time.Second} {
					err := construct(timeout, httpClient)
					if !errors.Is(err, ErrNonPositiveTimeout) {
						t.Fatalf("timeout=%s HTTPClient=%v constructor error = %v, want %v", timeout, httpClient != nil, err, ErrNonPositiveTimeout)
					}
				}
			}
		})
	}
}

func TestSlackNormalAndFallbackConstructorsRejectInvalidSamplingRate(t *testing.T) {
	cfg := NewConfigAdapter(pkglogger.SlackLoggerConfig{
		Token:                  "xoxb-test-token",
		Channel:                "test-channel",
		Timeout:                time.Second,
		Intervals:              []time.Duration{time.Hour},
		MaxLogsPerNotification: 1,
		MaxRestThreshold:       time.Hour,
		SamplingConfig: pkglogger.SamplingConfig{
			IsSamplingEnabled:      true,
			SamplingDebugRate:      0,
			SamplingInfoRate:       1,
			SamplingWarnRate:       1,
			SamplingErrorRate:      1,
			LoggingByRequestIDRate: 1,
		},
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewSlackLogger(context.Background(), cfg, observer.NopTracer{}, observer.NopMeter{})
			return err
		},
		"fallback": func() error {
			_, err := newHandleSlackLogger(context.Background(), cfg, observer.NopTracer{}, observer.NopMeter{})
			return err
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			err := construct()
			if !errors.Is(err, pkglogger.ErrInvalidSamplingRate) {
				t.Fatalf("constructor error = %v, want %v", err, pkglogger.ErrInvalidSamplingRate)
			}
			fieldErrs := pkglogger.ConfigFieldErrors(err)
			if len(fieldErrs) != 1 || fieldErrs[0].Backend != pkglogger.LogOutputSlack ||
				fieldErrs[0].Field != pkglogger.ConfigFieldSamplingDebugRate {
				t.Fatalf("config field errors = %+v, want %s %s", fieldErrs, pkglogger.LogOutputSlack, pkglogger.ConfigFieldSamplingDebugRate)
			}
		})
	}
}

func TestSlackBufferedLoggerConstructorsRejectNonPositiveMaxLogsPerNotification(t *testing.T) {
	tests := []struct {
		name      string
		construct func(SlackLoggerConfigProvider) error
	}{
		{
			name: "normal",
			construct: func(cfg SlackLoggerConfigProvider) error {
				_, err := NewSlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
		{
			name: "fallback",
			construct: func(cfg SlackLoggerConfigProvider) error {
				_, err := newHandleSlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
	}

	for _, constructor := range tests {
		t.Run(constructor.name, func(t *testing.T) {
			for _, maxLogs := range []int{-1, 0} {
				cfg := NewConfigAdapter(pkglogger.SlackLoggerConfig{
					Token:                  "xoxb-test-token",
					Channel:                "test-channel",
					Timeout:                time.Second,
					MaxLogsPerNotification: maxLogs,
				})
				err := constructor.construct(cfg)
				requireSlackConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldMaxLogsPerNotification)
			}
		})
	}
}

func TestSlackBufferedLoggerConstructorsRejectInvalidIntervalsAndRestThreshold(t *testing.T) {
	constructors := []struct {
		name      string
		construct func(SlackLoggerConfigProvider) error
	}{
		{
			name: "normal",
			construct: func(cfg SlackLoggerConfigProvider) error {
				_, err := NewSlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
		{
			name: "fallback",
			construct: func(cfg SlackLoggerConfigProvider) error {
				_, err := newHandleSlackLogger(
					context.Background(),
					cfg,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
	}
	// 不正な待機時間は、SlackIntervalErrorのIndexでIntervalsの中の位置を特定します。
	// wantIndexが-1のcaseは、要素の位置を持たない不正（空の一覧・休止時間）です。
	tests := []struct {
		name      string
		intervals []time.Duration
		threshold time.Duration
		wantKind  error
		wantField pkglogger.ConfigField
		wantIndex int
	}{
		{
			name:      "empty intervals",
			intervals: nil,
			threshold: time.Hour,
			wantKind:  pkglogger.ErrEmptySetting,
			wantField: pkglogger.ConfigFieldIntervals,
			wantIndex: -1,
		},
		{
			name:      "negative interval",
			intervals: []time.Duration{-time.Second},
			threshold: time.Hour,
			wantKind:  pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldIntervals,
			wantIndex: 0,
		},
		{
			name:      "zero first interval",
			intervals: []time.Duration{0, time.Minute},
			threshold: time.Hour,
			wantKind:  pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldIntervals,
			wantIndex: 0,
		},
		{
			name:      "zero later interval",
			intervals: []time.Duration{time.Minute, 0},
			threshold: time.Hour,
			wantKind:  pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldIntervals,
			wantIndex: 1,
		},
		{
			name:      "zero max rest threshold",
			intervals: []time.Duration{time.Minute},
			threshold: 0,
			wantKind:  pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldMaxRestThreshold,
			wantIndex: -1,
		},
		{
			name:      "negative max rest threshold",
			intervals: []time.Duration{time.Minute},
			threshold: -time.Second,
			wantKind:  pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldMaxRestThreshold,
			wantIndex: -1,
		},
	}

	for _, constructor := range constructors {
		t.Run(constructor.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					cfg := fallbackSlackTestConfig(tt.intervals)
					cfg.MaxRestThreshold = tt.threshold

					err := constructor.construct(NewConfigAdapter(cfg))
					requireSlackConfigFieldError(t, err, tt.wantKind, tt.wantField)
					var intervalErr *pkglogger.SlackIntervalError
					if found := errors.As(err, &intervalErr); found != (tt.wantIndex >= 0) ||
						(found && intervalErr.Index != tt.wantIndex) {
						t.Fatalf("constructor error = %#v, want the invalid interval at index %d", err, tt.wantIndex)
					}
				})
			}
		})
	}
}

func TestSlackBufferedLoggerConstructorsAcceptPositiveIntervals(t *testing.T) {
	cfg := fallbackSlackTestConfig([]time.Duration{time.Minute, 5 * time.Minute})
	adapter := NewConfigAdapter(cfg)

	normal, err := NewSlackLogger(
		context.Background(),
		adapter,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewSlackLogger() error = %v, want nil", err)
	}
	if err := normal.Close(); err != nil {
		t.Fatalf("SlackLogger.Close() error = %v", err)
	}

	fallback, err := newHandleSlackLogger(
		context.Background(),
		adapter,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("newHandleSlackLogger() error = %v, want nil", err)
	}
	if err := fallback.Close(); err != nil {
		t.Fatalf("FallbackLogger.Close() error = %v", err)
	}
}

// requireSlackConfigFieldErrorは、errがSlack loggerの一つの設定項目の不正を表し、不正の種類と
// 項目を保持することを検証します。入力値はmessageへ表示されるだけなので検証しません。
func requireSlackConfigFieldError(
	t *testing.T,
	err error,
	kind error,
	field pkglogger.ConfigField,
) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("constructor error = %v, want %v", err, kind)
	}
	fieldErrs := pkglogger.ConfigFieldErrors(err)
	if len(fieldErrs) != 1 || fieldErrs[0].Field != field || !errors.Is(fieldErrs[0], kind) {
		t.Fatalf("config field errors = %+v, want one %v of %s", fieldErrs, kind, field)
	}
}
