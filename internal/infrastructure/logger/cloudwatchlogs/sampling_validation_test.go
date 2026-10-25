package cloudwatchlogs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchLogsNormalAndFallbackConstructorsRejectInvalidSamplingRate(t *testing.T) {
	cfg := NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		FlushInterval:          time.Second,
		BufferFlushMaxEvents:   1,
		BufferFlushBytes:       1,
		FallbackQueueMaxEvents: 1,
		SamplingConfig:         invalidCloudWatchLogsSamplingConfig(),
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewCloudWatchLogsLogger(
				context.Background(), cfg, nil, nil, nil, nil,
				observer.NopTracer{}, observer.NopMeter{}, nil,
			)
			return err
		},
		"fallback": func() error {
			_, err := newFallbackLogger(
				context.Background(), cfg, nil, nil, nil, nil,
				observer.NopTracer{}, observer.NopMeter{},
			)
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
			if len(fieldErrs) != 1 || fieldErrs[0].Backend != pkglogger.LogOutputCloudWatchLogs ||
				fieldErrs[0].Field != pkglogger.ConfigFieldSamplingDebugRate {
				t.Fatalf("config field errors = %+v, want %s %s", fieldErrs, pkglogger.LogOutputCloudWatchLogs, pkglogger.ConfigFieldSamplingDebugRate)
			}
		})
	}
}

func invalidCloudWatchLogsSamplingConfig() pkglogger.SamplingConfig {
	cfg := validCloudWatchLogsSamplingConfig()
	cfg.SamplingDebugRate = 0
	return cfg
}

func validCloudWatchLogsSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled:      true,
		SamplingDebugRate:      1,
		SamplingInfoRate:       1,
		SamplingWarnRate:       1,
		SamplingErrorRate:      1,
		LoggingByRequestIDRate: 1,
	}
}
