package loki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLokiNormalAndFallbackConstructorsRejectInvalidSamplingRate(t *testing.T) {
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		FlushInterval:           time.Hour,
		MaxEntries:              1,
		BufferFlushBytes:        1,
		FallbackQueueMaxEntries: 1,
		MaxLineBytes:            1,
		MaxPushRequestBytes:     2,
		SamplingConfig:          invalidLokiSamplingConfig(),
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewLokiLogger(
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
			if len(fieldErrs) != 1 || fieldErrs[0].Backend != pkglogger.LogOutputLoki ||
				fieldErrs[0].Field != pkglogger.ConfigFieldSamplingDebugRate {
				t.Fatalf("config field errors = %+v, want %s %s", fieldErrs, pkglogger.LogOutputLoki, pkglogger.ConfigFieldSamplingDebugRate)
			}
		})
	}
}

func invalidLokiSamplingConfig() pkglogger.SamplingConfig {
	cfg := validLokiSamplingConfig()
	cfg.SamplingDebugRate = 0
	return cfg
}

func validLokiSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled:      true,
		SamplingDebugRate:      1,
		SamplingInfoRate:       1,
		SamplingWarnRate:       1,
		SamplingErrorRate:      1,
		LoggingByRequestIDRate: 1,
	}
}
