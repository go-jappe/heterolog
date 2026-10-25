package opensearch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestOpenSearchNormalAndFallbackConstructorsRejectInvalidSamplingRate(t *testing.T) {
	cfg := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
		FlushInterval:       time.Hour,
		BufferFlushBytes:    5 * 1024 * 1024,
		MaxBulkRequestBytes: 15 * 1024 * 1024,
		MaxBulkDocument:     1,
		SamplingConfig:      invalidOpenSearchSamplingConfig(),
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewOpenSearchLogger(
				context.Background(), cfg, nil, nil, nil, nil,
				observer.NopTracer{}, observer.NopMeter{}, nil,
			)
			return err
		},
		"fallback": func() error {
			_, err := newFallbackLogger(
				cfg, nil, nil, nil, nil, observer.NopTracer{}, observer.NopMeter{},
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
			if len(fieldErrs) != 1 || fieldErrs[0].Backend != pkglogger.LogOutputOpenSearch ||
				fieldErrs[0].Field != pkglogger.ConfigFieldSamplingDebugRate {
				t.Fatalf("config field errors = %+v, want %s %s", fieldErrs, pkglogger.LogOutputOpenSearch, pkglogger.ConfigFieldSamplingDebugRate)
			}
		})
	}
}

func invalidOpenSearchSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled:      true,
		SamplingDebugRate:      0,
		SamplingInfoRate:       1,
		SamplingWarnRate:       1,
		SamplingErrorRate:      1,
		LoggingByRequestIDRate: 1,
	}
}
