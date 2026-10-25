package cloudwatchlogs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchLogsConstructorsRejectInvalidBufferConfiguration(t *testing.T) {
	tests := []struct {
		name                   string
		bufferFlushMaxEvents   int
		bufferFlushBytes       int
		fallbackQueueMaxEvents int
		wantKind               error
		wantField              pkglogger.ConfigField
		wantValue              int
		wantLimit              any
	}{
		{
			name:                   "non-positive flush count",
			bufferFlushMaxEvents:   0,
			bufferFlushBytes:       1024,
			fallbackQueueMaxEvents: 1,
			wantKind:               pkglogger.ErrNonPositiveSetting,
			wantField:              pkglogger.ConfigFieldBufferFlushMaxEvents,
			wantValue:              0,
		},
		{
			name:                   "non-positive flush bytes",
			bufferFlushMaxEvents:   1,
			bufferFlushBytes:       0,
			fallbackQueueMaxEvents: 1,
			wantKind:               pkglogger.ErrNonPositiveSetting,
			wantField:              pkglogger.ConfigFieldBufferFlushBytes,
			wantValue:              0,
		},
		{
			name:                   "non-positive fallback queue",
			bufferFlushMaxEvents:   1,
			bufferFlushBytes:       1024,
			fallbackQueueMaxEvents: 0,
			wantKind:               pkglogger.ErrNonPositiveSetting,
			wantField:              pkglogger.ConfigFieldFallbackQueueMaxEvents,
			wantValue:              0,
		},
		{
			// fallback queueはbuffer flushの件数閾値以上でなければなりません。
			name:                   "fallback queue below flush count",
			bufferFlushMaxEvents:   2,
			bufferFlushBytes:       1024,
			fallbackQueueMaxEvents: 1,
			wantKind:               pkglogger.ErrInconsistentSetting,
			wantField:              pkglogger.ConfigFieldFallbackQueueMaxEvents,
			wantValue:              1,
			wantLimit:              2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestCloudWatchLogsLoggerConfigWithBufferLimits(
				tt.bufferFlushMaxEvents,
				tt.bufferFlushBytes,
				tt.fallbackQueueMaxEvents,
				time.Second,
				false,
			)
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
			for constructorName, construct := range constructors {
				t.Run(constructorName, func(t *testing.T) {
					err := construct()
					fieldErr := requireCloudWatchLogsConfigFieldError(t, err, tt.wantKind, tt.wantField, tt.wantValue)
					if fieldErr.Limit != tt.wantLimit {
						t.Fatalf("limit = %v, want %v", fieldErr.Limit, tt.wantLimit)
					}
				})
			}
		})
	}
}

// requireCloudWatchLogsConfigFieldErrorは、errがCloudWatch Logs loggerのfield項目の不正を
// 含み、不正の種類と不正な値を保持することを検証して、その項目のerrorを返します。
func requireCloudWatchLogsConfigFieldError(
	t *testing.T,
	err error,
	kind error,
	field pkglogger.ConfigField,
	value any,
) *pkglogger.ConfigFieldError {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("constructor error = %v, want %v", err, kind)
	}
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Field != field {
			continue
		}
		if !errors.Is(fieldErr, kind) || fieldErr.Value != value {
			t.Fatalf("config field error = %+v, want %v with value %v", fieldErr, kind, value)
		}
		return fieldErr
	}
	t.Fatalf("config field errors = %+v, want %s", pkglogger.ConfigFieldErrors(err), field)
	return nil
}
