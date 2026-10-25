package stdout

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestStdoutNormalAndNotificationTargetConstructorsRejectInvalidSamplingRate(t *testing.T) {
	cfg := NewConfigAdapter(pkglogger.StdoutLoggerConfig{
		SamplingConfig: invalidStdoutSamplingConfig(),
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewStdoutLogger(context.Background(), cfg, nil, observer.NopTracer{}, observer.NopMeter{})
			return err
		},
		// 内部ログの通知先も、通知の時点ではなく構築時に同じ規則で拒否します。
		"notification target": func() error {
			_, err := NewNotificationTarget(cfg, nil)
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
			if len(fieldErrs) != 1 || fieldErrs[0].Backend != pkglogger.LogOutputStdout ||
				fieldErrs[0].Field != pkglogger.ConfigFieldSamplingDebugRate {
				t.Fatalf("config field errors = %+v, want stdout %s", fieldErrs, pkglogger.ConfigFieldSamplingDebugRate)
			}
		})
	}
}

func invalidStdoutSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled:      true,
		SamplingDebugRate:      0,
		SamplingInfoRate:       1,
		SamplingWarnRate:       1,
		SamplingErrorRate:      1,
		LoggingByRequestIDRate: 1,
	}
}
