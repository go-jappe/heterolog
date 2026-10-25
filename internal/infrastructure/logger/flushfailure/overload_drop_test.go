package flushfailure

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// criticalMemoryMonitorは、常にmemoryの逼迫を報告する試験用monitorです。
type criticalMemoryMonitor struct{}

func (criticalMemoryMonitor) Current(context.Context) (memorypressure.MemoryPressure, error) {
	return memorypressure.MemoryPressure{}, nil
}

func (criticalMemoryMonitor) IsCritical(context.Context) (bool, error) {
	return true, nil
}

func (criticalMemoryMonitor) Last() memorypressure.MonitorSnapshot {
	return memorypressure.MonitorSnapshot{}
}

func (criticalMemoryMonitor) Reset() {}

func (criticalMemoryMonitor) Start(context.Context) error {
	return nil
}

// spoolが過負荷のためdrop policyで破棄した場合、同期APIへ返すCompositeのerrorも
// ErrLogDroppedを保ち、呼び出し元がerrors.Isで判定できます。
func TestBuildPropagatesSpoolOverloadDrop(t *testing.T) {
	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: t.TempDir(),
				Target:    "test-app",
				Dir:       "test-backend",
				OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
					OverloadPolicy:               pkgdurable.OverloadPolicyDrop,
					UseMemoryPressureForOverload: true,
				},
			},
		},
	})
	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		criticalMemoryMonitor{},
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	t.Cleanup(func() {
		if err := drainWithinHangTimeoutForTest(handler); err != nil {
			t.Errorf("Drain error = %v", err)
		}
	})

	err = handler.HandleFailure(context.Background(), loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{{
			Payload: map[string]any{"message": "dropped on overload"},
		}},
	})
	if !errors.Is(err, pkglogger.ErrLogDropped) {
		t.Fatalf("HandleFailure error = %v, want ErrLogDropped", err)
	}
}
