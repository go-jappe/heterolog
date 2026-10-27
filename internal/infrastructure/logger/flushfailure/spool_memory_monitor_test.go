package flushfailure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	memorymonitor "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// fixedMemoryPressureProviderは、決まったmemory逼迫を返し、読まれた回数を数える試験用providerです。
type fixedMemoryPressureProvider struct {
	pressure memorypressure.MemoryPressure
	err      error
	reads    atomic.Int64
}

func (p *fixedMemoryPressureProvider) ReadMemoryPressure(context.Context) (memorypressure.MemoryPressure, error) {
	p.reads.Add(1)
	return p.pressure, p.err
}

// useMemoryPressureProviderForTestは、spoolが自ら起動する監視のproviderを試験の間だけ差し替えます。
func useMemoryPressureProviderForTest(t *testing.T, provider memorypressure.Provider) {
	t.Helper()
	previous := newMemoryPressureProvider
	newMemoryPressureProvider = func() memorypressure.Provider { return provider }
	t.Cleanup(func() { newMemoryPressureProvider = previous })
}

// memoryThresholdSpoolConfigは、memory逼迫をspoolの閾値で判定し、過負荷ならdropする設定です。
func memoryThresholdSpoolConfig(t *testing.T, policy pkgdurable.SpoolOverloadPolicyConfig) loggerdurable.FailureStoreConfigProvider {
	t.Helper()
	policy.OverloadPolicy = pkgdurable.OverloadPolicyDrop
	policy.UseMemoryPressureForOverload = true
	return loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath:            t.TempDir(),
				Target:               "test-app",
				Dir:                  "test-backend",
				OverloadPolicyConfig: policy,
			},
		},
	})
}

func handleOneFailureForMemoryTest(handler *CompositeFailureHandler) error {
	return handler.HandleFailure(context.Background(), loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{{
			Payload: map[string]any{"message": "memory threshold entry"},
		}},
	})
}

// 監視を渡さない場合、spoolはMemoryCriticalRatioなどの閾値で判定する監視を自ら起動し、
// その判定で過負荷の方針を適用します。
func TestBuildAppliesSpoolMemoryThresholdsWithoutMonitor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ratio    float64
		wantDrop bool
	}{
		{name: "below the ratio", ratio: 0.5},
		{name: "at the ratio", ratio: 0.9, wantDrop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fixedMemoryPressureProvider{
				pressure: memorypressure.MemoryPressure{Ratio: tc.ratio, Reliable: true},
			}
			useMemoryPressureProviderForTest(t, provider)
			handler, err := Build(
				context.Background(),
				clock.NewUTCClock(),
				memoryThresholdSpoolConfig(t, pkgdurable.SpoolOverloadPolicyConfig{
					MemoryCriticalRatio:           0.9,
					MemoryCriticalConsecutiveHits: 1,
				}),
				nil,
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
			if provider.reads.Load() == 0 {
				t.Fatal("Build did not read the memory pressure before returning")
			}

			err = handleOneFailureForMemoryTest(handler)
			if got := errors.Is(err, pkglogger.ErrLogDropped); got != tc.wantDrop {
				t.Fatalf("HandleFailure error = %v, want ErrLogDropped %t", err, tc.wantDrop)
			}
			if !tc.wantDrop && err != nil {
				t.Fatalf("HandleFailure error = %v, want the record saved to the spool", err)
			}
		})
	}
}

// 連続回数は監視の読み取りごとに数えます。閾値を1回超えただけでは、過負荷にしません。
func TestBuildCountsSpoolMemoryHitsPerReading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider := &fixedMemoryPressureProvider{
			pressure: memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true},
		}
		useMemoryPressureProviderForTest(t, provider)
		handler, err := Build(
			context.Background(),
			clock.NewUTCClock(),
			memoryThresholdSpoolConfig(t, pkgdurable.SpoolOverloadPolicyConfig{
				MemoryCriticalRatio:           0.9,
				MemoryCriticalConsecutiveHits: 2,
			}),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
			FactoryOptions{},
		)
		if err != nil {
			t.Fatalf("Build error = %v", err)
		}
		// 起動時の1回だけでは連続2回に届かないため、保存します。
		if err := handleOneFailureForMemoryTest(handler); err != nil {
			t.Fatalf("first HandleFailure error = %v, want the record saved", err)
		}
		// 次の読み取りで2回目の該当になり、過負荷としてdropします。
		time.Sleep(spoolstorehandler.MemoryPressurePollInterval)
		synctest.Wait()
		if err := handleOneFailureForMemoryTest(handler); !errors.Is(err, pkglogger.ErrLogDropped) {
			t.Fatalf("second HandleFailure error = %v, want ErrLogDropped", err)
		}
		if err := drainWithinHangTimeoutForTest(handler); err != nil {
			t.Fatalf("Drain error = %v", err)
		}
	})
}

// spoolが自ら起動した監視は、handlerのShutdown（CompositeのDrain）で止まります。
// 止まらなければ、synctestのbubbleが終わりません。
func TestBuildStopsOwnedSpoolMemoryMonitorOnShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider := &fixedMemoryPressureProvider{
			pressure: memorypressure.MemoryPressure{Ratio: 0.5, Reliable: true},
		}
		useMemoryPressureProviderForTest(t, provider)
		handler, err := Build(
			context.Background(),
			clock.NewUTCClock(),
			memoryThresholdSpoolConfig(t, pkgdurable.SpoolOverloadPolicyConfig{MemoryCriticalRatio: 0.9}),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
			FactoryOptions{},
		)
		if err != nil {
			t.Fatalf("Build error = %v", err)
		}
		time.Sleep(3 * spoolstorehandler.MemoryPressurePollInterval)
		synctest.Wait()
		if got := provider.reads.Load(); got != 4 {
			t.Fatalf("reads before shutdown = %d, want 4 (start and three polls)", got)
		}
		if err := drainWithinHangTimeoutForTest(handler); err != nil {
			t.Fatalf("Drain error = %v", err)
		}
		readsAtShutdown := provider.reads.Load()
		time.Sleep(10 * spoolstorehandler.MemoryPressurePollInterval)
		synctest.Wait()
		if got := provider.reads.Load(); got != readsAtShutdown {
			t.Fatalf("reads after shutdown = %d, want %d", got, readsAtShutdown)
		}
	})
}

// 呼び出し元が監視を渡した場合は、その監視の判定を使い、spoolは自分の監視を起動しません。
func TestBuildPrefersGivenMemoryMonitorOverSpoolThresholds(t *testing.T) {
	provider := &fixedMemoryPressureProvider{
		pressure: memorypressure.MemoryPressure{Ratio: 0.5, Reliable: true},
	}
	useMemoryPressureProviderForTest(t, provider)
	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		memoryThresholdSpoolConfig(t, pkgdurable.SpoolOverloadPolicyConfig{MemoryCriticalRatio: 0.9}),
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
	if err := handleOneFailureForMemoryTest(handler); !errors.Is(err, pkglogger.ErrLogDropped) {
		t.Fatalf("HandleFailure error = %v, want the given monitor's overload", err)
	}
	if got := provider.reads.Load(); got != 0 {
		t.Fatalf("spool provider reads = %d, want 0 when a monitor is given", got)
	}
}

// 閾値を一つも有効にしない設定や、最初の読み取りの失敗は、構築時のerrorになります。
func TestBuildRejectsUnusableSpoolMemoryThresholds(t *testing.T) {
	readErr := errors.New("memory pressure is unavailable")
	for _, tc := range []struct {
		name     string
		policy   pkgdurable.SpoolOverloadPolicyConfig
		provider *fixedMemoryPressureProvider
		wantErr  error
	}{
		{
			name:     "no threshold",
			provider: &fixedMemoryPressureProvider{pressure: memorypressure.MemoryPressure{Ratio: 0.5, Reliable: true}},
			wantErr:  memorymonitor.ErrInvalidMonitorConfig,
		},
		{
			name:     "first read fails",
			policy:   pkgdurable.SpoolOverloadPolicyConfig{MemoryCriticalRatio: 0.9},
			provider: &fixedMemoryPressureProvider{err: readErr},
			wantErr:  readErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useMemoryPressureProviderForTest(t, tc.provider)
			handler, err := Build(
				context.Background(),
				clock.NewUTCClock(),
				memoryThresholdSpoolConfig(t, tc.policy),
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				FactoryOptions{},
			)
			if handler != nil {
				t.Cleanup(func() {
					if err := drainWithinHangTimeoutForTest(handler); err != nil {
						t.Errorf("Drain error = %v", err)
					}
				})
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Build error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
