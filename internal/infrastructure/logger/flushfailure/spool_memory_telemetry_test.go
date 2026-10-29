package flushfailure

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// TestBuildRecordsOwnedSpoolMemoryPressureは、spoolが自ら起動した監視が、読むたびにmemoryの
// 使用率と判定の結果を、出力先の名前でloggerのmeterのgaugeへ記録することを確認します。
func TestBuildRecordsOwnedSpoolMemoryPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		useMemoryPressureProviderForTest(t, &fixedMemoryPressureProvider{
			pressure: memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true},
		})
		meter := observertest.NewMeter()
		handler, err := Build(
			context.Background(),
			clock.NewUTCClock(),
			memoryThresholdSpoolConfig(t, pkgdurable.SpoolOverloadPolicyConfig{
				MemoryCriticalRatio:           0.9,
				MemoryCriticalConsecutiveHits: 2,
			}),
			nil,
			observer.NopTracer{},
			meter,
			FactoryOptions{Backend: "loki"},
		)
		require.NoError(t, err)

		// 起動時の1回目は連続2回に届かず、次の読み取りでcriticalになります。
		time.Sleep(spoolstorehandler.MemoryPressurePollInterval)
		synctest.Wait()
		require.NoError(t, drainWithinHangTimeoutForTest(handler))

		require.Equal(t, []float64{0.95, 0.95}, meter.GaugeValues(observertest.Key("heterolog_memory_pressure_ratio", "backend", "loki")))
		require.Equal(t, []float64{0, 1}, meter.GaugeValues(observertest.Key("heterolog_memory_pressure_critical", "backend", "loki")))
	})
}
