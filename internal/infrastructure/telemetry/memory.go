package telemetry

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RecordMemoryPressureは、backendのspoolが起動したmemory逼迫の監視が読んだ観測pressureと、
// その回の判定criticalをgaugeへ記録します。使用率は、信頼できる値を読めた場合だけ記録します。
func RecordMemoryPressure(
	ctx context.Context,
	meter pkgobserver.Meter,
	backend string,
	pressure memorypressure.MemoryPressure,
	critical bool,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	meter = observer.EnsureMeter(meter)
	attr := pkgobserver.Attr{Key: AttrBackend, Value: backend}
	if pressure.Reliable {
		meter.Gauge(MemoryPressureRatio).Record(ctx, pressure.Ratio, attr)
	}
	value := 0.0
	if critical {
		value = 1
	}
	meter.Gauge(MemoryPressureCritical).Record(ctx, value, attr)
}
