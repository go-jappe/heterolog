package telemetry

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// SpoolReplayCountsは、1回の送り直しの結果ごとのrecordの数です。
type SpoolReplayCounts struct {
	Success    int
	Retryable  int
	Dead       int
	Quarantine int
}

// RecordSpoolReplayは、送り直したrecordを結果ごとに数えます。0件の結果は記録しません。
func RecordSpoolReplay(ctx context.Context, meter pkgobserver.Meter, backend string, counts SpoolReplayCounts) {
	if ctx == nil {
		ctx = context.Background()
	}
	meter = observer.EnsureMeter(meter)
	for _, c := range []struct {
		outcome string
		count   int
	}{
		{OutcomeSuccess, counts.Success},
		{OutcomeRetryable, counts.Retryable},
		{OutcomeDead, counts.Dead},
		{OutcomeQuarantine, counts.Quarantine},
	} {
		if c.count <= 0 {
			continue
		}
		meter.Counter(SpoolReplayRecords).Add(ctx, int64(c.count),
			pkgobserver.Attr{Key: AttrBackend, Value: backend},
			pkgobserver.Attr{Key: AttrOutcome, Value: c.outcome},
		)
	}
}
