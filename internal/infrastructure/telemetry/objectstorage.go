package telemetry

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RecordObjectStorageUploadは、targetの1つの宛先への1回のuploadを、結果outcomeと所要時間で
// 記録します。
func RecordObjectStorageUpload(
	ctx context.Context,
	meter pkgobserver.Meter,
	target string,
	outcome string,
	elapsed time.Duration,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	meter = observer.EnsureMeter(meter)
	attrs := []pkgobserver.Attr{
		{Key: AttrTarget, Value: target},
		{Key: AttrOutcome, Value: outcome},
	}
	meter.Counter(ObjectStorageUploads).Add(ctx, 1, attrs...)
	meter.Histogram(ObjectStorageUploadDuration).Record(ctx, elapsed.Seconds(), attrs...)
}
