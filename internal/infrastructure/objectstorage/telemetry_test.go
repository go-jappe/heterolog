package objectstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// spanCapturingUploaderは、uploadに渡されたcontextのspanを記録します。
type spanCapturingUploader struct {
	span pkgobserver.Span
	err  error
}

func (u *spanCapturingUploader) Upload(ctx context.Context, _ uploader.UploadRequest) error {
	u.span = observer.SpanFromContext(ctx)
	return u.err
}

// TestMultiUploaderTracesAndCountsEachDestinationUploadは、宛先ごとのuploadを呼び出し元の
// spanの子のspanで囲んでそのcontextを渡し、成功・送り直さない失敗・panicを結果ごとに数え、
// 所要時間を記録し、失敗をspanに記録することを確認します。
func TestMultiUploaderTracesAndCountsEachDestinationUpload(t *testing.T) {
	t.Parallel()

	const target objectstorage.UploadTarget = "s3"
	succeeded := &spanCapturingUploader{}
	uploadErr := errors.New("access denied")
	rejected := &spanCapturingUploader{err: uploadErr}
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()
	multi, err := NewMultiUploaderWithObservers(tracer, meter,
		uploader.Destination{ID: firstDestinationID, Service: target, Uploader: succeeded},
		uploader.Destination{
			ID: secondDestinationID, Service: target, Uploader: rejected,
			ClassifyFailure: func(context.Context, error) uploader.FailureCategory {
				return uploader.FailureCategoryPermanent
			},
		},
		uploader.Destination{ID: "third", Service: target, Uploader: panicUploader{}},
	)
	require.NoError(t, err)
	ctx, parent := tracer.StartSpan(context.Background(), "rotation")

	multi.UploadDestinations(ctx, uploadertest.Request(t, "rotated.log.gz"), multi.DestinationIDs())
	parent.End()

	spans := tracer.Spans(SpanObjectStorageUpload)
	require.Len(t, spans, 3)
	byDestination := map[any]*observertest.Span{}
	for _, span := range spans {
		require.Same(t, parent, span.Parent)
		require.Equal(t, 1, span.Ends())
		gotTarget, _ := span.Attr("heterolog.target")
		require.Equal(t, "s3", gotTarget)
		destination, _ := span.Attr("heterolog.destination")
		byDestination[destination] = span
	}
	require.Same(t, byDestination[string(firstDestinationID)], succeeded.span)
	require.Same(t, byDestination[string(secondDestinationID)], rejected.span)
	require.Empty(t, byDestination[string(firstDestinationID)].Errors())
	require.Len(t, byDestination[string(secondDestinationID)].Errors(), 1)
	require.Len(t, byDestination["third"].Errors(), 1)

	const uploads = "heterolog_object_storage_uploads_total"
	require.Equal(t, map[string]int64{
		observertest.Key(uploads, "target", "s3", "outcome", "success"):   1,
		observertest.Key(uploads, "target", "s3", "outcome", "permanent"): 1,
		observertest.Key(uploads, "target", "s3", "outcome", "retryable"): 1,
	}, meter.Counters())
	const duration = "heterolog_object_storage_upload_duration_seconds"
	for _, outcome := range []string{"success", "permanent", "retryable"} {
		require.Len(t, meter.HistogramValues(observertest.Key(duration, "target", "s3", "outcome", outcome)), 1, outcome)
	}
}
