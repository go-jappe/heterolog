package compose

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// TestFromTargetsWithObserversInstrumentsUploadsは、WithObserversで渡したTracerとMeterが、
// 組み立てたuploaderの宛先ごとのuploadを計装することを確認します。
func TestFromTargetsWithObserversInstrumentsUploads(t *testing.T) {
	t.Parallel()

	const target objectstorage.UploadTarget = "custom"
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()
	created, err := FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{target},
	}), map[objectstorage.UploadTarget]Builder{
		target: func(objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
			return []uploader.Destination{{ID: "custom-destination", Uploader: &recordingUploader{}}}, nil
		},
	}, WithObservers(tracer, meter))
	require.NoError(t, err)

	require.NoError(t, created.Upload(context.Background(), uploadertest.Request(t, "rotated.log.gz")))

	require.Len(t, tracer.Spans(infraobjectstorage.SpanObjectStorageUpload), 1)
	require.Equal(t, int64(1), meter.Count(observertest.Key(
		"heterolog_object_storage_uploads_total", "target", "custom", "outcome", "success",
	)))
}
