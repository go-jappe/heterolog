package lifecycle

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"testing"

	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/stretchr/testify/require"
)

type lifecycleRunnerReportCall struct {
	report RunReport
	err    error
}

type lifecycleRunReporterFunc func(context.Context, RunReport, error)

func (f lifecycleRunReporterFunc) ReportRun(ctx context.Context, report RunReport, err error) {
	f(ctx, report, err)
}

type notifyingUploader struct {
	uploaded chan string
}

func newNotifyingUploader() *notifyingUploader {
	return &notifyingUploader{
		uploaded: make(chan string, 1),
	}
}

func (u *notifyingUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	_ = ctx
	select {
	case u.uploaded <- input.FilePath():
	default:
	}
	return nil
}

func readGzipFile(t *testing.T, path string) string {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(body)
}
