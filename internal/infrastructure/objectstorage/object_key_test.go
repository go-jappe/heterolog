package objectstorage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestBuildObjectKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   string
		filePath string
		want     string
	}{
		{
			name:     "prefix and file name",
			prefix:   "logs/default",
			filePath: "/var/log/app/rotated.log.gz",
			want:     "logs/default/rotated.log.gz",
		},
		{
			name:     "trim slashes",
			prefix:   "/logs/default/",
			filePath: "rotated.log.gz",
			want:     "logs/default/rotated.log.gz",
		},
		{
			name:     "empty prefix",
			prefix:   "",
			filePath: "/var/log/app/rotated.log.gz",
			want:     "rotated.log.gz",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := BuildObjectKey(tt.prefix, tt.filePath); got != tt.want {
				t.Fatalf("BuildObjectKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestObjectKeyUploaderResolvesDestinationPrefixAndPreservesAbsolutePath(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"logs/default", " logs/default "} {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()
			recorder := &objectKeyRecordingUploader{}
			wrapped := NewObjectKeyUploader(recorder, prefix)
			request := uploadertest.Request(t, filepath.Join(t.TempDir(), "Rotated.log.gz"))

			if err := wrapped.Upload(context.Background(), request); err != nil {
				t.Fatalf("Upload() error = %v", err)
			}
			if got, want := recorder.input.ObjectKey(), prefix+"/Rotated.log.gz"; got != want {
				t.Fatalf("ObjectKey = %q, want %q", got, want)
			}
			if got := recorder.input.FilePath(); got != request.FilePath() {
				t.Fatalf("FilePath = %q, want %q", got, request.FilePath())
			}
		})
	}
}

func TestObjectKeyUploaderRejectsZeroRequest(t *testing.T) {
	t.Parallel()

	recorder := &objectKeyRecordingUploader{}
	wrapped := NewObjectKeyUploader(recorder, "logs/default")

	err := wrapped.Upload(context.Background(), uploader.UploadRequest{})
	if err == nil {
		t.Fatal("Upload() error = nil, want invalid request error")
	}
	if recorder.called {
		t.Fatal("invalid request must not reach object uploader")
	}
}

type objectKeyRecordingUploader struct {
	input  uploader.UploadInput
	called bool
}

func (r *objectKeyRecordingUploader) Upload(ctx context.Context, input uploader.UploadInput) error {
	_ = ctx
	r.input = input
	r.called = true
	return nil
}
