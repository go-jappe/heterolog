package uploader_test

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestNewUploadRequestResolvesPathWithoutChangingFileName(t *testing.T) {
	t.Parallel()
	path := filepath.Join("Logs", "App_20260929_3.log.gz")
	want, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	request, err := uploader.NewUploadRequest(path)
	if err != nil {
		t.Fatal(err)
	}
	if request.FilePath() != want {
		t.Fatalf("FilePath() = %q, want %q", request.FilePath(), want)
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestNewUploadRequestRejectsEmptyPaths(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"", " ", "\t\n"} {
		if _, err := uploader.NewUploadRequest(path); err == nil {
			t.Errorf("NewUploadRequest(%q) accepted an empty path", path)
		}
	}
	if err := (uploader.UploadRequest{}).Validate(); err == nil {
		t.Fatal("zero UploadRequest must be rejected")
	}
}

func TestNewUploadInputRequiresRequestAndCompleteKey(t *testing.T) {
	t.Parallel()
	request, err := uploader.NewUploadRequest(filepath.Join(t.TempDir(), "App.log.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.NewUploadInput(request, ""); err == nil {
		t.Error("NewUploadInput() accepted an empty key")
	}
	if _, err := uploader.NewUploadInput(uploader.UploadRequest{}, "logs/App.log.gz"); err == nil {
		t.Fatal("NewUploadInput() accepted a zero request")
	}
	if err := (uploader.UploadInput{}).Validate(); err == nil {
		t.Fatal("zero UploadInput must be rejected")
	}
	for _, key := range []string{"logs/App.log.gz", " logs/App.log.gz ", "\tlogs/App.log.gz\n", " \t\n", "\u3000"} {
		input, err := uploader.NewUploadInput(request, key)
		if err != nil {
			t.Fatalf("NewUploadInput(%q): %v", key, err)
		}
		if input.FilePath() != request.FilePath() || input.ObjectKey() != key {
			t.Errorf("resolved input = (%q, %q), want (%q, %q)", input.FilePath(), input.ObjectKey(), request.FilePath(), key)
		}
		if err := input.Validate(); err != nil {
			t.Errorf("Validate(%q): %v", key, err)
		}
	}
}
