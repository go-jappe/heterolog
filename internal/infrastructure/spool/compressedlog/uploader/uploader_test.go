package uploader

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestNewRejectsUnsupportedService(t *testing.T) {
	for _, service := range []objectstorage.UploadTarget{"", "future-service"} {
		result, err := New(service, nil)
		var unsupported *UnsupportedUploadServiceError
		if result != nil || !errors.Is(err, ErrUnsupportedUploadService) || !errors.As(err, &unsupported) || unsupported.Service != service {
			t.Fatalf("New(%q) = %v, %v, want unsupported service error", service, result, err)
		}
	}
}

// 既存のlog出力と同じ文言を保ちます。
func TestUnsupportedUploadServiceErrorFormat(t *testing.T) {
	want := `unsupported compressed log spool upload service "future-service"`
	if got := (&UnsupportedUploadServiceError{Service: "future-service"}).Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestNewS3RequiresDestination(t *testing.T) {
	if result, err := New(objectstorage.UploadTargetS3, nil); result != nil || err == nil {
		t.Fatalf("New(s3, nil) = %v, %v", result, err)
	}
}
