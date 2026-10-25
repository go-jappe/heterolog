package s3

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestUploaderDelegatesOnceWithoutChangingRequest(t *testing.T) {
	remoteErr := errors.New("upload failed")
	remote := &testDestinationUploader{err: remoteErr}
	upload, err := New(remote)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := uploadertest.Request(t, "/spool/processing/app_20260922_1.log.gz")
	if err := upload.Upload(ctx, input); err != remoteErr {
		t.Fatalf("Upload error = %v, want original error", err)
	}
	if remote.calls != 1 || remote.ctx != ctx || remote.input != input {
		t.Fatalf("delegation changed: calls=%d input=%+v", remote.calls, remote.input)
	}
	ids := upload.DestinationIDs()
	if len(ids) != 1 || ids[0] != "destination-s3" {
		t.Fatalf("destination ids = %v", ids)
	}
}

func TestUploaderPreservesDestinationResultsWithoutReclassifying(t *testing.T) {
	// S3の生エラーだけならpermanentですが、送信先がretryableと確定した結果を維持します。
	uploadErr := &pkgs3.HTTPStatusError{Code: 403}
	want := uploader.UploadReport{Results: []uploader.DestinationResult{{
		DestinationID: "destination-s3", Err: uploadErr, FailureCategory: uploader.FailureCategoryRetryable,
	}}}
	remote := &testDestinationUploader{report: want}
	upload, err := New(remote)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := uploadertest.Request(t, "/spool/processing/app_20260922_1.log.gz")
	ids := []objectstorage.DestinationID{"destination-s3"}
	got := upload.UploadDestinations(ctx, input, ids)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UploadDestinations = %+v, want unchanged result %+v", got, want)
	}
	if remote.calls != 1 || remote.ctx != ctx || remote.input != input || !reflect.DeepEqual(remote.ids, ids) {
		t.Fatalf("delegation changed: calls=%d input=%+v ids=%v", remote.calls, remote.input, remote.ids)
	}
}

func TestUploaderUsesS3CircuitBreakerClassification(t *testing.T) {
	upload, err := New(&testDestinationUploader{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"service failure", &pkgs3.HTTPStatusError{Code: 503}, true},
		{"access denied", &pkgs3.HTTPStatusError{Code: 403}, false},
		{"request timeout", &pkgs3.HTTPStatusError{Code: 400, ErrorCode: pkgs3.ErrorCodeRequestTimeout}, true},
		{"temporary redirect", &pkgs3.HTTPStatusError{Code: 307, ErrorCode: pkgs3.ErrorCodeTemporaryRedirect}, true},
		{"operation aborted", &pkgs3.HTTPStatusError{Code: 409, ErrorCode: pkgs3.ErrorCodeOperationAborted}, true},
		{"canceled", context.Canceled, false},
		{"unknown", errors.New("unknown upload outcome"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision := upload.ClassifyRetry(context.Background(), test.err)
			if decision.Retry != test.retry {
				t.Fatalf("breaker classification = %+v, want retry=%t", decision, test.retry)
			}
		})
	}
	if upload.next.(*testDestinationUploader).calls != 0 {
		t.Fatal("classification performed another upload")
	}
}

func TestUploaderRejectsNilDestination(t *testing.T) {
	if got, err := New(nil); got != nil || err == nil {
		t.Fatalf("New(nil) = %v, %v", got, err)
	}
}

type testDestinationUploader struct {
	calls  int
	ctx    context.Context
	input  uploader.UploadRequest
	err    error
	ids    []objectstorage.DestinationID
	report uploader.UploadReport
}

func (u *testDestinationUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	u.calls++
	u.ctx, u.input = ctx, input
	return u.err
}

func (*testDestinationUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"destination-s3"}
}

func (u *testDestinationUploader) UploadDestinations(ctx context.Context, input uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	u.calls++
	u.ctx, u.input, u.ids = ctx, input, ids
	return u.report
}
