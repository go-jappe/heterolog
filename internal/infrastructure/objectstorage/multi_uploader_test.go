package objectstorage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

const (
	firstDestinationID  pkgobjectstorage.DestinationID = "first"
	secondDestinationID pkgobjectstorage.DestinationID = "second"
	thirdDestinationID  pkgobjectstorage.DestinationID = "third"
)

func TestMultiUploaderReturnsResultsInRequestedOrderAndContinuesAfterFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("upload failed")
	first := &recordingUploader{}
	second := &recordingUploader{err: wantErr}
	third := &recordingUploader{}
	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Uploader: first},
		uploader.Destination{ID: secondDestinationID, Uploader: second},
		uploader.Destination{ID: thirdDestinationID, Uploader: third},
	)

	report := multi.UploadDestinations(
		context.Background(),
		uploadertest.Request(t, "rotated.log.gz"),
		[]pkgobjectstorage.DestinationID{thirdDestinationID, secondDestinationID, firstDestinationID},
	)

	if got, want := len(report.Results), 3; got != want {
		t.Fatalf("result count = %d, want %d", got, want)
	}
	for index, wantID := range []pkgobjectstorage.DestinationID{
		thirdDestinationID,
		secondDestinationID,
		firstDestinationID,
	} {
		if got := report.Results[index].DestinationID; got != wantID {
			t.Fatalf("result[%d] destination = %q, want %q", index, got, wantID)
		}
	}
	if !report.Results[0].Succeeded() || !errors.Is(report.Results[1].Err, wantErr) || !report.Results[2].Succeeded() {
		t.Fatalf("results = %+v, want success/failure/success", report.Results)
	}
	if !first.called || !second.called || !third.called {
		t.Fatalf("called = first:%t second:%t third:%t, want all true", first.called, second.called, third.called)
	}
	var destinationErr *uploader.DestinationUploadError
	if !errors.As(report.Err(), &destinationErr) || destinationErr.Destination != secondDestinationID ||
		!errors.Is(destinationErr, wantErr) {
		t.Fatalf("report error = %v, want destination id and wrapped error", report.Err())
	}
}

func TestMultiUploaderUsesEachDestinationFailureClassifier(t *testing.T) {
	t.Parallel()

	uploadErr := errors.New("same backend response")
	multi := newTestMultiUploader(t,
		uploader.Destination{
			ID: firstDestinationID, Uploader: &recordingUploader{err: uploadErr},
			ClassifyFailure: func(context.Context, error) uploader.FailureCategory {
				return uploader.FailureCategoryPermanent
			},
		},
		uploader.Destination{
			ID: secondDestinationID, Uploader: &recordingUploader{err: uploadErr},
			ClassifyFailure: func(context.Context, error) uploader.FailureCategory {
				return uploader.FailureCategoryRetryable
			},
		},
	)
	report := multi.UploadDestinations(context.Background(), uploadertest.Request(t, "log.gz"), multi.DestinationIDs())
	for index, want := range []uploader.FailureCategory{uploader.FailureCategoryPermanent, uploader.FailureCategoryRetryable} {
		if got := report.Results[index]; !errors.Is(got.Err, uploadErr) || got.FailureCategory != want {
			t.Fatalf("result[%d] = %+v, want original error and category %q", index, got, want)
		}
	}
}

func TestMultiUploaderHandlesMissingOrInvalidFailureClassification(t *testing.T) {
	t.Parallel()

	uploadErr := errors.New("unclassified upload failure")
	for _, test := range []struct {
		name       string
		uploadErr  error
		classifier func(context.Context, error) uploader.FailureCategory
		want       uploader.FailureCategory
	}{
		{name: "unspecified", uploadErr: uploadErr, want: uploader.FailureCategoryRetryable},
		{name: "invalid", uploadErr: uploadErr, classifier: func(context.Context, error) uploader.FailureCategory {
			return "unknown"
		}, want: uploader.FailureCategoryRetryable},
		{name: "classifier panic", uploadErr: uploadErr, classifier: func(context.Context, error) uploader.FailureCategory {
			panic("classifier failed")
		}, want: uploader.FailureCategoryRetryable},
		{name: "success does not classify", classifier: func(context.Context, error) uploader.FailureCategory {
			panic("successful upload must not be classified")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			multi := newTestMultiUploader(t, uploader.Destination{
				ID: firstDestinationID, Uploader: &recordingUploader{err: test.uploadErr}, ClassifyFailure: test.classifier,
			})
			report := multi.UploadDestinations(context.Background(), uploadertest.Request(t, "log.gz"), multi.DestinationIDs())
			result := report.Results[0]
			if !errors.Is(result.Err, test.uploadErr) || result.FailureCategory != test.want {
				t.Fatalf("result = %+v, want error %v and category %q", result, test.uploadErr, test.want)
			}
		})
	}
}

func TestMultiUploaderUploadUsesAllDestinationsAndJoinsFailures(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first failed")
	secondErr := errors.New("second failed")
	first := &recordingUploader{err: firstErr}
	second := &recordingUploader{err: secondErr}
	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Uploader: first},
		uploader.Destination{ID: secondDestinationID, Uploader: second},
	)

	err := multi.Upload(context.Background(), uploadertest.Request(t, "rotated.log.gz"))
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Upload() error = %v, want both child errors", err)
	}
	if !first.called || !second.called {
		t.Fatalf("called = first:%t second:%t, want both true", first.called, second.called)
	}
}

func TestMultiUploaderRunsDestinationsConcurrently(t *testing.T) {
	t.Parallel()

	started := make(chan pkgobjectstorage.DestinationID, 2)
	release := make(chan struct{})
	multi := newTestMultiUploader(t,
		uploader.Destination{
			ID:       firstDestinationID,
			Uploader: blockingUploader{id: firstDestinationID, started: started, release: release},
		},
		uploader.Destination{
			ID:       secondDestinationID,
			Uploader: blockingUploader{id: secondDestinationID, started: started, release: release},
		},
	)

	done := make(chan uploader.UploadReport, 1)
	go func() {
		done <- multi.UploadDestinations(
			context.Background(),
			uploadertest.Request(t, "rotated.log.gz"),
			multi.DestinationIDs(),
		)
	}()

	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("all destinations did not start before one was released")
		}
	}
	close(release)

	select {
	case report := <-done:
		if err := report.Err(); err != nil {
			t.Fatalf("UploadDestinations() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UploadDestinations() did not finish")
	}
}

func TestMultiUploaderClassifiesPanicAndCompletesOtherDestination(t *testing.T) {
	t.Parallel()

	recorder := &recordingUploader{}
	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Uploader: panicUploader{}},
		uploader.Destination{ID: secondDestinationID, Uploader: recorder},
	)

	report := multi.UploadDestinations(
		context.Background(),
		uploadertest.Request(t, "rotated.log.gz"),
		multi.DestinationIDs(),
	)
	var panicErr *DestinationPanicError
	if !errors.As(report.Results[0].Err, &panicErr) || panicErr.Destination != firstDestinationID || panicErr.Value != "upload panic" {
		t.Fatalf("panic result = %+v, want panic error", report.Results[0])
	}
	if !report.Results[1].Succeeded() || !recorder.called {
		t.Fatalf("second result = %+v called=%t, want success", report.Results[1], recorder.called)
	}
}

func TestMultiUploaderClassifiesInvalidRequestedDestinations(t *testing.T) {
	t.Parallel()

	recorder := &recordingUploader{}
	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Uploader: recorder},
	)
	report := multi.UploadDestinations(
		context.Background(),
		uploadertest.Request(t, "rotated.log.gz"),
		[]pkgobjectstorage.DestinationID{
			firstDestinationID,
			firstDestinationID,
			"",
			"unknown",
		},
	)

	if !report.Results[0].Succeeded() {
		t.Fatalf("first result = %+v, want success", report.Results[0])
	}
	if !errors.Is(report.Results[1].Err, uploader.ErrDuplicateDestinationID) {
		t.Fatalf("duplicate result = %v", report.Results[1].Err)
	}
	if !errors.Is(report.Results[2].Err, uploader.ErrDestinationIDRequired) {
		t.Fatalf("blank result = %v", report.Results[2].Err)
	}
	if !errors.Is(report.Results[3].Err, uploader.ErrUnknownDestinationID) {
		t.Fatalf("unknown result = %v", report.Results[3].Err)
	}
}

func TestMultiUploaderRejectsUnconstructedRequestBeforeSending(t *testing.T) {
	t.Parallel()

	recorder := &recordingUploader{}
	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Uploader: recorder},
	)
	report := multi.UploadDestinations(context.Background(), uploader.UploadRequest{}, multi.DestinationIDs())
	if report.Err() == nil || recorder.called {
		t.Fatalf("invalid request: error=%v child called=%t", report.Err(), recorder.called)
	}
}

func TestNewMultiUploaderRejectsInvalidDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		destinations []uploader.Destination
		wantErr      error
	}{
		{name: "empty", wantErr: ErrDestinationsRequired},
		{
			name: "blank id",
			destinations: []uploader.Destination{
				{ID: " ", Uploader: &recordingUploader{}},
			},
			wantErr: uploader.ErrDestinationIDRequired,
		},
		{
			name: "nil uploader",
			destinations: []uploader.Destination{
				{ID: firstDestinationID},
			},
			wantErr: ErrDestinationUploaderRequired,
		},
		{
			name: "duplicate id",
			destinations: []uploader.Destination{
				{ID: firstDestinationID, Uploader: &recordingUploader{}},
				{ID: firstDestinationID, Uploader: &recordingUploader{}},
			},
			wantErr: uploader.ErrDuplicateDestinationID,
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			created, err := NewMultiUploader(testCase.destinations...)
			if err == nil {
				t.Fatalf("NewMultiUploader() error = nil, want error; uploader=%T", created)
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("NewMultiUploader() error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMultiUploaderDestinationIDsReturnsIndependentSlice(t *testing.T) {
	t.Parallel()

	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Uploader: &recordingUploader{}},
		uploader.Destination{ID: secondDestinationID, Uploader: &recordingUploader{}},
	)
	ids := multi.DestinationIDs()
	ids[0] = "changed"
	if got := multi.DestinationIDs()[0]; got != firstDestinationID {
		t.Fatalf("DestinationIDs()[0] = %q, want %q", got, firstDestinationID)
	}
}

func TestMultiUploaderLooksUpExplicitDestinationService(t *testing.T) {
	t.Parallel()

	multi := newTestMultiUploader(t,
		uploader.Destination{ID: firstDestinationID, Service: pkgobjectstorage.UploadTargetS3, Uploader: &recordingUploader{}},
		uploader.Destination{ID: secondDestinationID, Service: "custom", Uploader: &recordingUploader{}},
		uploader.Destination{ID: thirdDestinationID, Uploader: &recordingUploader{}},
	)
	for _, test := range []struct {
		id      pkgobjectstorage.DestinationID
		service pkgobjectstorage.UploadTarget
		found   bool
	}{
		{firstDestinationID, pkgobjectstorage.UploadTargetS3, true},
		{secondDestinationID, "custom", true},
		{thirdDestinationID, "", false},
		{"s3-unregistered", "", false},
	} {
		service, found := multi.DestinationService(test.id)
		if service != test.service || found != test.found {
			t.Fatalf("DestinationService(%q) = %q, %t; want %q, %t", test.id, service, found, test.service, test.found)
		}
	}
	var absent *MultiUploader
	if service, found := absent.DestinationService(firstDestinationID); service != "" || found {
		t.Fatalf("nil DestinationService = %q, %t", service, found)
	}
}

func newTestMultiUploader(t *testing.T, destinations ...uploader.Destination) *MultiUploader {
	t.Helper()
	multi, err := NewMultiUploader(destinations...)
	if err != nil {
		t.Fatalf("NewMultiUploader() error = %v", err)
	}
	return multi
}

type recordingUploader struct {
	called bool
	input  uploader.UploadRequest
	err    error
}

func (r *recordingUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	_ = ctx
	r.called = true
	r.input = input
	return r.err
}

type blockingUploader struct {
	id      pkgobjectstorage.DestinationID
	started chan<- pkgobjectstorage.DestinationID
	release <-chan struct{}
}

func (u blockingUploader) Upload(context.Context, uploader.UploadRequest) error {
	u.started <- u.id
	<-u.release
	return nil
}

type panicUploader struct{}

func (panicUploader) Upload(context.Context, uploader.UploadRequest) error {
	panic("upload panic")
}
