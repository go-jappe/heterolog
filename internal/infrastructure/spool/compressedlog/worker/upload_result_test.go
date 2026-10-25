package worker

import (
	"context"
	"errors"
	"slices"
	"testing"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestWorkerValidatesSingleDestinationUploadResult(t *testing.T) {
	const destination objectstorage.DestinationID = "selected-destination"
	uploadErr := errors.New("upload failed")
	permanent := uploader.DestinationResult{
		DestinationID: destination, Err: uploadErr, FailureCategory: uploader.FailureCategoryPermanent,
	}
	for _, tc := range []struct {
		name    string
		results []uploader.DestinationResult
		wantErr error
		success bool
	}{
		{name: "success", results: []uploader.DestinationResult{{DestinationID: destination}}, success: true},
		{name: "missing result"},
		{name: "duplicate results", results: []uploader.DestinationResult{permanent, permanent}, wantErr: uploadErr},
		{name: "another destination", results: []uploader.DestinationResult{{DestinationID: "another", Err: uploadErr}}, wantErr: uploadErr},
		{name: "no classification", results: []uploader.DestinationResult{{DestinationID: destination, Err: uploadErr}}, wantErr: uploadErr},
		{name: "invalid classification", results: []uploader.DestinationResult{{DestinationID: destination, Err: uploadErr, FailureCategory: "invalid"}}, wantErr: uploadErr},
		{name: "canceled upload", results: []uploader.DestinationResult{{DestinationID: destination, Err: context.Canceled}}, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := replaytest.NewStore(t)
			replaytest.SaveFile(t, store, "app_20261001_1.log.gz")
			upload := &reportedTestUploader{
				Uploader: &replaytest.Uploader{DestinationID: destination},
				report:   uploader.UploadReport{Results: tc.results},
			}
			worker := newReplayTestWorker(t, store, RouteRegistry{{
				FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
			}})
			report, err := worker.RunOnce(context.Background())
			if upload.calls != 1 || !slices.Equal(upload.requested, []objectstorage.DestinationID{destination}) {
				t.Fatalf("upload calls/destinations = %d/%v, want one call for %q", upload.calls, upload.requested, destination)
			}
			wantState := pkgspool.StateActive
			if tc.success {
				wantState = pkgspool.StateDone
				if err != nil || !report.File.UploadAttempted || !report.File.UploadSucceeded || report.File.FailureCategory != "" || report.File.DeferredReason != DeferredReasonNone {
					t.Fatalf("RunOnce = %+v, %v, want successful upload", report, err)
				}
			} else if err == nil || !report.File.UploadAttempted || report.File.UploadSucceeded || report.File.FailureCategory != uploader.FailureCategoryRetryable || report.File.DeferredReason != DeferredReasonRetryableUploadFailure {
				t.Fatalf("RunOnce = %+v, %v, want retryable failure", report, err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("RunOnce error = %v, want original error %v", err, tc.wantErr)
			}
			if report.File.State != wantState {
				t.Fatalf("state = %q, want %q", report.File.State, wantState)
			}
			_ = replaytest.OnlyFile(t, store, wantState)
		})
	}
}

type reportedTestUploader struct {
	*replaytest.Uploader
	report    uploader.UploadReport
	requested []objectstorage.DestinationID
	calls     int
}

func (u *reportedTestUploader) UploadDestinations(_ context.Context, _ uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	u.calls++
	u.requested = slices.Clone(ids)
	return u.report
}
