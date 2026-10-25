package worker

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestWorkerUsesUploaderFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name               string
		err                error
		category           uploader.FailureCategory
		wantState          pkgspool.State
		wantDeferredReason DeferredReason
	}{
		{
			name:     "unrecognized error classified permanent",
			err:      errors.New("another service rejected this file permanently"),
			category: uploader.FailureCategoryPermanent, wantState: pkgspool.StateDead,
			wantDeferredReason: DeferredReasonNone,
		},
		{
			name:     "forbidden error classified retryable",
			err:      &pkgs3.HTTPStatusError{Code: http.StatusForbidden},
			category: uploader.FailureCategoryRetryable, wantState: pkgspool.StateActive,
			wantDeferredReason: DeferredReasonRetryableUploadFailure,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := replaytest.NewStore(t)
			replaytest.SaveFile(t, store, "app_20260922_1.log.gz")
			upload := &classifiedTestUploader{
				Uploader: &replaytest.Uploader{Err: tc.err, DestinationID: "other-service"},
				category: tc.category,
			}
			worker := newReplayTestWorker(t, store, RouteRegistry{{
				FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
			}})
			report, err := worker.RunOnce(context.Background())
			if !errors.Is(err, tc.err) || report.File.State != tc.wantState {
				t.Fatalf("RunOnce = %+v, %v, want service-defined %s failure", report, err, tc.category)
			}
			if report.File.FailureCategory != tc.category || !report.File.UploadAttempted || report.File.UploadSucceeded || report.File.DeferredReason != tc.wantDeferredReason {
				t.Fatalf("RunOnce = %+v, want one %s result", report, tc.category)
			}
			if upload.CallCount() != 1 {
				t.Fatal("worker retried during one cycle")
			}
			_ = replaytest.OnlyFile(t, store, tc.wantState)
		})
	}
}

func TestWorkerUsesUploaderClassificationForCircuitBreaker(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260922_2.log.gz")
	upload := &classifiedTestUploader{
		Uploader: &replaytest.Uploader{Err: errors.New("other service busy"), DestinationID: "other-service"},
		category: uploader.FailureCategoryRetryable,
		retry:    retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonServiceTransient},
	}
	now := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}}, 5*time.Minute, &now)
	openReplayTestCircuit(t, worker, &now)
	report, err := worker.RunOnce(context.Background())
	if err != nil || report.File.UploadAttempted || report.File.DeferredReason != DeferredReasonCircuitBreaker || report.File.DeferredUntil.IsZero() {
		t.Fatalf("RunOnce = %+v, %v, want circuit suppression", report, err)
	}
	if want := replayTestFailuresToOpenCircuit(t); upload.CallCount() != want {
		t.Fatalf("uploads = %d, want %d", upload.CallCount(), want)
	}
}

type classifiedTestUploader struct {
	*replaytest.Uploader
	category uploader.FailureCategory
	retry    retryable.RetryDecision
}

func (u *classifiedTestUploader) ClassifyRetry(context.Context, error) retryable.RetryDecision {
	return u.retry
}

func (u *classifiedTestUploader) UploadDestinations(ctx context.Context, request uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	report := u.Uploader.UploadDestinations(ctx, request, ids)
	for index := range report.Results {
		if report.Results[index].Err != nil {
			report.Results[index].FailureCategory = u.category
		}
	}
	return report
}
