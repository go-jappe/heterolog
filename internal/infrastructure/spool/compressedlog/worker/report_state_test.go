package worker

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestWorkerReportRetainsProcessingStateWhenTransitionFails(t *testing.T) {
	permanentErr := &pkgs3.HTTPStatusError{Code: http.StatusForbidden}
	retryableErr := &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}
	for _, tc := range []struct {
		name            string
		targetState     pkgspool.State
		uploadErr       error
		wantErr         error
		noMatch         bool
		openCircuit     bool
		uploadAttempted bool
		uploadSucceeded bool
		failureCategory uploader.FailureCategory
		deferredReason  DeferredReason
	}{
		{
			name: "successful upload cannot move to done", targetState: pkgspool.StateDone,
			uploadAttempted: true, uploadSucceeded: true,
		},
		{
			name: "permanent failure cannot move to dead", targetState: pkgspool.StateDead,
			uploadErr: permanentErr, wantErr: permanentErr, uploadAttempted: true,
			failureCategory: uploader.FailureCategoryPermanent,
		},
		{
			name: "retryable failure cannot return to active", targetState: pkgspool.StateActive,
			uploadErr: retryableErr, wantErr: retryableErr, uploadAttempted: true,
			failureCategory: uploader.FailureCategoryRetryable, deferredReason: DeferredReasonRetryableUploadFailure,
		},
		{
			name: "unmatched route cannot return to active", targetState: pkgspool.StateActive,
			noMatch: true, wantErr: ErrNoMatchingRoute, deferredReason: DeferredReasonRouteUnavailable,
		},
		{
			name: "open circuit cannot return to active", targetState: pkgspool.StateActive,
			openCircuit: true, deferredReason: DeferredReasonCircuitBreaker,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, store := replaytest.NewDestinationStores(t)
			file := replaytest.SaveFile(t, store, "app_20261003_1.log.gz")
			original, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			targetDir, err := store.Root().StateDir(tc.targetState)
			if err != nil {
				t.Fatal(err)
			}
			targetPath := filepath.Join(targetDir, file.FileName)
			collision := []byte("existing destination payload")
			upload := &replaytest.Uploader{Err: tc.uploadErr}
			if tc.noMatch {
				upload.DestinationID = "other-destination"
			}
			claimedUpload := &claimedFileTestUploader{
				Uploader:    upload,
				t:           t,
				claimedPath: filepath.Join(store.Root().ProcessingDir(), file.FileName),
				onClaim: func() {
					// 元のfileがclaimされた後、そのdestinationを確認したりuploadを試みたりする前に
					// 衝突を作ります。
					if err := os.WriteFile(targetPath, collision, 0o640); err != nil {
						t.Fatal(err)
					}
				},
			}
			now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
			worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
				FileName: replaytest.FileNamePattern("app_"),
				Uploader: claimedUpload,
			}}, 5*time.Minute, &now)
			if tc.openCircuit {
				observeReplayTestFailuresUntilOpen(t, worker.circuitBreakers.getOrCreate(upload.DestinationIDs()[0]), &now)
			}

			report, err := worker.RunOnce(context.Background())
			if !errors.Is(err, os.ErrExist) {
				t.Errorf("RunOnce() error = %v, want destination collision", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("RunOnce() error = %v, want original error %v", err, tc.wantErr)
			}
			if !report.File.Claimed || report.File.FileName != file.FileName || report.File.State != pkgspool.StateProcessing {
				t.Errorf("RunOnce() report = %+v, want claimed file retained in processing", report)
			}
			if report.File.UploadAttempted != tc.uploadAttempted || report.File.UploadSucceeded != tc.uploadSucceeded ||
				report.File.FailureCategory != tc.failureCategory || report.File.DeferredReason != tc.deferredReason {
				t.Errorf("RunOnce() report = %+v, want attempted=%t succeeded=%t category=%q deferredReason=%q",
					report, tc.uploadAttempted, tc.uploadSucceeded, tc.failureCategory, tc.deferredReason)
			}
			wantUploads := 0
			if tc.uploadAttempted {
				wantUploads = 1
			}
			if upload.CallCount() != wantUploads {
				t.Errorf("upload calls = %d, want %d", upload.CallCount(), wantUploads)
			}
			if tc.openCircuit {
				if want := now.Add(5 * time.Minute); !report.File.DeferredUntil.Equal(want) {
					t.Errorf("deferred deadline = %v, want %v", report.File.DeferredUntil, want)
				}
			} else if !report.File.DeferredUntil.IsZero() {
				t.Errorf("deferred deadline = %v, want zero", report.File.DeferredUntil)
			}

			processing := replaytest.OnlyFile(t, store, pkgspool.StateProcessing)
			if processing.FileName != file.FileName {
				t.Errorf("processing file = %q, want %q", processing.FileName, file.FileName)
			}
			if got, err := os.ReadFile(processing.Path); err != nil || !bytes.Equal(got, original) {
				t.Errorf("processing contents = %q, %v, want unchanged %q", got, err, original)
			}
			if got, err := os.ReadFile(targetPath); err != nil || !bytes.Equal(got, collision) {
				t.Errorf("destination contents = %q, %v, want unchanged %q", got, err, collision)
			}
		})
	}
}

// claimedFileTestUploaderは、uploaderのdestination確認を使って、claim後に
// filesystem障害を起こします。uploadが延期される場合も含みます。
type claimedFileTestUploader struct {
	Uploader
	t           *testing.T
	claimedPath string
	onClaim     func()
	called      bool
}

func (u *claimedFileTestUploader) DestinationIDs() []objectstorage.DestinationID {
	u.t.Helper()
	if !u.called {
		if _, err := os.Stat(u.claimedPath); err == nil {
			u.called = true
			u.onClaim()
		} else if !errors.Is(err, os.ErrNotExist) {
			u.t.Fatal(err)
		}
	}
	return u.Uploader.DestinationIDs()
}
