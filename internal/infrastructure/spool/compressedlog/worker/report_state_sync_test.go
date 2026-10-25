package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestWorkerReportsMovedStateWhenDirectorySyncFails(t *testing.T) {
	uploadErr := errors.New("upload failed")
	for _, tc := range []struct {
		name       string
		category   uploader.FailureCategory
		wantState  pkgspool.State
		wantReason DeferredReason
	}{
		{name: "successful upload", wantState: pkgspool.StateDone},
		{name: "permanent failure", category: uploader.FailureCategoryPermanent, wantState: pkgspool.StateDead},
		{name: "retryable failure", category: uploader.FailureCategoryRetryable, wantState: pkgspool.StateActive, wantReason: DeferredReasonRetryableUploadFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := replaytest.NewStore(t)
			saved := replaytest.SaveFile(t, store, "app_20261003_1.log.gz")
			upload := &classifiedTestUploader{Uploader: &replaytest.Uploader{}, category: tc.category}
			if tc.category != "" {
				upload.Err = uploadErr
			}
			directory, err := store.Root().StateDir(tc.wantState)
			if err != nil {
				t.Fatal(err)
			}
			var restore func()
			claimedUpload := &claimedFileTestUploader{
				Uploader:    upload,
				t:           t,
				claimedPath: filepath.Join(store.Root().ProcessingDir(), saved.FileName),
				onClaim: func() {
					// claim完了後に同期だけを失敗させ、rename自体は成功させます。
					restore = testutil.DenyDirectorySync(t, directory)
				},
			}
			worker := newReplayTestWorker(t, store, RouteRegistry{{
				FileName: replaytest.FileNamePattern("app_"),
				Uploader: claimedUpload,
			}})
			report, err := worker.RunOnce(context.Background())
			if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
				t.Fatalf("RunOnce() error = %v, want directory sync failure", err)
			}
			if tc.category != "" && !errors.Is(err, uploadErr) {
				t.Fatalf("RunOnce() error = %v, want upload error preserved", err)
			}
			if !report.File.Claimed || report.File.FileName != saved.FileName || report.File.State != tc.wantState {
				t.Fatalf("RunOnce() report = %+v, want claimed file in %s", report, tc.wantState)
			}
			if !report.File.UploadAttempted || report.File.UploadSucceeded != (tc.category == "") || report.File.FailureCategory != tc.category {
				t.Fatalf("RunOnce() report = %+v, want upload result unchanged", report)
			}
			if report.File.DeferredReason != tc.wantReason || upload.CallCount() != 1 {
				t.Fatalf("RunOnce() report/calls = %+v/%d", report, upload.CallCount())
			}
			restore()
			_ = replaytest.OnlyFile(t, store, tc.wantState)
			if _, err := os.Stat(filepath.Join(store.Root().ProcessingDir(), saved.FileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("processing file after successful rename: %v", err)
			}
		})
	}
}

func TestWorkerReportsClaimedFileWhenDirectorySyncFails(t *testing.T) {
	store := replaytest.NewStore(t)
	saved := replaytest.SaveFile(t, store, "app_20261003_1.log.gz")
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})
	restore := testutil.DenyDirectorySync(t, store.Root().ProcessingDir())
	// RunOnceの回復用ディレクトリ走査ではなく、claim後の同期失敗を確認します。
	report, err := worker.processOldestActiveFile(context.Background())
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("processOldestActiveFile() error = %v, want directory sync failure", err)
	}
	if !report.Claimed || report.FileName != saved.FileName || report.State != pkgspool.StateProcessing {
		t.Fatalf("report = %+v, want claimed processing file", report)
	}
	if report.UploadAttempted || report.UploadSucceeded || report.FailureCategory != "" || report.DeferredReason != DeferredReasonNone || upload.CallCount() != 0 {
		t.Fatalf("report/calls = %+v/%d, want no upload", report, upload.CallCount())
	}
	restore()
	_ = replaytest.OnlyFile(t, store, pkgspool.StateProcessing)
	if _, err := os.Stat(saved.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active file after successful claim: %v", err)
	}
}
