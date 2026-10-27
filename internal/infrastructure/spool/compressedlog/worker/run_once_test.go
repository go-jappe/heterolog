package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestWorkerRunOnceRecoversProcessingBeforeUpload(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260922_1.log.gz")
	processing, claimed, err := store.ClaimOldestActive(context.Background())
	if err != nil || !claimed {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", processing, claimed, err)
	}
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})

	report, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if report.RecoveredProcessingCount != 1 || !report.File.Claimed ||
		report.File.FileName != processing.FileName || report.File.State != pkgspool.StateDone ||
		!report.File.UploadAttempted || !report.File.UploadSucceeded {
		t.Fatalf("RunOnce() report = %+v, want one recovered and uploaded file", report)
	}
	if upload.CallCount() != 1 || upload.LastPath() != processing.Path {
		t.Fatalf("upload calls/path = %d/%q, want 1/%q", upload.CallCount(), upload.LastPath(), processing.Path)
	}
	if done := replaytest.OnlyFile(t, store, pkgspool.StateDone); done.FileName != processing.FileName {
		t.Fatalf("done file = %+v, want %q", done, processing.FileName)
	}
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing} {
		files, err := store.ListFiles(context.Background(), state)
		if err != nil || len(files) != 0 {
			t.Fatalf("ListFiles(%q) = %+v, %v, want no files", state, files, err)
		}
	}
}

func TestWorkerRunOnceDynamicEmptyRoutesLeaveQueuedFilesUntouched(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260922_2.log.gz")
	processing, claimed, err := store.ClaimOldestActive(context.Background())
	if err != nil || !claimed {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", processing, claimed, err)
	}
	active := replaytest.SaveFile(t, store, "app_20260922_3.log.gz")
	oldTime := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, path := range []string{processing.Path, active.Path} {
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
	}
	upload := &replaytest.Uploader{}
	fixedRoutes := RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}}
	var currentRoutes RouteRegistry
	worker, err := NewWorker(store, fixedRoutes, 5*time.Minute, WithRouteSnapshot(func() RouteRegistry {
		return currentRoutes
	}))
	if err != nil {
		t.Fatal(err)
	}

	report, err := worker.RunOnce(context.Background())
	if err != nil || !reflect.DeepEqual(report, RunReport{}) {
		t.Fatalf("RunOnce() = %+v, %v, want empty result while routes are absent", report, err)
	}
	if upload.CallCount() != 0 {
		t.Fatal("worker with no current routes uploaded a file")
	}
	if got := replaytest.OnlyFile(t, store, pkgspool.StateProcessing); !reflect.DeepEqual(got, processing) {
		t.Fatalf("processing file = %+v, want unchanged %+v", got, processing)
	}
	if got := replaytest.OnlyFile(t, store, pkgspool.StateActive); !reflect.DeepEqual(got, active) {
		t.Fatalf("active file = %+v, want unchanged %+v", got, active)
	}
	for _, path := range []string{processing.Path, active.Path} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.ModTime().Equal(oldTime) {
			t.Fatalf("file %q mtime = %v, want unchanged %v", path, info.ModTime(), oldTime)
		}
	}

	currentRoutes = fixedRoutes
	report, err = worker.RunOnce(context.Background())
	if err != nil || report.RecoveredProcessingCount != 1 || !report.File.UploadSucceeded ||
		report.File.FileName != active.FileName || upload.CallCount() != 1 {
		t.Fatalf("RunOnce() after registration = %+v, %v; uploads = %d", report, err, upload.CallCount())
	}
}

func TestWorkerRunOnceRecoveryFailureBlocksUpload(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260922_4.log.gz")
	processing, claimed, err := store.ClaimOldestActive(context.Background())
	if err != nil || !claimed {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", processing, claimed, err)
	}
	// 衝突するactive fileが、platformの権限checkに頼らずにrecoveryを妨げます。
	// 待機中のactive fileもuploadしてはなりません。
	activePath := filepath.Join(store.Root().ActiveDir(), processing.FileName)
	activeBytes := []byte("different compressed log payload")
	if err := os.WriteFile(activePath, activeBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})

	report, err := worker.RunOnce(context.Background())
	if !errors.Is(err, os.ErrExist) || !reflect.DeepEqual(report, RunReport{}) {
		t.Fatalf("RunOnce() = %+v, %v, want recovery conflict and no claimed file", report, err)
	}
	if upload.CallCount() != 0 {
		t.Fatal("worker uploaded a file after recovery failed")
	}
	if got := replaytest.OnlyFile(t, store, pkgspool.StateProcessing); !reflect.DeepEqual(got, processing) {
		t.Fatalf("processing file = %+v, want retained %+v", got, processing)
	}
	got, err := os.ReadFile(activePath)
	if err != nil || string(got) != string(activeBytes) {
		t.Fatalf("conflicting active file = %q, %v, want original contents", got, err)
	}
}

func TestWorkerRunOnceCanceledContextDoesNotRecoverProcessing(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260922_5.log.gz")
	processing, claimed, err := store.ClaimOldestActive(context.Background())
	if err != nil || !claimed {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", processing, claimed, err)
	}
	upload := &replaytest.Uploader{}
	snapshotCalls := 0
	worker, err := NewWorker(store, nil, 5*time.Minute, WithRouteSnapshot(func() RouteRegistry {
		snapshotCalls++
		return RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
		}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report, err := worker.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(report, RunReport{}) {
		t.Fatalf("RunOnce() = %+v, %v, want cancellation without file changes", report, err)
	}
	if snapshotCalls != 0 || upload.CallCount() != 0 {
		t.Fatalf("snapshot/upload calls = %d/%d, want 0/0", snapshotCalls, upload.CallCount())
	}
	if got := replaytest.OnlyFile(t, store, pkgspool.StateProcessing); !reflect.DeepEqual(got, processing) {
		t.Fatalf("processing file = %+v, want unchanged %+v", got, processing)
	}
}

func TestWorkerRunOnceEmptySpoolHasNoFileResult(t *testing.T) {
	store := replaytest.NewStore(t)
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})

	report, err := worker.RunOnce(context.Background())
	if err != nil || !reflect.DeepEqual(report, RunReport{}) {
		t.Fatalf("RunOnce() = %+v, %v, want no recovery or claimed file", report, err)
	}
	if upload.CallCount() != 0 {
		t.Fatal("worker with an empty spool attempted an upload")
	}
}
