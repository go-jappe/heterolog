package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestWorkerCancellationWhileWaitingForClaimLock(t *testing.T) {
	store := replaytest.NewStore(t)
	file := replaytest.SaveFile(t, store, "app_20260916_1.log.gz")
	upload := &replaytest.Uploader{}
	claimMu := &infrafsstore.ContextMutex{}
	worker, err := NewWorker(store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}}, 5*time.Minute, WithClaimLock(claimMu))
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		claimMu.Lock()
		defer claimMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		report, err := worker.RunOnce(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || report.File.Claimed {
			t.Fatalf("waiting worker = %+v, %v", report, err)
		}
	})
	if upload.CallCount() != 0 {
		t.Fatal("canceled worker uploaded a file")
	}
	if remaining := replaytest.OnlyFile(t, store, pkgspool.StateActive); !reflect.DeepEqual(remaining, file) {
		t.Fatalf("canceled claim changed active file: %+v", remaining)
	}
	if report, err := worker.RunOnce(context.Background()); err != nil || report.File.State != pkgspool.StateDone {
		t.Fatalf("retry worker = %+v, %v", report, err)
	}
}
