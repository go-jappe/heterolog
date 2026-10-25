package worker

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// upload先が上限を超えるRetry-Afterを返した場合は、宛先の待機を上限まで短縮し、短縮した内容を
// reportへ残す。上限以下の指定はそのまま待つ。
func TestWorkerCapsDestinationRetryAfterAtConfiguredMaximum(t *testing.T) {
	for _, tt := range []struct {
		name       string
		retryAfter time.Duration
		wantWait   time.Duration
		clamped    bool
	}{
		{name: "above maximum", retryAfter: 3 * time.Hour, wantWait: time.Hour, clamped: true},
		{name: "below maximum", retryAfter: 30 * time.Minute, wantWait: 30 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := replaytest.NewStore(t)
				replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
				upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{
					Code:               http.StatusServiceUnavailable,
					RetryAfterDuration: tt.retryAfter,
				}}
				worker, err := NewWorker(store, RouteRegistry{{
					FileName: replaytest.FileNamePattern("app_"),
					Uploader: upload,
				}}, 5*time.Minute, WithMaxRetryAfter(time.Hour))
				if err != nil {
					t.Fatal(err)
				}

				start := time.Now()
				failed, err := worker.RunOnce(context.Background())
				if err == nil || failed.File.DeferredReason != DeferredReasonRetryableUploadFailure {
					t.Fatalf("throttled RunOnce() report/error = %+v/%v", failed, err)
				}
				wantNotBefore := start.Add(tt.wantWait)
				if !failed.File.DeferredUntil.Equal(wantNotBefore) {
					t.Fatalf("DeferredUntil = %s, want %s", failed.File.DeferredUntil, wantNotBefore)
				}
				clamp := failed.File.RetryAfterClamp
				switch {
				case tt.clamped && (clamp == nil || clamp.Requested != tt.retryAfter || clamp.MaxRetryAfter != time.Hour ||
					!clamp.NotBefore.Equal(wantNotBefore) || clamp.Stored):
					t.Fatalf("RetryAfterClamp = %+v, want requested %s capped at 1h until %s", clamp, tt.retryAfter, wantNotBefore)
				case !tt.clamped && clamp != nil:
					t.Fatalf("RetryAfterClamp = %+v, want nil below the maximum", clamp)
				}
				upload.SetError(nil)

				time.Sleep(tt.wantWait - time.Nanosecond)
				deferred, err := worker.RunOnce(context.Background())
				if err != nil || deferred.File.DeferredReason != DeferredReasonRetryAfter || deferred.File.UploadAttempted ||
					deferred.File.RetryAfterClamp != nil {
					t.Fatalf("RunOnce() before deadline report/error = %+v/%v, want deferred without a new clamp", deferred, err)
				}

				time.Sleep(time.Nanosecond)
				resumed, err := worker.RunOnce(context.Background())
				if err != nil || !resumed.File.UploadSucceeded {
					t.Fatalf("RunOnce() at deadline report/error = %+v/%v, want uploaded", resumed, err)
				}
				if got := upload.CallCount(); got != 2 {
					t.Fatalf("upload calls = %d, want 2", got)
				}
			})
		})
	}
}

func TestNewWorkerRejectsNegativeMaxRetryAfterAndUsesDefaultForZero(t *testing.T) {
	store := replaytest.NewStore(t)
	if _, err := NewWorker(store, nil, 5*time.Minute, WithMaxRetryAfter(-time.Second)); !errors.Is(err, ErrInvalidMaxRetryAfter) {
		t.Fatalf("NewWorker() error = %v, want ErrInvalidMaxRetryAfter", err)
	}
	for _, options := range [][]Option{{WithMaxRetryAfter(0)}, nil} {
		worker, err := NewWorker(store, nil, 5*time.Minute, options...)
		if err != nil {
			t.Fatal(err)
		}
		if got := worker.retryAfter.maxWait; got != time.Hour {
			t.Fatalf("max retry-after = %s, want default 1h", got)
		}
	}
}
