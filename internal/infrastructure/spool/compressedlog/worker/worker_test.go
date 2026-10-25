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

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// hangDetectionTimeoutは、背景のgoroutineの進行を実時間で待つ試験が、処理が止まったと判断するまでの上限です。
const hangDetectionTimeout = 10 * time.Second

func TestWorkerSelectsExactlyMatchingFilenameRoute(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "error_app_20260906_1.log.gz")
	defaultUpload := &replaytest.Uploader{}
	errorUpload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{
		{FileName: replaytest.FileNamePattern("app_"), Uploader: defaultUpload},
		{FileName: replaytest.FileNamePattern("error_app_"), Uploader: errorUpload},
	})

	report, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("Worker.RunOnce() error = %v", err)
	}
	if report.File.State != pkgspool.StateDone || !report.File.UploadSucceeded || report.File.DeferredReason != DeferredReasonNone {
		t.Fatalf("Worker.RunOnce() report = %+v, want uploaded done", report)
	}
	if defaultUpload.CallCount() != 0 || errorUpload.CallCount() != 1 {
		t.Fatalf("upload calls default=%d error=%d, want 0/1", defaultUpload.CallCount(), errorUpload.CallCount())
	}
	if got := filepath.Base(errorUpload.LastPath()); got != "error_app_20260906_1.log.gz" {
		t.Fatalf("uploaded basename = %q", got)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
}

func TestWorkerNoMatchingRouteReturnsFileToActive(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	worker := newReplayTestWorker(t, store, nil)

	report, err := worker.RunOnce(context.Background())
	if !errors.Is(err, ErrNoMatchingRoute) {
		t.Fatalf("Worker.RunOnce() error = %v, want ErrNoMatchingRoute", err)
	}
	if report.File.State != pkgspool.StateActive || report.File.DeferredReason != DeferredReasonRouteUnavailable || !report.File.DeferredUntil.IsZero() {
		t.Fatalf("Worker.RunOnce() report = %+v, want indefinitely deferred active", report)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateActive)
}

func TestWorkerResendsCustomFormatWithoutCodec(t *testing.T) {
	store := replaytest.NewStore(t)
	sourcePath := filepath.Join(t.TempDir(), "app_20260925_1.log.custom")
	// 保存・再送はcodecの実装を必要とせず、独自形式のバイト列を変更しません。
	payload := []byte{0x00, 0xfe, 0x81, 0x13, 0x37, 0xff}
	if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveRetryableFailure(context.Background(), sourcePath); err != nil {
		t.Fatal(err)
	}
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})

	report, err := worker.RunOnce(context.Background())
	if err != nil || !report.File.UploadSucceeded || report.File.State != pkgspool.StateDone {
		t.Fatalf("RunOnce() = %+v, %v, want uploaded done", report, err)
	}
	if upload.CallCount() != 1 || filepath.Base(upload.LastPath()) != filepath.Base(sourcePath) {
		t.Fatalf("upload calls/path = %d/%q", upload.CallCount(), upload.LastPath())
	}
	doneFile := replaytest.OnlyFile(t, store, pkgspool.StateDone)
	stored, err := os.ReadFile(doneFile.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, payload) {
		t.Fatalf("stored bytes = %x, want %x", stored, payload)
	}
}

func TestWorkerAmbiguousFileNameRoutesFailClosedInActive(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	first := &replaytest.Uploader{}
	second := &replaytest.Uploader{DestinationID: "other-s3"}
	fileName := replaytest.FileNamePattern("app_")
	worker := newReplayTestWorker(t, store, RouteRegistry{
		{FileName: fileName, Uploader: first},
		{FileName: fileName, Uploader: second},
	})

	report, err := worker.RunOnce(context.Background())
	if !errors.Is(err, ErrAmbiguousRoute) {
		t.Fatalf("Worker.RunOnce() error = %v, want ErrAmbiguousRoute", err)
	}
	if report.File.State != pkgspool.StateActive || report.File.DeferredReason != DeferredReasonRouteUnavailable {
		t.Fatalf("Worker.RunOnce() report = %+v, want deferred active", report)
	}
	if first.CallCount() != 0 || second.CallCount() != 0 {
		t.Fatal("ambiguous route performed an upload")
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateActive)
}

func TestWorkerDuplicateOwnersForSameDestinationAreOneRoute(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	first := &replaytest.Uploader{}
	second := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, RouteRegistry{
		{FileName: replaytest.FileNamePattern("app_"), Uploader: first},
		{FileName: replaytest.FileNamePattern("app_"), Uploader: second},
	})

	report, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("Worker.RunOnce() error = %v", err)
	}
	if report.File.State != pkgspool.StateDone || first.CallCount()+second.CallCount() != 1 {
		t.Fatalf("Worker.RunOnce() report/calls = %+v/%d", report, first.CallCount()+second.CallCount())
	}
}

func TestWorkerMatchesOnlyCanonicalCompressedFileNamesInAnyDirectory(t *testing.T) {
	upload := &replaytest.Uploader{}
	fileName := logpattern.FileNamePattern{
		Dir: t.TempDir(), Target: "app", Date: "20261020", Index: 99, Extension: "log",
	}
	worker := newReplayTestWorker(t, replaytest.NewStore(t), RouteRegistry{{
		FileName: fileName,
		Uploader: upload,
	}})
	spoolDir := t.TempDir()
	for _, tc := range []struct {
		name      string
		wantMatch bool
	}{
		{name: "app_20260906_1.log.gz", wantMatch: true},
		{name: "app_20260906_2.log.custom", wantMatch: true},
		{name: "app_20260906_1.log"},
		{name: "app_20260906_01.log.gz"},
		{name: "app_20260906_0.log.gz"},
		{name: "app_20260906_-1.log.gz"},
		{name: "app_20260230_1.log.gz"},
		{name: "app_20260906_extra_1.log.gz"},
		{name: "app_20260906_1.log.gz.tmp"},
		{name: "app_20260906_1.other.gz"},
		{name: "error_app_20260906_1.log.gz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matched, destination, err := worker.matchingRoutes(filepath.Join(spoolDir, tc.name))
			if err != nil || (len(matched) == 1) != tc.wantMatch {
				t.Fatalf("matchingRoutes() = %d matches, %v; want match=%t", len(matched), err, tc.wantMatch)
			}
			if tc.wantMatch && destination != "s3" {
				t.Fatalf("matchingRoutes() destination = %q, want s3", destination)
			}
		})
	}
}

func TestWorkerCircuitBreakerUsesBackendThresholdsAndFiveMinuteCooldown(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}}
	routes := RouteRegistry{{FileName: replaytest.FileNamePattern("app_"), Uploader: upload}}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, routes, 5*time.Minute, &now)
	failuresToOpen := replayTestFailuresToOpenCircuit(t)

	openReplayTestCircuit(t, worker, &now)
	if got := upload.CallCount(); got != failuresToOpen {
		t.Fatalf("upload calls before open = %d, want %d", got, failuresToOpen)
	}

	suppressed, err := worker.RunOnce(context.Background())
	if err != nil || suppressed.File.UploadAttempted || suppressed.File.DeferredReason != DeferredReasonCircuitBreaker {
		t.Fatalf("open RunOnce() report/error = %+v/%v", suppressed, err)
	}
	wantOpenUntil := now.Add(5 * time.Minute)
	if !suppressed.File.DeferredUntil.Equal(wantOpenUntil) {
		t.Fatalf("open deadline = %s, want %s", suppressed.File.DeferredUntil, wantOpenUntil)
	}
	if got := upload.CallCount(); got != failuresToOpen {
		t.Fatalf("open breaker upload calls = %d, want %d", got, failuresToOpen)
	}

	now = wantOpenUntil
	upload.SetError(nil)
	probe, err := worker.RunOnce(context.Background())
	if err != nil || !probe.File.UploadAttempted || !probe.File.UploadSucceeded || probe.File.State != pkgspool.StateDone {
		t.Fatalf("probe RunOnce() report/error = %+v/%v", probe, err)
	}
	if got := upload.CallCount(); got != failuresToOpen+1 {
		t.Fatalf("upload calls after successful probe = %d, want %d", got, failuresToOpen+1)
	}
}

func TestWorkerFailedProbeReopensForFiveMinutes(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}}, 5*time.Minute, &now)
	openReplayTestCircuit(t, worker, &now)

	suppressed, err := worker.RunOnce(context.Background())
	if err != nil || suppressed.File.DeferredUntil.IsZero() {
		t.Fatalf("open RunOnce() report/error = %+v/%v", suppressed, err)
	}
	now = suppressed.File.DeferredUntil
	failedProbe, err := worker.RunOnce(context.Background())
	if err == nil || !failedProbe.File.UploadAttempted || failedProbe.File.State != pkgspool.StateActive || failedProbe.File.DeferredReason != DeferredReasonRetryableUploadFailure {
		t.Fatalf("failed probe report/error = %+v/%v", failedProbe, err)
	}

	reopened, err := worker.RunOnce(context.Background())
	if err != nil || reopened.File.UploadAttempted || reopened.File.DeferredReason != DeferredReasonCircuitBreaker {
		t.Fatalf("reopened RunOnce() report/error = %+v/%v", reopened, err)
	}
	if want := now.Add(5 * time.Minute); !reopened.File.DeferredUntil.Equal(want) {
		t.Fatalf("reopened deadline = %s, want %s", reopened.File.DeferredUntil, want)
	}
}

func TestWorkerUploaderPanicDuringProbeReleasesProbeLease(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}}, 5*time.Minute, &now)
	openReplayTestCircuit(t, worker, &now)
	suppressed, err := worker.RunOnce(context.Background())
	if err != nil || suppressed.File.DeferredUntil.IsZero() {
		t.Fatalf("open RunOnce() report/error = %+v/%v", suppressed, err)
	}

	now = suppressed.File.DeferredUntil
	upload.SetPanic("uploader bug")
	panicReport, err := worker.RunOnce(context.Background())
	if !errors.Is(err, ErrRouteUploaderPanic) || panicReport.File.State != pkgspool.StateActive {
		t.Fatalf("panic probe report/error = %+v/%v", panicReport, err)
	}

	// panicは復旧の証拠ではないため、breakerを閉じずにprobe枠だけを解放します。
	// 直後の送信は新しいprobeとなり、一時的な失敗ですぐにcooldownへ戻ります。
	upload.SetPanic(nil)
	reopened, err := worker.RunOnce(context.Background())
	if err == nil || !reopened.File.UploadAttempted || reopened.File.DeferredUntil.IsZero() {
		t.Fatalf("post-panic failed probe report/error = %+v/%v, want immediate reopen", reopened, err)
	}
	if want := now.Add(5 * time.Minute); !reopened.File.DeferredUntil.Equal(want) {
		t.Fatalf("post-panic reopened deadline = %s, want %s", reopened.File.DeferredUntil, want)
	}

	now = reopened.File.DeferredUntil
	upload.SetError(nil)
	recovered, err := worker.RunOnce(context.Background())
	if err != nil || !recovered.File.UploadSucceeded || recovered.File.State != pkgspool.StateDone {
		t.Fatalf("post-panic RunOnce() report/error = %+v/%v", recovered, err)
	}
}

func TestWorkerProbeWithoutBackendResponseKeepsCircuitHalfOpen(t *testing.T) {
	for _, tc := range []struct {
		name string
		// probeは、cooldown後の一件目の送信を実行し、その結果を返します。
		probe func(t *testing.T, worker *Worker, upload *replaytest.Uploader) (RunReport, error)
	}{
		{
			name: "caller canceled during upload",
			probe: func(t *testing.T, worker *Worker, upload *replaytest.Uploader) (RunReport, error) {
				ready := make(chan struct{}, 1)
				block := make(chan struct{})
				upload.Ready, upload.Block = ready, block
				defer func() { upload.Ready, upload.Block = nil, nil }()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type result struct {
					report RunReport
					err    error
				}
				done := make(chan result, 1)
				go func() {
					report, err := worker.RunOnce(ctx)
					done <- result{report: report, err: err}
				}()
				select {
				case <-ready:
				case <-time.After(hangDetectionTimeout):
					t.Fatal("probe upload did not start")
				}
				cancel()
				select {
				case got := <-done:
					return got.report, got.err
				case <-time.After(hangDetectionTimeout):
					t.Fatal("canceled probe did not finish")
					return RunReport{}, nil
				}
			},
		},
		{
			name: "local error without response",
			probe: func(t *testing.T, worker *Worker, upload *replaytest.Uploader) (RunReport, error) {
				upload.SetError(errors.New("read compressed log file"))
				return worker.RunOnce(context.Background())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := replaytest.NewStore(t)
			replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
			unavailable := &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}
			upload := &replaytest.Uploader{Err: unavailable}
			now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
				FileName: replaytest.FileNamePattern("app_"),
				Uploader: upload,
			}}, 5*time.Minute, &now)
			openReplayTestCircuit(t, worker, &now)
			suppressed, err := worker.RunOnce(context.Background())
			if err != nil || suppressed.File.DeferredUntil.IsZero() {
				t.Fatalf("open RunOnce() report/error = %+v/%v", suppressed, err)
			}

			now = suppressed.File.DeferredUntil
			probe, err := tc.probe(t, worker, upload)
			if err == nil || !probe.File.UploadAttempted || probe.File.State != pkgspool.StateActive || !probe.File.DeferredUntil.IsZero() {
				t.Fatalf("probe report/error = %+v/%v, want retained file without a new cooldown", probe, err)
			}

			// 結果を確認できなかったprobeではbreakerを閉じないため、次の送信が改めてprobeになり、
			// 一時的な失敗ですぐにcooldownへ戻ります。closedなら通常の一回目の失敗としてopenしません。
			upload.SetError(unavailable)
			reopened, err := worker.RunOnce(context.Background())
			if err == nil || !reopened.File.UploadAttempted {
				t.Fatalf("next probe report/error = %+v/%v", reopened, err)
			}
			if want := now.Add(5 * time.Minute); !reopened.File.DeferredUntil.Equal(want) {
				t.Fatalf("next probe deadline = %s, want immediate reopen until %s", reopened.File.DeferredUntil, want)
			}
		})
	}
}

func TestWorkerProbeRejectedBySendDestinationClosesCircuit(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	replaytest.SaveFile(t, store, "app_20260906_2.log.gz")
	unavailable := &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}
	upload := &replaytest.Uploader{Err: unavailable}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}}, 5*time.Minute, &now)
	openReplayTestCircuit(t, worker, &now)
	suppressed, err := worker.RunOnce(context.Background())
	if err != nil || suppressed.File.DeferredUntil.IsZero() {
		t.Fatalf("open RunOnce() report/error = %+v/%v", suppressed, err)
	}

	// 4xxは再送しませんが、送信先が応答できる状態に戻ったことを示します。
	now = suppressed.File.DeferredUntil
	upload.SetError(&pkgs3.HTTPStatusError{Code: http.StatusForbidden})
	rejected, err := worker.RunOnce(context.Background())
	if err == nil || rejected.File.State != pkgspool.StateDead || !rejected.File.DeferredUntil.IsZero() {
		t.Fatalf("rejected probe report/error = %+v/%v, want dead file without cooldown", rejected, err)
	}

	// breakerはclosedへ戻るため、次の一時的な失敗は新しい連続失敗の一回目でopenしません。
	upload.SetError(unavailable)
	next, err := worker.RunOnce(context.Background())
	if err == nil || !next.File.UploadAttempted || !next.File.DeferredUntil.IsZero() {
		t.Fatalf("failure after rejected probe report/error = %+v/%v, want closed breaker", next, err)
	}
}

func TestWorkerRetryAfterDefersSameDestinationUntilDeadline(t *testing.T) {
	store := replaytest.NewStore(t)
	first := replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	second := replaytest.SaveFile(t, store, "app_20260906_2.log.gz")
	oldest := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	setReplayTestQueueTime(t, first.Path, oldest)
	setReplayTestQueueTime(t, second.Path, oldest.Add(time.Minute))
	upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{
		Code:               http.StatusServiceUnavailable,
		RetryAfterDuration: time.Hour,
	}}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}}, 5*time.Minute, &now)

	failed, err := worker.RunOnce(context.Background())
	if err == nil || failed.File.FileName != first.FileName || failed.File.DeferredReason != DeferredReasonRetryableUploadFailure {
		t.Fatalf("throttled RunOnce() report/error = %+v/%v", failed, err)
	}
	notBefore := now.Add(time.Hour)
	if !failed.File.DeferredUntil.Equal(notBefore) {
		t.Fatalf("throttled deadline = %s, want Retry-After deadline %s", failed.File.DeferredUntil, notBefore)
	}
	upload.SetError(nil)

	// 待機中は同じ宛先の別ファイルも送信せず、activeへ戻します。
	for _, at := range []time.Time{now, notBefore.Add(-time.Nanosecond)} {
		now = at
		deferred, err := worker.RunOnce(context.Background())
		if err != nil || deferred.File.FileName != second.FileName || deferred.File.UploadAttempted ||
			deferred.File.DeferredReason != DeferredReasonRetryAfter || !deferred.File.DeferredUntil.Equal(notBefore) ||
			deferred.File.State != pkgspool.StateActive {
			t.Fatalf("RunOnce() at %s report/error = %+v/%v, want deferred until %s", at, deferred, err, notBefore)
		}
		if got := upload.CallCount(); got != 1 {
			t.Fatalf("upload calls during Retry-After = %d, want 1", got)
		}
		// 保留したファイルも待機列の末尾へ戻るため、次は先に失敗したファイルが選ばれます。
		first, second = second, first
	}

	now = notBefore
	resumed, err := worker.RunOnce(context.Background())
	if err != nil || !resumed.File.UploadSucceeded || resumed.File.FileName != second.FileName {
		t.Fatalf("RunOnce() after Retry-After report/error = %+v/%v, want %q uploaded", resumed, err, second.FileName)
	}
	if got := upload.CallCount(); got != 2 {
		t.Fatalf("upload calls after Retry-After = %d, want 2", got)
	}
}

func TestWorkerRetryAfterDoesNotShortenCircuitBreakerCooldown(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable}}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker := newReplayTestWorkerWithCircuitClock(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}}, 5*time.Minute, &now)
	failures := replayTestFailuresToOpenCircuit(t)
	for attempt := 1; attempt < failures; attempt++ {
		if _, err := worker.RunOnce(context.Background()); err == nil {
			t.Fatalf("failure %d error = nil", attempt)
		}
		now = now.Add(replayTestFailureInterval)
	}

	// circuit breakerを開く失敗が短いRetry-Afterを返しても、保留期限は遅い方です。
	upload.SetError(&pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable, RetryAfterDuration: time.Minute})
	opened, err := worker.RunOnce(context.Background())
	if err == nil || !opened.File.DeferredUntil.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("opening RunOnce() report/error = %+v/%v, want circuit cooldown deadline", opened, err)
	}
}

func TestDestinationWorkerRetryAfterDoesNotBlockOtherDestinations(t *testing.T) {
	stores, err := objectstoragespool.NewDestinationStores(replaytest.NewStore(t).Root())
	if err != nil {
		t.Fatal(err)
	}
	throttledStore, err := stores.ForDestination("s3-a")
	if err != nil {
		t.Fatal(err)
	}
	otherStore, err := stores.ForDestination("s3-b")
	if err != nil {
		t.Fatal(err)
	}
	throttledFirst := replaytest.SaveFile(t, throttledStore, "app_20260906_1.log.gz")
	throttledSecond := replaytest.SaveFile(t, throttledStore, "app_20260906_2.log.gz")
	otherFirst := replaytest.SaveFile(t, otherStore, "app_20260906_3.log.gz")
	otherSecond := replaytest.SaveFile(t, otherStore, "app_20260906_4.log.gz")
	oldest := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	for index, file := range []objectstoragespool.CompressedLogFile{throttledFirst, throttledSecond, otherFirst, otherSecond} {
		setReplayTestQueueTime(t, file.Path, oldest.Add(time.Duration(index)*time.Minute))
	}
	throttled := &replaytest.Uploader{DestinationID: "s3-a", Err: &pkgs3.HTTPStatusError{
		Code:               http.StatusTooManyRequests,
		RetryAfterDuration: time.Hour,
	}}
	other := &replaytest.Uploader{DestinationID: "s3-b"}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	worker, err := NewDestinationWorker(stores, RouteRegistry{
		{FileName: replaytest.FileNamePattern("app_"), Uploader: throttled},
		{FileName: replaytest.FileNamePattern("app_"), Uploader: other},
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	worker.retryAfter = newDestinationRetryAfterRegistry(func() time.Time { return now })

	failed, err := worker.RunOnce(context.Background())
	if err == nil || failed.File.DestinationID != "s3-a" || failed.File.FileName != throttledFirst.FileName ||
		!failed.File.DeferredUntil.Equal(now.Add(time.Hour)) {
		t.Fatalf("throttled RunOnce() report/error = %+v/%v", failed, err)
	}
	throttled.SetError(nil)
	waitingInfo := make(map[string]os.FileInfo)
	for _, file := range []objectstoragespool.CompressedLogFile{throttledFirst, throttledSecond} {
		info, err := os.Stat(filepath.Join(throttledStore.Root().ActiveDir(), file.FileName))
		if err != nil {
			t.Fatal(err)
		}
		waitingInfo[file.FileName] = info
	}

	// 待機中の宛先はファイルを取得せずに飛ばし、同じ周期で他の宛先を送信します。
	for _, want := range []string{otherFirst.FileName, otherSecond.FileName} {
		report, err := worker.RunOnce(context.Background())
		if err != nil || report.File.DestinationID != "s3-b" || report.File.FileName != want || !report.File.UploadSucceeded {
			t.Fatalf("RunOnce() during Retry-After report/error = %+v/%v, want %q uploaded to s3-b", report, err, want)
		}
	}
	idle, err := worker.RunOnce(context.Background())
	if err != nil || idle.File.Claimed {
		t.Fatalf("RunOnce() with only waiting files report/error = %+v/%v, want no claim", idle, err)
	}
	if got := throttled.CallCount(); got != 1 {
		t.Fatalf("throttled destination upload calls = %d, want 1", got)
	}
	for name, before := range waitingInfo {
		after, err := os.Stat(filepath.Join(throttledStore.Root().ActiveDir(), name))
		if err != nil {
			t.Fatalf("waiting file %q left active: %v", name, err)
		}
		if !after.ModTime().Equal(before.ModTime()) {
			t.Fatalf("waiting file %q was requeued during Retry-After", name)
		}
	}

	now = now.Add(time.Hour)
	for _, want := range []string{throttledSecond.FileName, throttledFirst.FileName} {
		report, err := worker.RunOnce(context.Background())
		if err != nil || report.File.DestinationID != "s3-a" || report.File.FileName != want || !report.File.UploadSucceeded {
			t.Fatalf("RunOnce() after Retry-After report/error = %+v/%v, want %q uploaded to s3-a", report, err, want)
		}
	}
}

// setReplayTestQueueTimeは、spool内の待機順を決める更新時刻を設定します。
func setReplayTestQueueTime(t *testing.T, path string, queuedAt time.Time) {
	t.Helper()
	if err := os.Chtimes(path, queuedAt, queuedAt); err != nil {
		t.Fatal(err)
	}
}

func TestWorkersShareCircuitStateByDestinationAndSeparateOtherDestinations(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	registry, err := newDestinationCircuitBreakerRegistry(
		5*time.Minute,
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	destinationA := objectstorage.DestinationID("s3-a")
	destinationB := objectstorage.DestinationID("s3-b")
	observeReplayTestFailuresUntilOpen(t, registry.getOrCreate(destinationA), &now)

	storeA := replaytest.NewStore(t)
	replaytest.SaveFile(t, storeA, "app_20260906_1.log.gz")
	uploadA := &replaytest.Uploader{DestinationID: destinationA}
	workerA := newReplayTestWorker(t, storeA, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: uploadA,
	}})
	workerA.circuitBreakers = registry
	reportA, err := workerA.RunOnce(context.Background())
	if err != nil || reportA.File.UploadAttempted || reportA.File.DeferredReason != DeferredReasonCircuitBreaker {
		t.Fatalf("destination A report/error = %+v/%v, want circuit suppression", reportA, err)
	}
	if uploadA.CallCount() != 0 {
		t.Fatal("destination A uploader called while its shared circuit was open")
	}

	storeB := replaytest.NewStore(t)
	replaytest.SaveFile(t, storeB, "app_20260906_2.log.gz")
	uploadB := &replaytest.Uploader{DestinationID: destinationB}
	workerB := newReplayTestWorker(t, storeB, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: uploadB,
	}})
	workerB.circuitBreakers = registry
	reportB, err := workerB.RunOnce(context.Background())
	if err != nil || !reportB.File.UploadSucceeded || reportB.File.State != pkgspool.StateDone {
		t.Fatalf("destination B report/error = %+v/%v, want independent success", reportB, err)
	}
	if uploadB.CallCount() != 1 {
		t.Fatalf("destination B upload calls = %d, want 1", uploadB.CallCount())
	}
}

func TestWorkerPermanentFailureMovesFileToDead(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "debug_app_20260906_1.log.gz")
	upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: http.StatusForbidden}}
	worker := newReplayTestWorker(t, store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("debug_app_"),
		Uploader: upload,
	}})

	report, err := worker.RunOnce(context.Background())
	if err == nil {
		t.Fatal("Worker.RunOnce() error = nil, want permanent upload error")
	}
	if report.File.State != pkgspool.StateDead || report.File.FailureCategory != uploader.FailureCategoryPermanent || report.File.DeferredReason != DeferredReasonNone {
		t.Fatalf("Worker.RunOnce() report = %+v, want permanent dead", report)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDead)
}

func TestWorkerConstructorValidation(t *testing.T) {
	store := replaytest.NewStore(t)
	if _, err := NewWorker(nil, nil, 5*time.Minute); !errors.Is(err, ErrNilStore) {
		t.Fatalf("NewWorker(nil) error = %v", err)
	}
	if _, err := NewWorker(store, nil, 0); !errors.Is(err, ErrInvalidCircuitBreakerOpenCooldown) {
		t.Fatalf("non-positive cooldown error = %v", err)
	}
	for _, tc := range []struct {
		name     string
		fileName logpattern.FileNamePattern
	}{
		{name: "empty"},
		{name: "missing target", fileName: logpattern.FileNamePattern{Extension: "log"}},
		{name: "blank target", fileName: logpattern.FileNamePattern{Target: " \t\u3000", Extension: "log"}},
		{name: "missing extension", fileName: logpattern.FileNamePattern{Target: "app"}},
		{name: "blank extension", fileName: logpattern.FileNamePattern{Target: "app", Extension: " \t\u3000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateRouteFileName(tc.fileName); !errors.Is(err, ErrInvalidRouteFileName) {
				t.Fatalf("ValidateRouteFileName() error = %v, want ErrInvalidRouteFileName", err)
			}
			if _, err := NewWorker(store, RouteRegistry{{FileName: tc.fileName, Uploader: &replaytest.Uploader{}}}, 5*time.Minute); !errors.Is(err, ErrInvalidRouteFileName) {
				t.Fatalf("NewWorker() error = %v, want ErrInvalidRouteFileName", err)
			}
		})
	}
	if _, err := NewWorker(store, RouteRegistry{{FileName: replaytest.FileNamePattern("app_")}}, 5*time.Minute); !errors.Is(err, ErrNilRouteUploader) {
		t.Fatalf("nil uploader error = %v", err)
	}
}

func TestWorkerRejectsInvalidUploaderDestinations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		destinations []objectstorage.DestinationID
	}{
		{name: "none"},
		{name: "empty", destinations: []objectstorage.DestinationID{""}},
		{name: "whitespace", destinations: []objectstorage.DestinationID{" \t "}},
		{name: "multiple", destinations: []objectstorage.DestinationID{"primary", "backup"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := replaytest.NewStore(t)
			upload := &destinationListTestUploader{
				Uploader:     &replaytest.Uploader{},
				destinations: tc.destinations,
			}
			routes := RouteRegistry{{FileName: replaytest.FileNamePattern("app_"), Uploader: upload}}
			if _, err := NewWorker(store, routes, time.Minute); !errors.Is(err, ErrInvalidRouteDestination) {
				t.Fatalf("NewWorker() error = %v, want ErrInvalidRouteDestination", err)
			}

			// 動的な一覧でも、不正な宛先への送信を止め、取得したファイルを残します。
			replaytest.SaveFile(t, store, "app_20260929_1.log.gz")
			worker, err := NewWorker(store, nil, time.Minute, WithRouteSnapshot(func() RouteRegistry { return routes }))
			if err != nil {
				t.Fatal(err)
			}
			report, err := worker.RunOnce(context.Background())
			if !errors.Is(err, ErrInvalidRouteDestination) || report.File.State != pkgspool.StateActive || report.File.DeferredReason != DeferredReasonRouteUnavailable || upload.CallCount() != 0 {
				t.Fatalf("RunOnce() = %+v, %v, upload calls = %d; want retained active without upload", report, err, upload.CallCount())
			}
			_ = replaytest.OnlyFile(t, store, pkgspool.StateActive)
		})
	}
}

type destinationListTestUploader struct {
	*replaytest.Uploader
	destinations []objectstorage.DestinationID
}

func (u *destinationListTestUploader) DestinationIDs() []objectstorage.DestinationID {
	return u.destinations
}

func newReplayTestWorker(
	t *testing.T,
	store *objectstoragespool.Store,
	routes RouteRegistry,
) *Worker {
	t.Helper()
	worker, err := NewWorker(store, routes, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func newReplayTestWorkerWithCircuitClock(
	t *testing.T,
	store *objectstoragespool.Store,
	routes RouteRegistry,
	openCooldown time.Duration,
	now *time.Time,
) *Worker {
	t.Helper()
	worker, err := NewWorker(store, routes, openCooldown)
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return *now }
	registry, err := newDestinationCircuitBreakerRegistry(openCooldown, clock)
	if err != nil {
		t.Fatal(err)
	}
	worker.circuitBreakers = registry
	worker.retryAfter = newDestinationRetryAfterRegistry(clock)
	return worker
}

// replayTestFailureIntervalは、試験で送信失敗を繰り返す間隔です。宛先別circuit breakerは
// backend送信用の既定値を使うため、連続失敗とみなす最大間隔以内に収めます。
const replayTestFailureInterval = 5 * time.Second

// replayTestFailuresToOpenCircuitは、replayTestFailureInterval間隔の失敗で宛先別circuit breakerが
// openするまでの失敗回数を、既定の閾値から求めます。最初の失敗から
// DefaultCircuitBreakerMinFailureDurationToOpen以上経過し、かつ
// DefaultCircuitBreakerMinConsecutiveFailuresToOpen回以上連続した失敗でopenします。
func replayTestFailuresToOpenCircuit(t *testing.T) int {
	t.Helper()
	if replayTestFailureInterval > retryable.DefaultCircuitBreakerMaxFailureIntervalForConsecutive {
		t.Fatalf(
			"failure interval %s exceeds the consecutive-failure interval %s",
			replayTestFailureInterval,
			retryable.DefaultCircuitBreakerMaxFailureIntervalForConsecutive,
		)
	}
	intervalsToOpen := (retryable.DefaultCircuitBreakerMinFailureDurationToOpen + replayTestFailureInterval - 1) /
		replayTestFailureInterval
	return max(int(intervalsToOpen)+1, retryable.DefaultCircuitBreakerMinConsecutiveFailuresToOpen)
}

// openReplayTestCircuitは、productionと同じ既定の閾値に従い、同じ失敗を
// replayTestFailureInterval間隔で繰り返してcircuit breakerをopenします。
// nowは最後の失敗時刻で止めます。
func openReplayTestCircuit(t *testing.T, worker *Worker, now *time.Time) {
	t.Helper()
	failures := replayTestFailuresToOpenCircuit(t)
	for attempt := 0; attempt < failures; attempt++ {
		report, err := worker.RunOnce(context.Background())
		if err == nil || !report.File.UploadAttempted || report.File.FailureCategory != uploader.FailureCategoryRetryable || report.File.DeferredReason != DeferredReasonRetryableUploadFailure {
			t.Fatalf("failure %d report/error = %+v/%v", attempt+1, report, err)
		}
		if opened := !report.File.DeferredUntil.IsZero(); opened != (attempt == failures-1) {
			t.Fatalf("failure %d/%d opened circuit = %t, report = %+v", attempt+1, failures, opened, report)
		}
		if attempt < failures-1 {
			*now = now.Add(replayTestFailureInterval)
		}
	}
}

// observeReplayTestFailuresUntilOpenは、BeforeRequestで許可された通常送信の失敗を
// productionと同じRecordRequestFailureで記録し、既定の閾値でbreakerをopenします。
func observeReplayTestFailuresUntilOpen(
	t *testing.T,
	breaker *retryable.CircuitBreaker,
	now *time.Time,
) {
	t.Helper()
	decision := retryable.RetryDecision{
		Retry:  true,
		Reason: retryable.RetryReasonHTTPStatus,
		Code:   http.StatusServiceUnavailable,
	}
	key := retryable.CircuitBreakerKey{
		Reason: decision.Reason,
		Code:   decision.Code,
	}
	failures := replayTestFailuresToOpenCircuit(t)
	for attempt := 0; attempt < failures; attempt++ {
		request := breaker.BeforeRequest()
		if !request.AllowRequest || request.Probe {
			t.Fatalf("failure %d request decision = %+v, want normal request", attempt+1, request)
		}
		report := breaker.RecordRequestFailure(key, decision, request)
		if attempt < failures-1 {
			if !report.OpenUntil.IsZero() {
				t.Fatalf("failure %d opened circuit too early: %+v", attempt+1, report)
			}
			*now = now.Add(replayTestFailureInterval)
			continue
		}
		if report.OpenUntil.IsZero() {
			t.Fatalf("failure %d did not open circuit: %+v", attempt+1, report)
		}
	}
}
