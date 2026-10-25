package compressedlog

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestRunnerRequeuesUnmatchedOldestSoNextCycleCanProcessLaterRoute(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	replaytest.SaveFile(t, store, "error_app_20260906_1.log.gz")
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("error_app_"),
		Uploader: upload,
	}})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	firstReport, err := runner.RunOnce(context.Background())
	if !errors.Is(err, compressedlogworker.ErrNoMatchingRoute) {
		t.Fatalf("first Runner.RunOnce() error = %v, want compressedlogworker.ErrNoMatchingRoute", err)
	}
	if len(firstReport.CompressedLogFiles) != 1 || firstReport.CompressedLogFiles[0].FileName != "app_20260906_1.log.gz" {
		t.Fatalf("first Runner.RunOnce() report = %+v, want only oldest unmatched file", firstReport)
	}
	if got := upload.CallCount(); got != 0 {
		t.Fatalf("first-cycle upload call count = %d, want 0", got)
	}

	secondReport, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second Runner.RunOnce() error = %v", err)
	}
	if len(secondReport.CompressedLogFiles) != 1 || secondReport.CompressedLogFiles[0].FileName != "error_app_20260906_1.log.gz" {
		t.Fatalf("second Runner.RunOnce() report = %+v, want later matching file", secondReport)
	}
	if got := upload.CallCount(); got != 1 {
		t.Fatalf("second-cycle upload call count = %d, want 1", got)
	}
	if active := replaytest.OnlyFile(t, store, pkgspool.StateActive); active.FileName != "app_20260906_1.log.gz" {
		t.Fatalf("active file = %q, want unmatched file", active.FileName)
	}
	if done := replaytest.OnlyFile(t, store, pkgspool.StateDone); done.FileName != "error_app_20260906_1.log.gz" {
		t.Fatalf("done file = %q, want matched file", done.FileName)
	}
}

func TestRunnerRetryableFailureMovesFileBehindOlderWaitingFile(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	replaytest.SaveFile(t, store, "app_20260906_2.log.gz")
	upload := &replaytest.Uploader{Err: errors.New("unclassified transient upload failure")}
	worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil)
	if err != nil {
		t.Fatal(err)
	}

	first, err := runner.RunOnce(context.Background())
	if err == nil || len(first.CompressedLogFiles) != 1 || first.CompressedLogFiles[0].FileName != "app_20260906_1.log.gz" {
		t.Fatalf("first Runner.RunOnce() report/error = %+v/%v", first, err)
	}
	upload.SetError(nil)
	second, err := runner.RunOnce(context.Background())
	if err != nil || len(second.CompressedLogFiles) != 1 || second.CompressedLogFiles[0].FileName != "app_20260906_2.log.gz" {
		t.Fatalf("second Runner.RunOnce() report/error = %+v/%v, want second file", second, err)
	}
	if got := upload.CallCount(); got != 2 {
		t.Fatalf("upload calls = %d, want one per cycle", got)
	}
}

func TestRunnerRetryAfterDefersOnlyTheRejectingDestination(t *testing.T) {
	// synctestの仮想時刻でRetry-Afterの1時間を待ちます。待機順を決める更新時刻も
	// 同じ時刻源で設定し、保存時のfilesystem時刻に依存しません。
	synctest.Test(t, func(t *testing.T) {
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
		firstFile := replaytest.SaveFile(t, throttledStore, "app_20260906_3.log.gz")
		secondFile := replaytest.SaveFile(t, throttledStore, "app_20260906_4.log.gz")
		otherFile := replaytest.SaveFile(t, otherStore, "app_20260906_5.log.gz")
		oldest := time.Now().UTC().Add(-2 * time.Hour)
		for index, file := range []objectstoragespool.CompressedLogFile{firstFile, secondFile, otherFile} {
			queuedAt := oldest.Add(time.Duration(index) * time.Minute)
			if err := os.Chtimes(file.Path, queuedAt, queuedAt); err != nil {
				t.Fatal(err)
			}
		}

		throttled := &replaytest.Uploader{DestinationID: "s3-a", Err: &pkgs3.HTTPStatusError{
			Code:               http.StatusServiceUnavailable,
			RetryAfterDuration: time.Hour,
		}}
		other := &replaytest.Uploader{DestinationID: "s3-b"}
		worker, err := compressedlogworker.NewDestinationWorker(stores, compressedlogworker.RouteRegistry{
			{FileName: replaytest.FileNamePattern("app_"), Uploader: throttled},
			{FileName: replaytest.FileNamePattern("app_"), Uploader: other},
		}, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil)
		if err != nil {
			t.Fatal(err)
		}

		failedAt := time.Now()
		firstReport, err := runner.RunOnce(context.Background())
		if err == nil || len(firstReport.CompressedLogFiles) != 1 ||
			firstReport.CompressedLogFiles[0].FileName != firstFile.FileName {
			t.Fatalf(
				"first Runner.RunOnce() report/error = %+v/%v, want failed %q",
				firstReport,
				err,
				firstFile.FileName,
			)
		}
		if got, want := firstReport.CompressedLogFiles[0].DeferredUntil, failedAt.Add(time.Hour); !got.Equal(want) {
			t.Fatalf("first Runner.RunOnce() deferred until %s, want Retry-After deadline %s", got, want)
		}
		throttled.SetError(nil)

		// Retry-Afterの間は同じ宛先の他のファイルも送らず、他の宛先の再送は続けます。
		secondReport, err := runner.RunOnce(context.Background())
		if err != nil || len(secondReport.CompressedLogFiles) != 1 ||
			secondReport.CompressedLogFiles[0].FileName != otherFile.FileName ||
			!secondReport.CompressedLogFiles[0].UploadSucceeded {
			t.Fatalf(
				"second Runner.RunOnce() report/error = %+v/%v, want %q uploaded to another destination",
				secondReport,
				err,
				otherFile.FileName,
			)
		}
		time.Sleep(time.Hour - time.Second)
		waitingReport, err := runner.RunOnce(context.Background())
		if err != nil || len(waitingReport.CompressedLogFiles) != 0 {
			t.Fatalf("Runner.RunOnce() before Retry-After = %+v/%v, want no upload", waitingReport, err)
		}
		if got := throttled.CallCount(); got != 1 {
			t.Fatalf("throttled destination upload calls = %d, want 1 before Retry-After", got)
		}

		// 期限後は同じ宛先の待機列を古い順に再開します。失敗したファイルは末尾へ戻っています。
		time.Sleep(time.Second)
		for _, want := range []string{secondFile.FileName, firstFile.FileName} {
			report, err := runner.RunOnce(context.Background())
			if err != nil || len(report.CompressedLogFiles) != 1 ||
				report.CompressedLogFiles[0].FileName != want || !report.CompressedLogFiles[0].UploadSucceeded {
				t.Fatalf("Runner.RunOnce() after Retry-After report/error = %+v/%v, want %q", report, err, want)
			}
		}
		if got := throttled.CallCount(); got != 3 {
			t.Fatalf("throttled destination upload calls = %d, want one PUT per RunOnce after Retry-After", got)
		}
		done, err := throttledStore.ListFiles(context.Background(), pkgspool.StateDone)
		if err != nil {
			t.Fatal(err)
		}
		if len(done) != 2 {
			t.Fatalf("done files = %+v, want both throttled files", done)
		}
	})
}

func TestRunnerRetryQueueOrderingSurvivesStoreWorkerRunnerRecreation(t *testing.T) {
	store := replaytest.NewStore(t)
	firstFile := replaytest.SaveFile(t, store, "app_20260906_11.log.gz")
	secondFile := replaytest.SaveFile(t, store, "app_20260906_12.log.gz")
	oldest := time.Now().UTC().Add(-2 * time.Hour)
	if err := os.Chtimes(firstFile.Path, oldest, oldest); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(secondFile.Path, oldest.Add(time.Hour), oldest.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	upload := &replaytest.Uploader{Err: errors.New("unclassified transient upload failure")}
	routes := compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}}
	worker := newReplayTestWorker(t, store, routes)
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil)
	if err != nil {
		t.Fatal(err)
	}
	firstReport, err := runner.RunOnce(context.Background())
	if err == nil || len(firstReport.CompressedLogFiles) != 1 ||
		firstReport.CompressedLogFiles[0].FileName != firstFile.FileName {
		t.Fatalf("first Runner.RunOnce() report/error = %+v/%v", firstReport, err)
	}

	// Recreate every replay-layer object from the same filesystem root. Queue
	// fairness must come from the requeued file's durable mtime, not memory held
	// by the original Store, compressedlogworker.Worker, or Runner.
	restartedStore, err := objectstoragespool.NewStore(store.Root())
	if err != nil {
		t.Fatal(err)
	}
	upload.SetError(nil)
	restartedWorker := newReplayTestWorker(t, restartedStore, routes)
	restartedRunner, err := NewRunner(
		restartedWorker,
		NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondReport, err := restartedRunner.RunOnce(context.Background())
	if err != nil || len(secondReport.CompressedLogFiles) != 1 ||
		secondReport.CompressedLogFiles[0].FileName != secondFile.FileName {
		t.Fatalf(
			"restarted Runner.RunOnce() report/error = %+v/%v, want %q",
			secondReport,
			err,
			secondFile.FileName,
		)
	}
}

func TestRunnerRetryableFailureRequeuesAtUploadCompletionTime(t *testing.T) {
	// 再送待ちへ戻す時刻はtime.Nowで付けるため、仮想時計で試行開始・file 2の保存・
	// 失敗の時刻を十分に離し、filesystemの更新時刻の分解能に依存せず順序を確認します。
	synctestFromWallClockForTest(t, func(t *testing.T) {
		store := replaytest.NewStore(t)
		firstFile := replaytest.SaveFile(t, store, "app_20260906_21.log.gz")
		old := time.Now().UTC().Add(-time.Hour)
		if err := os.Chtimes(firstFile.Path, old, old); err != nil {
			t.Fatal(err)
		}

		ready := make(chan struct{}, 1)
		block := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(block) }) }
		t.Cleanup(release)
		upload := &replaytest.Uploader{
			Err:   errors.New("unclassified transient upload failure"),
			Ready: ready,
			Block: block,
		}
		worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"),
			Uploader: upload,
		}})
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil)
		if err != nil {
			t.Fatal(err)
		}

		type runResult struct {
			report RunReport
			err    error
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		resultCh := make(chan runResult, 1)
		go func() {
			report, runErr := runner.RunOnce(ctx)
			resultCh <- runResult{report: report, err: runErr}
		}()

		select {
		case <-ready:
		case <-time.After(time.Minute):
			t.Fatal("first upload did not start")
		}
		secondFile := replaytest.SaveFile(t, store, "app_20260906_22.log.gz")
		// Place file 2 strictly after the first attempt started, then keep the
		// first PUT blocked until strictly after file 2's queue timestamp. If file
		// 1 were timestamped at attempt start, it would incorrectly remain oldest.
		betweenAttemptAndFailure := time.Now().UTC().Add(10 * time.Second)
		if err := os.Chtimes(
			secondFile.Path,
			betweenAttemptAndFailure,
			betweenAttemptAndFailure,
		); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Second)
		release()

		firstResult := <-resultCh
		if firstResult.err == nil || len(firstResult.report.CompressedLogFiles) != 1 ||
			firstResult.report.CompressedLogFiles[0].FileName != firstFile.FileName {
			t.Fatalf(
				"blocked Runner.RunOnce() report/error = %+v/%v",
				firstResult.report,
				firstResult.err,
			)
		}
		upload.SetError(nil)
		secondReport, err := runner.RunOnce(context.Background())
		if err != nil || len(secondReport.CompressedLogFiles) != 1 ||
			secondReport.CompressedLogFiles[0].FileName != secondFile.FileName {
			t.Fatalf(
				"next Runner.RunOnce() report/error = %+v/%v, want %q",
				secondReport,
				err,
				secondFile.FileName,
			)
		}
	})
}

func TestRunnerNeverReactivatesDoneOrDeadFiles(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	success := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: success,
	}})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)

	permanentSource := replaytest.WriteSource(t, "debug_app_20260906_1.log.gz")
	if _, err := store.SavePermanentFailure(context.Background(), permanentSource); err != nil {
		t.Fatal(err)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDead)

	replacement := &replaytest.Uploader{}
	replacementWorker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{
		{FileName: replaytest.FileNamePattern("app_"), Uploader: replacement},
		{FileName: replaytest.FileNamePattern("debug_app_"), Uploader: replacement},
	})
	replacementRunner, err := NewRunner(
		replacementWorker,
		NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	report, err := replacementRunner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("Runner.RunOnce() error = %v", err)
	}
	if len(report.CompressedLogFiles) != 0 || replacement.CallCount() != 0 {
		t.Fatalf("terminal files were replayed: report=%+v calls=%d", report, replacement.CallCount())
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDead)
}

func TestRunnerRunOnceRecoversCrashLeftProcessingBeforeReplay(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	if _, claimed, err := store.ClaimOldestActive(context.Background()); err != nil {
		t.Fatalf("Store.ClaimOldestActive() error = %v", err)
	} else if !claimed {
		t.Fatal("Store.ClaimOldestActive() claimed = false, want true")
	}
	worker := newReplayTestWorker(t, store, nil)
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Hour}), nil)
	if err != nil {
		t.Fatal(err)
	}

	report, err := runner.RunOnce(context.Background())
	if !errors.Is(err, compressedlogworker.ErrNoMatchingRoute) || report.RecoveredProcessingCount != 1 {
		t.Fatalf("RunOnce() = %+v, %v; want recovered file followed by unmatched replay", report, err)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateActive)
}

func TestRunnerForceCancellationReturnsInFlightUploadToActive(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	ready := make(chan struct{}, 1)
	neverComplete := make(chan struct{})
	upload := &replaytest.Uploader{Ready: ready, Block: neverComplete}
	worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"),
		Uploader: upload,
	}})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.runWithContexts(ctx, ctx) }()
	select {
	case <-ready:
	case <-time.After(hangDetectionTimeout):
		t.Fatal("upload did not start")
	}
	cancel()
	if err := waitRunnerOperationForTest(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Runner.Run() error = %v, want context.Canceled", err)
	}
	_ = replaytest.OnlyFile(t, store, pkgspool.StateActive)
}

func TestRunnerConstructorValidation(t *testing.T) {
	if _, err := NewRunner(nil, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Second}), nil); !errors.Is(err, ErrNilWorker) {
		t.Fatalf("NewRunner(nil) error = %v, want ErrNilWorker", err)
	}
	worker := newReplayTestWorker(t, replaytest.NewStore(t), nil)
	if _, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{}), nil); !errors.Is(err, ErrInvalidRunnerInterval) {
		t.Fatalf("NewRunner(zero interval) error = %v, want ErrInvalidRunnerInterval", err)
	}
}
