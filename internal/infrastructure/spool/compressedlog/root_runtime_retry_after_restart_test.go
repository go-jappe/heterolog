package compressedlog

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func retryAfterRestartRootConfig(t *testing.T) ConfigProvider {
	t.Helper()
	cfg := managedUploaderTestConfig(resolvedTempDirForTest(t))
	cfg.RunnerInterval, cfg.LifecycleInterval = time.Hour, time.Hour
	cfg.MaxRetryAfter = 30 * time.Minute
	return NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3)
}

func registerRetryAfterRestartRoute(t *testing.T, runtime *RootRuntime, upload *replaytest.Uploader, warnings *[]error) *ManagedRoute {
	t.Helper()
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		OnWarn:   func(_ context.Context, warning error) { *warnings = append(*warnings, warning) },
	}, upload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = route.Close() })
	return route
}

// 再起動後のruntimeは、宛先別spoolに保存した期限まで、その宛先のファイルを送信しない。
func TestRootRuntimeRestoresDestinationRetryAfterAcrossRestart(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		ctx := context.Background()
		config := retryAfterRestartRootConfig(t)
		first := newTestRootRuntime(t, config)
		var firstWarnings []error
		throttled := &replaytest.Uploader{DestinationID: "s3-throttled", Err: &pkgs3.HTTPStatusError{
			Code:               http.StatusServiceUnavailable,
			RetryAfterDuration: 10 * time.Minute,
		}}
		route := registerRetryAfterRestartRoute(t, first, throttled, &firstWarnings)
		if _, err := route.SaveRetryableFailure(ctx, replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
			t.Fatal(err)
		}
		failed, err := first.runner.RunOnce(ctx)
		if err == nil || len(failed.CompressedLogFiles) != 1 || failed.CompressedLogFiles[0].DeferredReason == "" {
			t.Fatalf("throttled RunOnce() report/error = %+v/%v", failed, err)
		}
		notBefore := failed.CompressedLogFiles[0].DeferredUntil
		if want := time.Now().Add(10 * time.Minute); !notBefore.Equal(want) {
			t.Fatalf("DeferredUntil = %s, want %s", notBefore, want)
		}
		if len(failed.RetryAfterMarkers) != 0 {
			t.Fatalf("RetryAfterMarkers = %+v, want the deadline saved without warnings", failed.RetryAfterMarkers)
		}
		if err := errors.Join(first.Close(), route.Close()); err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Minute)
		restarted := newTestRootRuntime(t, config)
		var warnings []error
		resumedUpload := &replaytest.Uploader{DestinationID: "s3-throttled"}
		registerRetryAfterRestartRoute(t, restarted, resumedUpload, &warnings)
		deferred, err := restarted.runner.RunOnce(ctx)
		if err != nil || len(deferred.CompressedLogFiles) != 0 || resumedUpload.CallCount() != 0 {
			t.Fatalf("RunOnce() after restart report/error = %+v/%v, want the destination kept waiting", deferred, err)
		}
		restarted.ReportSpoolRunnerRun(ctx, deferred, err)
		if len(warnings) != 0 {
			t.Fatalf("warnings after restart = %v, want none", warnings)
		}

		time.Sleep(time.Until(notBefore))
		resumed, err := restarted.runner.RunOnce(ctx)
		if err != nil || len(resumed.CompressedLogFiles) != 1 || !resumed.CompressedLogFiles[0].UploadSucceeded || resumedUpload.CallCount() != 1 {
			t.Fatalf("RunOnce() at the persisted deadline report/error = %+v/%v, want uploaded", resumed, err)
		}
	})
}

// 再起動前に保存した期限の短縮と、壊れたmarkerを、宛先を示す警告としてOnWarnへ一度ずつ渡す。
func TestRootRuntimeWarnsAboutPersistedRetryAfterMarkers(t *testing.T) {
	const destination objectstorage.DestinationID = "s3-marker"
	for _, tt := range []struct {
		name  string
		write func(t *testing.T, store *retryafter.Store)
		check func(t *testing.T, warnings []error, report RunReport, upload *replaytest.Uploader)
	}{
		{
			name: "stored deadline beyond maximum",
			write: func(t *testing.T, store *retryafter.Store) {
				if err := store.Save(retryafter.State{NotBefore: time.Now().Add(3 * time.Hour)}); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, warnings []error, report RunReport, upload *replaytest.Uploader) {
				var clamped *RetryAfterClampWarning
				if len(warnings) != 1 || !errors.Is(warnings[0], ErrRetryAfterClamped) || !errors.As(warnings[0], &clamped) {
					t.Fatalf("warnings = %v, want one stored Retry-After clamp", warnings)
				}
				want := RetryAfterClampWarning{
					DestinationID: destination,
					Requested:     3 * time.Hour,
					MaxRetryAfter: 30 * time.Minute,
					NotBefore:     time.Now().Add(30 * time.Minute).Round(0),
					Stored:        true,
				}
				if clamped.DestinationID != want.DestinationID || clamped.Requested != want.Requested ||
					clamped.MaxRetryAfter != want.MaxRetryAfter || !clamped.NotBefore.Equal(want.NotBefore) || !clamped.Stored {
					t.Fatalf("stored clamp warning = %+v, want %+v", *clamped, want)
				}
				if len(report.CompressedLogFiles) != 0 || upload.CallCount() != 0 {
					t.Fatalf("report/uploads = %+v/%d, want the clamped deadline honored", report, upload.CallCount())
				}
			},
		},
		{
			name: "corrupt marker",
			write: func(t *testing.T, store *retryafter.Store) {
				if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(store.Path(), []byte("not json"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, warnings []error, report RunReport, upload *replaytest.Uploader) {
				var markerWarning *RetryAfterMarkerWarning
				var markerErr *retryafter.MarkerError
				if len(warnings) != 1 || !errors.Is(warnings[0], ErrRetryAfterMarker) || !errors.Is(warnings[0], retryafter.ErrInvalidMarker) ||
					!errors.As(warnings[0], &markerWarning) || !errors.As(warnings[0], &markerErr) {
					t.Fatalf("warnings = %v, want one invalid Retry-After marker warning", warnings)
				}
				if markerWarning.DestinationID != destination || markerErr.Operation != retryafter.MarkerOperationLoad {
					t.Fatalf("marker warning = %+v / %+v, want a load failure for %q", markerWarning, markerErr, destination)
				}
				if len(report.CompressedLogFiles) != 1 || !report.CompressedLogFiles[0].UploadSucceeded || upload.CallCount() != 1 {
					t.Fatalf("report/uploads = %+v/%d, want delivery to continue", report, upload.CallCount())
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctestFromWallClockForTest(t, func(t *testing.T) {
				ctx := context.Background()
				runtime := newTestRootRuntime(t, retryAfterRestartRootConfig(t))
				var warnings []error
				upload := &replaytest.Uploader{DestinationID: destination}
				route := registerRetryAfterRestartRoute(t, runtime, upload, &warnings)
				if _, err := route.SaveRetryableFailure(ctx, replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
					t.Fatal(err)
				}
				store, err := retryafter.NewObjectStorageStore(route.Root().Path(), string(destination))
				if err != nil {
					t.Fatal(err)
				}
				tt.write(t, store)

				report, err := runtime.runner.RunOnce(ctx)
				if err != nil {
					t.Fatal(err)
				}
				runtime.ReportSpoolRunnerRun(ctx, report, err)
				tt.check(t, warnings, report, upload)

				next, err := runtime.runner.RunOnce(ctx)
				runtime.ReportSpoolRunnerRun(ctx, next, err)
				if len(warnings) != 1 {
					t.Fatalf("warnings after the next cycle = %v, want no repeated warning", warnings)
				}
			})
		})
	}
}

// routeが直接uploadのRetry-Afterを記録すると、再送workerと共有する宛先の期限になる。
// 上限を超える指定は上限までに短縮して一度だけ警告し、より後の期限を短縮しない。
// Retry-Afterを含まない失敗を記録しないことは、
// TestManagedRouteRecordUploadRetryAfterIgnoresFailuresWithoutTemporaryRetryAfterで確認する。
func TestManagedRouteRecordUploadRetryAfterSharesClampedDeadline(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		ctx := context.Background()
		runtime := newTestRootRuntime(t, retryAfterRestartRootConfig(t))
		var warnings []error
		upload := &replaytest.Uploader{DestinationID: "s3-direct"}
		route := registerRetryAfterRestartRoute(t, runtime, upload, &warnings)
		// 稼働中のworkerは、この宛先の保存済みの期限をすでに読み込んでいます。
		if idle, err := runtime.runner.RunOnce(ctx); err != nil || len(idle.CompressedLogFiles) != 0 {
			t.Fatalf("RunOnce() before the direct upload report/error = %+v/%v", idle, err)
		}
		if _, err := route.SaveRetryableFailure(ctx, replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
			t.Fatal(err)
		}

		route.RecordUploadRetryAfter(ctx, &pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable, RetryAfterDuration: 3 * time.Hour})
		notBefore := time.Now().Add(30 * time.Minute)
		deferred, runErr := runtime.runner.RunOnce(ctx)
		if runErr != nil || len(deferred.CompressedLogFiles) != 0 || upload.CallCount() != 0 {
			t.Fatalf("RunOnce() report/error = %+v/%v, want the running worker to defer the destination", deferred, runErr)
		}

		route.RecordUploadRetryAfter(ctx, &pkgs3.HTTPStatusError{Code: http.StatusTooManyRequests, RetryAfterDuration: 10 * time.Minute})
		store, err := retryafter.NewObjectStorageStore(route.Root().Path(), "s3-direct")
		if err != nil {
			t.Fatal(err)
		}
		saved, _, err := store.Load(time.Now(), time.Hour)
		if err != nil || !saved.NotBefore.Equal(notBefore) {
			t.Fatalf("saved marker = %+v/%v, want the clamped deadline %s kept", saved, err, notBefore)
		}
		runtime.ReportSpoolRunnerRun(ctx, deferred, runErr)
		var clamped *RetryAfterClampWarning
		if len(warnings) != 1 || !errors.As(warnings[0], &clamped) || clamped.Stored ||
			clamped.Requested != 3*time.Hour || clamped.MaxRetryAfter != 30*time.Minute || !clamped.NotBefore.Equal(notBefore) {
			t.Fatalf("warnings = %v, want one clamp of the 3h direct-upload Retry-After", warnings)
		}

		time.Sleep(time.Until(notBefore))
		resumed, err := runtime.runner.RunOnce(ctx)
		if err != nil || len(resumed.CompressedLogFiles) != 1 || !resumed.CompressedLogFiles[0].UploadSucceeded {
			t.Fatalf("RunOnce() at the deadline report/error = %+v/%v, want uploaded", resumed, err)
		}
	})
}

// requireNoRetryAfterMarkerFileは、宛先別spoolに待機期限のmarkerが作られていないことを確認します。
func requireNoRetryAfterMarkerFile(t *testing.T, route *ManagedRoute, destination objectstorage.DestinationID) {
	t.Helper()
	path := filepath.Join(route.Root().Path(), retryafter.DirectoryName, string(destination)+".json")
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Retry-After marker %q stat error = %v, want no marker", path, err)
	}
}

// 直接uploadの失敗のうち、Retry-Afterを含まないものと、一時的な失敗ではない拒否が返したRetry-Afterは、
// 宛先の期限として記録しない。稼働中の再送workerは次の周期にファイルを送信し、markerも作らない。
func TestManagedRouteRecordUploadRetryAfterIgnoresFailuresWithoutTemporaryRetryAfter(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		ctx := context.Background()
		const destination objectstorage.DestinationID = "s3-direct"
		runtime := newTestRootRuntime(t, retryAfterRestartRootConfig(t))
		var warnings []error
		upload := &replaytest.Uploader{DestinationID: destination}
		route := registerRetryAfterRestartRoute(t, runtime, upload, &warnings)
		if _, err := route.SaveRetryableFailure(ctx, replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
			t.Fatal(err)
		}

		for _, uploadErr := range []error{
			errors.New("temporary outage without Retry-After"),
			&pkgs3.HTTPStatusError{Code: http.StatusServiceUnavailable},
			// 恒久的な拒否は再送しないため、Retry-Afterを伴っても宛先を待たせません。
			&pkgs3.HTTPStatusError{Code: http.StatusForbidden, RetryAfterDuration: time.Hour},
		} {
			route.RecordUploadRetryAfter(ctx, uploadErr)
		}
		report, err := runtime.runner.RunOnce(ctx)
		if err != nil || len(report.CompressedLogFiles) != 1 || !report.CompressedLogFiles[0].UploadSucceeded || upload.CallCount() != 1 {
			t.Fatalf("RunOnce() report/error = %+v/%v (uploads %d), want the file uploaded without waiting",
				report, err, upload.CallCount())
		}
		requireNoRetryAfterMarkerFile(t, route, destination)
		runtime.ReportSpoolRunnerRun(ctx, report, err)
		if len(warnings) != 0 {
			t.Fatalf("warnings = %v, want none", warnings)
		}
	})
}

// 直接uploadの呼び出し元のcontextが終了していても、送信先が一時的な失敗とともに応答で指定した
// Retry-Afterは宛先の期限として記録し、保存する。
func TestManagedRouteRecordUploadRetryAfterRecordsAfterCallerContextEnds(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		const destination objectstorage.DestinationID = "s3-direct"
		runtime := newTestRootRuntime(t, retryAfterRestartRootConfig(t))
		var warnings []error
		upload := &replaytest.Uploader{DestinationID: destination}
		route := registerRetryAfterRestartRoute(t, runtime, upload, &warnings)
		if _, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
			t.Fatal(err)
		}

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		route.RecordUploadRetryAfter(canceled, &pkgs3.HTTPStatusError{Code: http.StatusTooManyRequests, RetryAfterDuration: 10 * time.Minute})
		notBefore := time.Now().Add(10 * time.Minute)
		deferred, err := runtime.runner.RunOnce(context.Background())
		if err != nil || len(deferred.CompressedLogFiles) != 0 || upload.CallCount() != 0 {
			t.Fatalf("RunOnce() report/error = %+v/%v, want the destination deferred", deferred, err)
		}
		store, err := retryafter.NewObjectStorageStore(route.Root().Path(), string(destination))
		if err != nil {
			t.Fatal(err)
		}
		if saved, _, err := store.Load(time.Now(), time.Hour); err != nil || !saved.NotBefore.Equal(notBefore) {
			t.Fatalf("saved marker = %+v/%v, want the deadline %s", saved, err, notBefore)
		}
	})
}

// 宛先の期限を保存できなかった場合は、runnerの周期の後に保存し直す。周期は保存の再試行の既定値
// （1秒）と異なる値にし、runtimeが周期をworkerへ渡していることを確認する。
func TestRootRuntimeRetriesFailedMarkerSaveAfterRunnerInterval(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		ctx := context.Background()
		const (
			destination objectstorage.DestinationID = "s3-throttled"
			interval                                = 250 * time.Millisecond
		)
		cfg := managedUploaderTestConfig(resolvedTempDirForTest(t))
		cfg.RunnerInterval, cfg.LifecycleInterval = interval, time.Hour
		cfg.MaxRetryAfter = 30 * time.Minute
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		upload := &replaytest.Uploader{DestinationID: destination, Err: &pkgs3.HTTPStatusError{
			Code:               http.StatusTooManyRequests,
			RetryAfterDuration: 10 * time.Minute,
		}}
		// 周期処理も同じruntimeで動くため、ここではOnWarnを登録せず、markerの結果をRunOnceのreportで確認します。
		route, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: replaytest.FileNamePattern("app_")}, upload)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = route.Close() })
		// .retry-afterを通常fileで塞ぎ、最初の期限の保存を失敗させます。
		obstruction := filepath.Join(route.Root().Path(), retryafter.DirectoryName)
		if err := os.WriteFile(obstruction, []byte("not a directory"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := route.SaveRetryableFailure(ctx, replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
			t.Fatal(err)
		}

		failed, err := runtime.runner.RunOnce(ctx)
		notBefore := time.Now().Add(10 * time.Minute)
		if err == nil || len(failed.CompressedLogFiles) != 1 || !failed.CompressedLogFiles[0].DeferredUntil.Equal(notBefore) {
			t.Fatalf("throttled RunOnce() report/error = %+v/%v, want deferred until %s", failed, err, notBefore)
		}
		if !hasRetryAfterMarkerFailure(failed.RetryAfterMarkers, destination, retryafter.MarkerOperationSave) {
			t.Fatalf("RetryAfterMarkers = %+v, want the failed marker save", failed.RetryAfterMarkers)
		}
		if err := os.Remove(obstruction); err != nil {
			t.Fatal(err)
		}

		time.Sleep(interval)
		if deferred, err := runtime.runner.RunOnce(ctx); err != nil || len(deferred.CompressedLogFiles) != 0 {
			t.Fatalf("RunOnce() one runner interval later report/error = %+v/%v, want the destination still deferred", deferred, err)
		}
		store, err := retryafter.NewObjectStorageStore(route.Root().Path(), string(destination))
		if err != nil {
			t.Fatal(err)
		}
		if saved, _, err := store.Load(time.Now(), time.Hour); err != nil || !saved.NotBefore.Equal(notBefore) {
			t.Fatalf("marker one runner interval after the failure = %+v/%v, want the deadline %s saved again", saved, err, notBefore)
		}
		if got := upload.CallCount(); got != 1 {
			t.Fatalf("upload calls = %d, want only the throttled attempt", got)
		}
	})
}

// hasRetryAfterMarkerFailureは、宛先のmarkerについてoperationの失敗が報告されているかを返します。
func hasRetryAfterMarkerFailure(
	reports []compressedlogworker.RetryAfterMarkerReport,
	destination objectstorage.DestinationID,
	operation retryafter.MarkerOperation,
) bool {
	for _, report := range reports {
		if report.DestinationID != destination {
			continue
		}
		for _, err := range markerErrors(report.Err) {
			if err.Operation == operation {
				return true
			}
		}
	}
	return false
}

// markerErrorsは、errのerror treeに含まれるMarkerErrorを出現順に返します。
func markerErrors(err error) []*retryafter.MarkerError {
	var found []*retryafter.MarkerError
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		if markerErr, ok := err.(*retryafter.MarkerError); ok {
			found = append(found, markerErr)
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return found
}

// 再起動前の期限を読み込み時に短縮し、短縮した期限の保存にも失敗したworkerの結果は、短縮の警告と
// markerの警告として一度ずつOnWarnへ渡す。workerが一つの結果として報告することは
// worker.TestDestinationWorkerRetriesSavingDeadlineClampedAtLoadで確認する。
func TestRootRuntimeWarnsOnceEachForClampedDeadlineThatCouldNotBeSaved(t *testing.T) {
	const destination objectstorage.DestinationID = "s3-marker"
	ctx := context.Background()
	runtime := newTestRootRuntime(t, retryAfterRestartRootConfig(t))
	var warnings []error
	registerRetryAfterRestartRoute(t, runtime, &replaytest.Uploader{DestinationID: destination}, &warnings)
	saveErr := &retryafter.MarkerError{
		Operation: retryafter.MarkerOperationSave,
		Path:      "s3-marker.json",
		Err:       errors.New("injected clamped marker save failure"),
	}
	clamp := &pkgspool.RetryAfterClamp{
		Requested:     3 * time.Hour,
		MaxRetryAfter: 30 * time.Minute,
		NotBefore:     time.Date(2026, 10, 5, 12, 30, 0, 0, time.UTC),
		Stored:        true,
	}
	runtime.ReportSpoolRunnerRun(ctx, RunReport{RetryAfterMarkers: []compressedlogworker.RetryAfterMarkerReport{{
		DestinationID: destination, Clamp: clamp, Err: saveErr,
	}}}, nil)

	var clamped *RetryAfterClampWarning
	var markerWarning *RetryAfterMarkerWarning
	clampWarnings, markerWarnings := 0, 0
	for _, warning := range warnings {
		switch {
		case errors.As(warning, &clamped):
			clampWarnings++
			want := RetryAfterClampWarning{
				DestinationID: destination, Requested: clamp.Requested, MaxRetryAfter: clamp.MaxRetryAfter,
				NotBefore: clamp.NotBefore, Stored: true,
			}
			if *clamped != want {
				t.Fatalf("clamp warning = %+v, want %+v", *clamped, want)
			}
		case errors.As(warning, &markerWarning):
			markerWarnings++
			if markerWarning.DestinationID != destination || !errors.Is(warning, ErrRetryAfterMarker) || !errors.Is(warning, saveErr) {
				t.Fatalf("marker warning = %+v, want the save failure for %q", markerWarning, destination)
			}
		default:
			t.Fatalf("unexpected warning %v", warning)
		}
	}
	if clampWarnings != 1 || markerWarnings != 1 {
		t.Fatalf("warnings = %v, want one clamp warning and one marker warning", warnings)
	}

	runtime.ReportSpoolRunnerRun(ctx, RunReport{}, nil)
	if len(warnings) != 2 {
		t.Fatalf("warnings after the next cycle = %v, want no repeated warning", warnings)
	}
}
