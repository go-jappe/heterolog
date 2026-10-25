package fsarchivehandler

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchivePipelineCheckpointErrorNotifiesAndCallerShutdownDrainsRecovery(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, withReport := range []bool{false, true} {
			entrypoint := "HandleFailure"
			if withReport {
				entrypoint = "HandleFailureWithReport"
			}
			t.Run(string(failureCategory)+"/"+entrypoint, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
					checkpointErr := errors.New("checkpoint load failed")
					var pipeline *FailureArchivePipeline
					var callbackSeen atomic.Bool
					callbackDone := make(chan struct{})
					const destination = objectstorage.DestinationID("checkpoint-notification")
					uploadClient := newDestinationFailureArchiveUploader(destination)
					pipeline = newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
						cfg.enableRotate = true
						cfg.enableFileCompress = true
						cfg.setArchiveUploader(failureCategory, uploadClient)
						// upload処理とupload対象の保護の確認のどちらが先にLoadしても、upload処理が
						// 失敗を観測して通知するよう、通知されるまでLoadを失敗させます。
						store := &transientArchiveCheckpointStore{loadFailuresUntil: callbackDone, failure: checkpointErr}
						cfg.embedArchiveCheckpointStore(failureCategory, &store.Store, store)
					}, InternalLogHandlers{OnError: func(
						_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter,
					) {
						if !errors.Is(err, checkpointErr) || !callbackSeen.CompareAndSwap(false, true) {
							return
						}
						defer close(callbackDone)
						// 通知元のwriter lockは解放済みなので、保存済みのrecordを確認できます。
						activePath, err := pipeline.ActiveArchivePath(failureCategory)
						if err != nil {
							t.Errorf("ActiveArchivePath from checkpoint OnError: %v", err)
							return
						}
						records := readArchiveRecords(t, activePath)
						if len(records) != 1 || records[0].Payload["id"] != "checkpoint-shutdown-reentry" {
							t.Errorf("record not persisted before checkpoint OnError: %+v", records)
							return
						}
					}})
					oldPath, err := pipeline.ActiveArchivePath(failureCategory)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Truncate(oldPath, int64(pipeline.handler.Config.MaxFileSize()+1)); err != nil {
						t.Fatal(err)
					}
					result := retryableArchiveResult(now, "checkpoint-shutdown-reentry")
					if failureCategory == failurearchive.FailureCategoryPermanent {
						result.DeadRecords = result.RetryableRecords
						result.RetryableRecords = nil
					}
					if withReport {
						report, err := pipeline.HandleFailureWithReport(context.Background(), result)
						if err != nil {
							t.Fatalf("HandleFailureWithReport: %v", err)
						}
						if len(report.Durables) != 1 || report.Durables[0].Status != pkglogger.DurableSucceeded {
							t.Errorf("durable report = %+v, want successful record persistence", report.Durables)
						}
					} else if err := pipeline.HandleFailure(context.Background(), result); err != nil && !errors.Is(err, checkpointErr) {
						// 圧縮後のLoad障害はtaskから通知されます。並行する保存後cleanupも
						// 同じfileを観測した場合だけ、HandleFailureにも同じ障害が返ります。
						t.Errorf("HandleFailure error = %v, want nil or checkpoint load failure", err)
					}
					select {
					case <-callbackDone:
					case <-time.After(8 * time.Second):
						t.Fatal("checkpoint OnError was not called")
					}
					if err := pipeline.Shutdown(context.Background()); err != nil {
						t.Fatalf("caller Shutdown did not drain accepted recovery: %v", err)
					}
					if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
						t.Errorf("compressed raw source remains: %v", err)
					}
					if _, err := os.Stat(oldPath + ".gz"); !errors.Is(err, os.ErrNotExist) {
						t.Errorf("accepted recovery did not finish compressed archive removal: %v", err)
					}
					if attempts := uploadClient.attemptCount(oldPath+".gz", destination); attempts != 1 {
						t.Errorf("accepted recovery upload attempts=%d, want one after checkpoint load recovers", attempts)
					}
				})
			})
		}
	}
}

func TestFailureArchivePipelineWarnHandlerRunsAfterWriterUnlock(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	notified := make(chan struct{}, 1)
	var pipeline *FailureArchivePipeline
	pipeline = newTestArchiveHandler(
		t,
		now,
		nil,
		InternalLogHandlers{OnWarn: func(
			context.Context,
			*logging.LogEntry,
			observer.Tracer,
			observer.Meter,
		) {
			if !pipeline.handler.Fsstore.Mu.TryLock() {
				t.Error("OnWarn called while the writer lock was held")
			} else {
				pipeline.handler.Fsstore.Mu.Unlock()
			}
			notified <- struct{}{}
		}},
	)
	initializeInlineArchiveTrackers(t, pipeline)

	handler := pipeline.handler
	handler.retryableDirectory.lastAgePurgeAt = now
	activePath := handler.Fsstore.ActiveFilePathRetryable
	activeBytes := handler.Config.MaxDirSize() + 1
	if err := os.Truncate(activePath, int64(activeBytes)); err != nil {
		t.Fatalf("enlarge active retryable archive: %v", err)
	}
	handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(activeBytes)

	handleResult := make(chan error, 1)
	go func() {
		handleResult <- pipeline.HandleFailure(
			context.Background(),
			retryableArchiveResult(now, "warn-shutdown-reentry"),
		)
	}()

	select {
	case <-notified:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("OnWarn was not called")
	}
	select {
	case err := <-handleResult:
		if err != nil {
			t.Fatalf("HandleFailure() error = %v", err)
		}
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("HandleFailure did not return after OnWarn")
	}
}

func TestFailureArchivePipelineUploadErrorIsNotifiedAndKeepsSource(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 30, 0, 0, time.UTC)
	notified := make(chan struct{}, 1)
	var pipeline *FailureArchivePipeline
	pipeline = newTestArchiveHandler(
		t,
		now,
		func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(
				failurearchive.FailureCategoryRetryable,
				failingFailureArchiveUploader{err: errors.New("upload failed")},
			)
		},
		InternalLogHandlers{OnError: func(
			context.Context,
			error,
			*logging.LogEntry,
			observer.Tracer,
			observer.Meter,
		) {
			if !pipeline.handler.Fsstore.Mu.TryLock() {
				t.Error("upload OnError called while the writer lock was held")
			} else {
				pipeline.handler.Fsstore.Mu.Unlock()
			}
			select {
			case notified <- struct{}{}:
			default:
			}
		}},
	)
	// 送信待ちのfileは、構築時に予約されるtarget-only再試行で最初のcleanupから送信します。
	compressedPath := filepath.Join(
		pipeline.handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260830_1.jsonl.gz",
	)
	if err := os.WriteFile(compressedPath, []byte("compressed"), 0o600); err != nil {
		t.Fatalf("write pending compressed archive: %v", err)
	}

	handleResult := make(chan error, 1)
	go func() {
		handleResult <- pipeline.HandleFailure(
			context.Background(),
			retryableArchiveResult(now, "upload-shutdown-reentry"),
		)
	}()

	select {
	case <-notified:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("upload OnError was not called")
	}
	select {
	case err := <-handleResult:
		if err != nil {
			t.Fatalf("HandleFailure() error = %v", err)
		}
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("HandleFailure did not return after upload OnError")
	}
	if _, err := os.Stat(compressedPath); err != nil {
		t.Fatalf("failed upload removed local compressed archive: %v", err)
	}
}

func TestFailureArchivePipelineWriteErrorNotifiedDuringRequest(t *testing.T) {
	for _, withReport := range []bool{false, true} {
		name := "HandleFailure"
		if withReport {
			name = "HandleFailureWithReport"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 8, 31, 11, 0, 0, 0, time.UTC)
			writeErr := errors.New("archive write failed")
			cleanupErr := errors.New("archive cleanup scan failed")
			pipeline := newTestArchiveHandler(t, now, nil)
			handler := pipeline.handler
			var notifications, scans int
			handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				if !errors.Is(err, writeErr) {
					return
				}
				notifications++
				pipeline.operationMu.Lock()
				operationCount := pipeline.operationCount
				pipeline.operationMu.Unlock()
				if operationCount != 1 {
					t.Errorf("accepted request count during OnError = %d, want 1", operationCount)
				}
				if !handler.Fsstore.Mu.TryLock() {
					t.Error("write OnError called before releasing the writer lock")
				} else {
					handler.Fsstore.Mu.Unlock()
				}
				if scans != 0 {
					t.Error("write OnError was delayed until cleanup")
				}
			}
			handler.appendJSONLWriter = func(io.Writer, ...any) (uint64, error) {
				return 0, writeErr
			}
			handler.globArchiveFiles = func(logpattern.FileNamePattern, logpattern.GlobMode) ([]string, error) {
				scans++
				if notifications != 1 {
					t.Errorf("write error notifications before cleanup = %d, want 1", notifications)
				}
				return nil, cleanupErr
			}
			result := loggerfailure.SendResult{RetryableRecords: []loggerfailure.FailedRecord{{
				Payload: map[string]any{"message": "notify before cleanup"},
			}}}
			var err error
			if withReport {
				_, err = pipeline.HandleFailureWithReport(context.Background(), result)
			} else {
				err = pipeline.HandleFailure(context.Background(), result)
			}
			if !errors.Is(err, writeErr) {
				t.Fatalf("HandleFailure error = %v, want write error", err)
			}
			if notifications != 1 {
				t.Fatalf("write notifications=%d, want 1", notifications)
			}
			if withReport {
				if scans != 0 {
					t.Fatalf("HandleFailureWithReport cleanup scans=%d, want 0", scans)
				}
			} else if scans == 0 || !errors.Is(err, cleanupErr) {
				t.Fatalf("cleanup scans=%d, error=%v, want scan failure after write notification", scans, err)
			}
		})
	}
}
