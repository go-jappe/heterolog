package fsarchivehandler

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestArchiveFailedAppendPreservesEarlierSuccessAndNextRecord(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		for _, withReport := range []bool{false, true} {
			for _, failure := range []string{"partial write", "complete write with error", "encode after complete record", "short write"} {
				t.Run(string(category)+"/"+failure+map[bool]string{false: "/normal", true: "/report"}[withReport], func(t *testing.T) {
					now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
					pipeline := newTestArchiveHandler(t, now, nil)
					send := func(result loggerfailure.SendResult) error {
						if !withReport {
							return pipeline.HandleFailure(context.Background(), result)
						}
						report, err := pipeline.HandleFailureWithReport(context.Background(), result)
						require.Len(t, report.Durables, 1)
						if err == nil {
							require.Equal(t, pkglogger.DurableSucceeded, report.Durables[0].Status)
						} else {
							require.Equal(t, pkglogger.DurableFailed, report.Durables[0].Status)
						}
						return err
					}
					require.NoError(t, send(failureArchiveResultForFailureCategory(category, "acknowledged-before")))
					path, _, _ := failureArchiveActiveState(pipeline, category)
					before, err := os.ReadFile(path)
					require.NoError(t, err)
					writeErr := errors.New("injected disk-full write")
					// 失敗するこのrequestでも、最初のrecordは完全に書き込まれます。失敗するのは
					// 2回目の書き込みだけなので、rollbackはrequest全体を対象にしなければなりません。
					result := failureArchiveResultForFailureCategory(category, "unacknowledged-complete")
					second := archiveFailedRecord("unacknowledged-second", now, 503, "unavailable")
					if failure == "encode after complete record" {
						second.Payload["not_json"] = make(chan int)
					} else {
						pipeline.handler.appendJSONLWriter = func(writer io.Writer, records ...any) (uint64, error) {
							calls := 0
							return writeArchiveJSONL(archiveWriteFunc(func(p []byte) (int, error) {
								calls++
								if calls == 1 {
									return writer.Write(p)
								}
								limit := 7
								if failure == "complete write with error" {
									limit = len(p)
								}
								n, err := writer.Write(p[:limit])
								if err != nil || failure == "short write" {
									return n, err
								}
								return n, writeErr
							}), records...)
						}
					}
					if category == failurearchive.FailureCategoryRetryable {
						result.RetryableRecords = append(result.RetryableRecords, second)
					} else {
						result.DeadRecords = append(result.DeadRecords, second)
					}
					err = send(result)
					require.Error(t, err)
					if failure == "short write" {
						require.ErrorIs(t, err, io.ErrShortWrite)
					} else if failure != "encode after complete record" {
						require.ErrorIs(t, err, writeErr)
					}
					afterFailure, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Equal(t, before, afterFailure, "failed request must not alter earlier successful records")
					pipeline.handler.appendJSONLWriter = nil
					require.NoError(t, send(failureArchiveResultForFailureCategory(category, "acknowledged-after")))
					rows := readArchiveRecords(t, path)
					require.Len(t, rows, 2)
					require.Equal(t, "acknowledged-before", rows[0].Payload["id"])
					require.Equal(t, "acknowledged-after", rows[1].Payload["id"])
				})
			}
		}
	}
}

func TestArchiveRollbackFailureBlocksAppendAndRotationUntilRecovery(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) { cfg.enableRotate = true })
			mutableClock := &archivePurgeClock{now: now}
			pipeline.handler.Clock = mutableClock
			result := failureArchiveResultForFailureCategory(category, "acknowledged-before")
			_, err := pipeline.HandleFailureWithReport(context.Background(), result)
			require.NoError(t, err)
			path, file, _ := failureArchiveActiveState(pipeline, category)
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			writeErr := errors.New("partial disk-full write")
			rollbackErr := errors.New("truncate unavailable")
			installPartialArchiveWrite(pipeline, writeErr)
			pipeline.handler.truncateArchiveAppend = func(*os.File, int64) error { return rollbackErr }
			report, err := pipeline.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(category, "partial"))
			require.ErrorIs(t, err, writeErr)
			require.ErrorIs(t, err, rollbackErr)
			require.Equal(t, pkglogger.DurableFailed, report.Durables[0].Status)
			partial, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, len(before)+7, len(partial))
			pipeline.handler.appendJSONLWriter = nil
			mutableClock.Set(now.AddDate(0, 0, 1))
			report, err = pipeline.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(category, "blocked"))
			require.ErrorIs(t, err, rollbackErr)
			require.Equal(t, pkglogger.DurableFailed, report.Durables[0].Status)
			stillPath, stillFile, _ := failureArchiveActiveState(pipeline, category)
			require.Equal(t, path, stillPath)
			require.Same(t, file, stillFile)
			unchanged, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, partial, unchanged)
			assertArchivePathProtected(t, pipeline.handler.directoryForFailureCategory(category).guard, path, true)
			pipeline.handler.truncateArchiveAppend = nil
			report, err = pipeline.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(category, "acknowledged-after"))
			require.NoError(t, err)
			require.Equal(t, pkglogger.DurableSucceeded, report.Durables[0].Status)
			newPath, _, _ := failureArchiveActiveState(pipeline, category)
			require.NotEqual(t, path, newPath)
			rows := readArchiveRecords(t, path)
			require.Len(t, rows, 1)
			require.Equal(t, "acknowledged-before", rows[0].Payload["id"])
			rows = readArchiveRecords(t, newPath)
			require.Len(t, rows, 1)
			require.Equal(t, "acknowledged-after", rows[0].Payload["id"])
		})
	}
}

func TestArchiveCloseReportsUnrecoveredAppendAndRestartPreservesTail(t *testing.T) {
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) { cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable })
	category := failurearchive.FailureCategoryRetryable
	_, err := pipeline.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(category, "acknowledged-before"))
	require.NoError(t, err)
	path, _, _ := failureArchiveActiveState(pipeline, category)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	installPartialArchiveWrite(pipeline, errors.New("partial write"))
	rollbackErr := errors.New("truncate unavailable")
	pipeline.handler.truncateArchiveAppend = func(*os.File, int64) error { return rollbackErr }
	_, err = pipeline.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(category, "partial"))
	require.ErrorIs(t, err, rollbackErr)
	partial, err := os.ReadFile(path)
	require.NoError(t, err)
	require.ErrorIs(t, shutdownFailureArchiveForTest(pipeline), rollbackErr)

	reopened, err := NewFailureArchivePipeline(context.Background(), archiveFixedClock{now: now}, pipeline.handler.Config,
		observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{})
	require.Nil(t, reopened)
	require.ErrorIs(t, err, failurearchive.ErrIncompleteJSONLTail)
	unchanged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, partial, unchanged, "restart must not silently truncate unknown bytes")

	// operatorが明示的に修復すれば、同じarchive rootで再起動できます。
	require.NoError(t, os.WriteFile(path, before, 0o640))
	reopened, err = NewFailureArchivePipeline(context.Background(), archiveFixedClock{now: now}, pipeline.handler.Config,
		observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{})
	require.NoError(t, err)
	useTestArchiveAvailableDiskSpace(reopened)
	t.Cleanup(func() { require.NoError(t, shutdownFailureArchiveForTest(reopened)) })
	report, err := reopened.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(category, "after-restart"))
	require.NoError(t, err)
	require.Equal(t, pkglogger.DurableSucceeded, report.Durables[0].Status)
	rows := readArchiveRecords(t, path)
	require.Len(t, rows, 2)
	require.Equal(t, "acknowledged-before", rows[0].Payload["id"])
	require.Equal(t, "after-restart", rows[1].Payload["id"])
}

func TestArchiveAppendRollbackInvalidatesDirectoryUsage(t *testing.T) {
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	tracker := pipeline.handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)
	require.True(t, tracker.TryStartObservation(now, pipeline.handler.Config, true))
	tracker.FinishObservation(0)
	require.False(t, tracker.TryStartObservation(now, pipeline.handler.Config, false))
	installPartialArchiveWrite(pipeline, errors.New("partial write"))
	_, err := pipeline.HandleFailureWithReport(context.Background(), failureArchiveResultForFailureCategory(failurearchive.FailureCategoryRetryable, "partial"))
	require.Error(t, err)
	require.True(t, tracker.TryStartObservation(now, pipeline.handler.Config, false), "rollback must invalidate the growth estimate")
	tracker.AbortObservation()
}

type archiveWriteFunc func([]byte) (int, error)

func (f archiveWriteFunc) Write(p []byte) (int, error) { return f(p) }

func installPartialArchiveWrite(pipeline *FailureArchivePipeline, writeErr error) {
	pipeline.handler.appendJSONLWriter = func(writer io.Writer, records ...any) (uint64, error) {
		return writeArchiveJSONL(&partialArchiveWriter{writer: writer, limit: 7, err: writeErr}, records...)
	}
}
