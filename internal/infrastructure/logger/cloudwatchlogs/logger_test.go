package cloudwatchlogs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

var cloudWatchLogsTestPlainTextFormatter = formatter.NewPlainTextFormatter(
	timestamp.RFC3339Formatter(),
)

func mustNewCloudWatchLogsFailureComposite(
	t *testing.T,
	handlers []flushfailure.FailureHandler,
) *flushfailure.CompositeFailureHandler {
	t.Helper()
	composite, err := flushfailure.NewCompositeFailureHandler(handlers, 1)
	if err != nil {
		t.Fatalf("NewCompositeFailureHandler() error = %v", err)
	}
	return composite
}

func mustNewCloudWatchLogsReportingFailureComposite(
	t *testing.T,
	handlers []flushfailure.FailureHandlerWithReport,
) *flushfailure.ReportingCompositeFailureHandler {
	t.Helper()
	composite, err := flushfailure.NewReportingCompositeFailureHandler(handlers, 1)
	if err != nil {
		t.Fatalf("NewReportingCompositeFailureHandler() error = %v", err)
	}
	return composite
}

// 通常APIでもerrorを渡さないErrorWithStackを拒否せず、stackを持つERRORのeventとして送信します。
func TestCloudWatchLogsLoggerErrorWithStackWithoutErrorPutsStack(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	logger, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(100, time.Hour, true),
		client,
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewCloudWatchLogsLogger() error = %v", err)
	}
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	if err := handleTestErrorWithStack(context.Background(), logger, "missing error", nil); err != nil {
		t.Fatalf("ErrorWithStack(nil) error = %v, want nil", err)
	}
	requireCloudWatchLogsStackEventWithoutError(t, client, "missing error")
}

func TestCloudWatchLogsFlushManagerPutLogEntriesSendsFormattedEvents(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	err := flushManager.Put(context.Background(), newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "hello cloudwatch"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if len(client.events) != 1 {
		t.Fatalf("event count = %d, want 1", len(client.events))
	}
	if got := client.events[0].Timestamp; !got.Equal(time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)) {
		t.Fatalf("event timestamp = %s", got)
	}
	if !strings.Contains(client.events[0].Message, "hello cloudwatch") {
		t.Fatalf("event message = %q", client.events[0].Message)
	}
}

func TestCloudWatchLogsFlushManagerPutWaitsForFailureHandlerAndReturnsItsError(t *testing.T) {
	client := &recordingCloudWatchLogsClient{
		err: &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests"},
	}
	handleErr := errors.New("cloudwatch durable handling failed")
	failureHandler := &recordingCloudWatchLogsFailureHandler{err: handleErr}
	composite := mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{failureHandler})
	flushManager := newTestCloudWatchLogsFlushManager(client, composite)

	err := flushManager.Put(context.Background(), newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "retryable cloudwatch"))
	if !errors.Is(err, handleErr) {
		t.Fatalf("Put() error = %v, want durable handling error", err)
	}

	// Putは同期境界なので、戻った時点でhandlerの結果を観測できます。
	result, ok := failureHandler.result()
	if !ok {
		t.Fatalf("failure handler was not invoked")
	}
	if len(result.RetryableRecords) != 1 {
		t.Fatalf("retryable records = %d, want 1", len(result.RetryableRecords))
	}
	if len(result.DeadRecords) != 0 {
		t.Fatalf("dead records = %d, want 0", len(result.DeadRecords))
	}
	if result.TerminalError != nil {
		t.Fatalf("TerminalError = %v, want classified record failure only", result.TerminalError)
	}
	if !errors.Is(result.RetryableRecords[0].Cause, client.err) {
		t.Fatalf("classified failure lost its cause: %v", result.RetryableRecords[0].Cause)
	}
}

// durable処理を完了できない場合は、failure handlerのerrorだけでなくCloudWatch Logsの拒否そのものを
// errors.Asで判別できるように返します。durable処理が完了した場合は従来どおり成功とします。
func TestCloudWatchLogsFlushManagerKeepsBackendCauseWhenDurableHandlingFails(t *testing.T) {
	rejection := &pkgcloudwatchlogs.HTTPStatusError{
		Code:         http.StatusForbidden,
		Status:       "403 Forbidden",
		AwsErrorType: "AccessDeniedException",
	}
	handlerErr := errors.New("cloudwatch durable handling failed")
	for _, mode := range []string{"put", "flush"} {
		for _, test := range []struct {
			name           string
			handler        flushfailure.FailureHandler
			wantHandlerErr error
		}{
			{name: "without failure handler", wantHandlerErr: flushfailure.ErrNoFailureHandler},
			{name: "failure handler error", handler: &recordingCloudWatchLogsFailureHandler{err: handlerErr}, wantHandlerErr: handlerErr},
			{name: "durable handling succeeded", handler: &recordingCloudWatchLogsFailureHandler{}},
		} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				var composite *flushfailure.CompositeFailureHandler
				if test.handler != nil {
					composite = mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{test.handler})
				}
				flushManager := newTestCloudWatchLogsFlushManager(&recordingCloudWatchLogsClient{err: rejection}, composite)
				entry := newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "rejected by CloudWatch Logs")

				var err error
				if mode == "put" {
					err = flushManager.Put(context.Background(), entry)
				} else {
					err = flushManager.Flush(context.Background(), []*logging.LogEntry{entry})
				}

				if test.wantHandlerErr == nil {
					if err != nil {
						t.Fatalf("error after completed durable handling = %v, want nil", err)
					}
					return
				}
				if !errors.Is(err, test.wantHandlerErr) {
					t.Fatalf("error = %v, want durable handling error %v", err, test.wantHandlerErr)
				}
				var statusErr *pkgcloudwatchlogs.HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
					t.Fatalf("error = %v, want the CloudWatch Logs HTTP 403 rejection", err)
				}
			})
		}
	}
}

// 別のphysical requestが受理された場合も、退避処理を完了できなかったrequestの元のerrorを
// 返してerrors.Asで判別できることを確認します（requestごとの結果に原因を保持します）。
func TestCloudWatchLogsFlushManagerKeepsBackendCauseForPartiallyAcceptedFlush(t *testing.T) {
	rejection := &pkgcloudwatchlogs.HTTPStatusError{
		Code:         http.StatusForbidden,
		Status:       "403 Forbidden",
		AwsErrorType: "AccessDeniedException",
	}
	client := &recordingCloudWatchLogsClient{}
	client.putFunc = func(string, []pkgcloudwatchlogs.Event) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
		if len(client.calls) == 2 {
			return nil, rejection
		}
		return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
	}
	handlerErr := errors.New("cloudwatch durable handling failed")
	handler := &recordingCloudWatchLogsFailureHandler{err: handlerErr}
	// hourly streamへ振り分け、時刻の異なる二件を別のPutLogEvents requestにします。
	flushManager := newTestCloudWatchLogsFlushManagerWithStreamRouting(
		client,
		mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{handler}),
		"stream-a",
		pkgcloudwatchlogs.StreamRoutingHourly,
	)
	occurredAt := time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)
	entries := []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "accepted request", occurredAt),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "rejected request", occurredAt.Add(time.Hour)),
	}

	err := flushManager.Flush(context.Background(), entries)

	if len(client.calls) != 2 {
		t.Fatalf("PutLogEvents calls = %d, want one request per hourly stream", len(client.calls))
	}
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Flush() error = %v, want durable handling error %v", err, handlerErr)
	}
	var statusErr *pkgcloudwatchlogs.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
		t.Fatalf("Flush() error = %v, want the CloudWatch Logs HTTP 403 rejection", err)
	}
	res, called := handler.result()
	if !called || res.FailedCount() != 1 {
		t.Fatalf("failure handler result = %+v (called=%t), want only the rejected request", res, called)
	}
}

func TestCloudWatchLogsFlushManagerFlushWaitsForFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingCloudWatchLogsClient{
			err: &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests"},
		}
		failureHandler := newBlockingCloudWatchLogsFailureHandler()
		handleErr := errors.New("cloudwatch buffered durable handling failed")
		failureHandler.err = handleErr
		t.Cleanup(failureHandler.release)
		composite := mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{failureHandler})
		flushManager := newTestCloudWatchLogsFlushManager(client, composite)

		flushDone := make(chan error, 1)
		go func() {
			err := flushManager.Flush(
				context.Background(),
				[]*logging.LogEntry{newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "buffered cloudwatch")},
			)
			flushDone <- err
		}()

		select {
		case <-failureHandler.started:
		case <-time.After(time.Second):
			t.Fatal("failure handler did not start")
		}

		// Flushはdurable failure処理が確定するまで戻りません。
		synctest.Wait()
		select {
		case err := <-flushDone:
			t.Fatalf("Flush() returned before failure handler completion: %v", err)
		default:
		}

		select {
		case <-failureHandler.finished:
			t.Fatal("failure handler completed before it was released")
		default:
		}

		failureHandler.release()
		select {
		case <-failureHandler.finished:
		case <-time.After(time.Second):
			t.Fatal("failure handler did not finish after release")
		}
		if err := <-flushDone; !errors.Is(err, handleErr) {
			t.Fatalf("Flush() error = %v, want durable handling error", err)
		}
	})
}

func TestCloudWatchLogsFlushManagerCloseWaitsForAcceptedDurableFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingCloudWatchLogsClient{
			err: &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests"},
		}
		failureHandler := newBlockingCloudWatchLogsFailureHandler()
		t.Cleanup(failureHandler.release)
		composite := mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{failureHandler})
		flushManager := newTestCloudWatchLogsFlushManager(client, composite)

		flushDone := make(chan error, 1)
		go func() {
			err := flushManager.Flush(
				context.Background(),
				[]*logging.LogEntry{
					newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "durable failure before close"),
				},
			)
			flushDone <- err
		}()

		select {
		case <-failureHandler.started:
		case <-time.After(time.Second):
			t.Fatal("failure handler did not start")
		}

		// Flushはdurable failure処理が確定するまで戻りません。
		synctest.Wait()
		select {
		case err := <-flushDone:
			t.Fatalf("Flush() returned before durable failure handling completed: %v", err)
		default:
		}

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- flushManager.Close(context.Background())
		}()

		// Closeは、すでに受理したdurable failure処理が完了するまで戻りません。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close() returned before durable failure handling completed: %v", err)
		default:
		}

		failureHandler.release()
		if err := <-flushDone; err != nil {
			t.Fatalf("Flush() error = %v", err)
		}

		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close() did not return after durable failure handling completed")
		}
	})
}

func TestCloudWatchLogsFlushManagerFailureDocumentKeepsSentEvent(t *testing.T) {
	client := &recordingCloudWatchLogsClient{
		err: &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests"},
	}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)
	entry := newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "retryable cloudwatch")

	res, err := flushManager.putEntryAsSingleRequest(context.Background(), entry)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v", err)
	}
	if len(client.events) != 1 {
		t.Fatalf("sent events = %d, want 1", len(client.events))
	}
	if len(res.RetryableRecords) != 1 {
		t.Fatalf("retryable records = %d, want 1", len(res.RetryableRecords))
	}
	sentEvent := client.events[0]
	doc := res.RetryableRecords[0].Payload
	if got, want := doc["timestamp"], sentEvent.Timestamp.Format(time.RFC3339Nano); got != want {
		t.Fatalf("failure document timestamp = %v, want sent event timestamp %s", got, want)
	}
	if got, want := doc["line"], sentEvent.Message; got != want {
		t.Fatalf("failure document line = %v, want sent event message %q", got, want)
	}
}

func TestCloudWatchLogsFlushManagerClassifiesAgeRejectionsByReturnedIndexes(t *testing.T) {
	client := &recordingCloudWatchLogsClient{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooOldLogEventEndIndex:   intPointer(1),
				TooNewLogEventStartIndex: intPointer(2),
			},
		},
	}
	failureHandler := &recordingCloudWatchLogsFailureHandler{}
	composite := mustNewCloudWatchLogsFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	flushManager := newTestCloudWatchLogsFlushManager(client, composite)
	acceptedTime := time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)
	oldTime := acceptedTime.Add(-time.Hour)
	futureTime := acceptedTime.Add(time.Hour)
	entries := []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "accepted cloudwatch", acceptedTime),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too new cloudwatch", futureTime),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too old cloudwatch", oldTime),
	}

	err := flushManager.Flush(context.Background(), entries)

	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	res, ok := failureHandler.result()
	if !ok {
		t.Fatal("failure handler was not invoked")
	}
	if res.TerminalError != nil {
		t.Fatalf("TerminalError = %v, want nil", res.TerminalError)
	}
	if !res.HasSendFailure() {
		t.Fatalf("expected partial rejection to be treated as failure")
	}
	if got := res.SuccessCount(); got != 1 {
		t.Fatalf("SuccessCount = %d, want 1", got)
	}
	if len(res.DeadRecords) != 1 {
		t.Fatalf("dead records = %d, want 1", len(res.DeadRecords))
	}
	if len(res.RetryableRecords) != 1 {
		t.Fatalf("retryable records = %d, want 1", len(res.RetryableRecords))
	}
	if got := res.DeadRecords[0].FailureCode; got != loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeRejectedTooOld) {
		t.Fatalf("permanent FailureCode = %q", got)
	}
	if got := res.DeadRecords[0].Payload["message"]; got != "too old cloudwatch" {
		t.Fatalf("permanent message = %#v, want %q", got, "too old cloudwatch")
	}
	if got := res.RetryableRecords[0].FailureCode; got != loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeRejectedTooNew) {
		t.Fatalf("retryable FailureCode = %q", got)
	}
	if got := res.RetryableRecords[0].Payload["message"]; got != "too new cloudwatch" {
		t.Fatalf("retryable message = %#v, want %q", got, "too new cloudwatch")
	}
	if len(client.events) != 3 {
		t.Fatalf("sent events = %d, want 3", len(client.events))
	}
	if client.events[0].Message == client.events[1].Message || client.events[0].Message == client.events[2].Message {
		t.Fatalf("sent event messages were not distinct: %+v", client.events)
	}
	if !strings.Contains(client.events[0].Message, "too old cloudwatch") ||
		!strings.Contains(client.events[1].Message, "accepted cloudwatch") ||
		!strings.Contains(client.events[2].Message, "too new cloudwatch") {
		t.Fatalf("sent events are not sorted by timestamp: %+v", client.events)
	}
}

func TestCloudWatchLogsFlushManagerKeepsTooNewOnlyRejectionRecordSpecific(t *testing.T) {
	client := &recordingCloudWatchLogsClient{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooNewLogEventStartIndex: intPointer(1),
			},
		},
	}
	failureHandler := &recordingCloudWatchLogsFailureHandler{}
	composite := mustNewCloudWatchLogsFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	flushManager := newTestCloudWatchLogsFlushManager(client, composite)
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

	err := flushManager.Flush(context.Background(), []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "accepted", now),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too new", now.Add(time.Hour)),
	})

	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	result, ok := failureHandler.result()
	if !ok {
		t.Fatal("failure handler was not invoked")
	}
	if got := result.SuccessCount(); got != 1 {
		t.Fatalf("SuccessCount() = %d, want 1", got)
	}
	if len(result.RetryableRecords) != 1 || len(result.DeadRecords) != 0 {
		t.Fatalf("records = retryable:%d dead:%d, want 1/0", len(result.RetryableRecords), len(result.DeadRecords))
	}
	if got := result.RetryableRecords[0].Payload["message"]; got != "too new" {
		t.Fatalf("retryable message = %#v, want %q", got, "too new")
	}
}

func TestCloudWatchLogsFlushManagerFlushClassifiesInvalidEventRecordsBeforeClient(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	failureHandler := &recordingCloudWatchLogsFailureHandler{}
	composite := mustNewCloudWatchLogsFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	flushManager := newTestCloudWatchLogsFlushManager(client, composite)
	now := time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)
	entries := []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "valid cloudwatch", now),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "", now),
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			strings.Repeat("x", pkgcloudwatchlogs.MaxPutLogEventBytes+1),
			now,
		),
		nil,
	}

	err := flushManager.Flush(context.Background(), entries)

	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	res, ok := failureHandler.result()
	if !ok {
		t.Fatal("failure handler was not invoked")
	}
	if res.TerminalError != nil {
		t.Fatalf("TerminalError = %v, want nil", res.TerminalError)
	}
	if got := res.Meta.InputCount; got != 3 {
		t.Fatalf("failure handler input count = %d, want 3 invalid records", got)
	}
	if len(client.events) != 1 {
		t.Fatalf("client events = %d, want 1", len(client.events))
	}
	if !strings.Contains(client.events[0].Message, "valid cloudwatch") {
		t.Fatalf("sent event message = %q, want valid record", client.events[0].Message)
	}
	if len(res.RetryableRecords) != 0 {
		t.Fatalf("retryable records = %d, want 0", len(res.RetryableRecords))
	}
	if len(res.QuarantineRecords) != 3 {
		t.Fatalf("quarantine records = %d, want 3", len(res.QuarantineRecords))
	}
	if res.QuarantineRecords[0].FailureCode != loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeInvalidEventMessageEmpty) {
		t.Fatalf("first FailureCode = %q, want %q", res.QuarantineRecords[0].FailureCode, infracloudwatchlogs.FailureCodeInvalidEventMessageEmpty)
	}
	if res.QuarantineRecords[1].FailureCode != loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeInvalidEventTooLarge) {
		t.Fatalf("second FailureCode = %q, want %q", res.QuarantineRecords[1].FailureCode, infracloudwatchlogs.FailureCodeInvalidEventTooLarge)
	}
	if res.QuarantineRecords[2].FailureCode != loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeInvalidEventMessageEmpty) {
		t.Fatalf("third FailureCode = %q, want %q", res.QuarantineRecords[2].FailureCode, infracloudwatchlogs.FailureCodeInvalidEventMessageEmpty)
	}
}

func TestCloudWatchLogsFlushManagerPutEntryKeepsLocalInvalidRecordQuarantined(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	result, err := flushManager.putEntryAsSingleRequest(
		context.Background(),
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, ""),
	)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v", err)
	}
	if len(result.QuarantineRecords) != 1 {
		t.Fatalf("quarantine records = %d, want 1", len(result.QuarantineRecords))
	}
	if len(result.RetryableRecords) != 0 || len(result.DeadRecords) != 0 {
		t.Fatalf(
			"unexpected records = retryable:%d dead:%d",
			len(result.RetryableRecords),
			len(result.DeadRecords),
		)
	}
	if got := result.QuarantineRecords[0].FailureCode; got != loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeInvalidEventMessageEmpty) {
		t.Fatalf("FailureCode = %q, want %q", got, infracloudwatchlogs.FailureCodeInvalidEventMessageEmpty)
	}
	if len(client.events) != 0 {
		t.Fatalf("client events = %d, want 0", len(client.events))
	}
}

func TestCloudWatchLogsFlushManagerFlushSendsMultiplePhysicalRequests(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)
	first := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	last := first.Add(24*time.Hour + time.Millisecond)
	var gotErr error
	flushManager.OnError = func(
		ctx context.Context,
		err error,
		entry *logging.LogEntry,
		tracer pkgobserver.Tracer,
		meter pkgobserver.Meter,
	) {
		gotErr = err
	}

	err := flushManager.Flush(context.Background(), []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "first cloudwatch", first),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "last cloudwatch", last),
	})

	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if gotErr != nil {
		t.Fatalf("OnError err = %v, want no notification", gotErr)
	}
	if len(client.calls) != 2 {
		t.Fatalf("PutLogEvents request count = %d, want 2", len(client.calls))
	}
	if len(client.calls[0].events) != 1 || len(client.calls[1].events) != 1 {
		t.Fatalf("PutLogEvents request event counts = [%d, %d], want [1, 1]",
			len(client.calls[0].events), len(client.calls[1].events))
	}
}

func TestCloudWatchLogsFlushManagerFlushKeepsAcceptedEntryCompletedWhenRejectedDurableHandlingFails(
	t *testing.T,
) {
	handleErr := errors.New("cloudwatch rejected event durable handling failed")
	failureHandler := &recordingCloudWatchLogsFailureHandler{err: handleErr}
	composite := mustNewCloudWatchLogsFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	client := &recordingCloudWatchLogsClient{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooNewLogEventStartIndex: intPointer(1),
			},
		},
	}
	flushManager := newTestCloudWatchLogsFlushManager(client, composite)
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

	// input順と送信順を逆にし、AWSのrejection indexを元bufferのindexへ正しく戻せる
	// ことも同時に確認します。送信順はaccepted、too newです。
	err := flushManager.Flush(context.Background(), []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too new", now.Add(time.Hour)),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "accepted", now),
	})

	if !errors.Is(err, handleErr) {
		t.Fatalf("Flush() error = %v, want %v", err, handleErr)
	}
	if len(client.calls) != 1 {
		t.Fatalf("PutLogEvents request count = %d, want 1", len(client.calls))
	}
	result, called := failureHandler.result()
	if !called || result.SuccessCount() != 1 || result.TerminalError != nil {
		t.Fatalf("classified rejection must retain the accepted event: %+v", result)
	}
	// 退避処理を完了できなかった場合も、応答内で拒否されたeventの理由を返します。
	var flushRejection *pkgcloudwatchlogs.RejectedLogEventError
	if !errors.As(err, &flushRejection) || flushRejection.Reason != pkgcloudwatchlogs.RejectedLogEventReasonTooNew {
		t.Fatalf("Flush() error = %v, want the too_new rejection joined", err)
	}
	target := cloudWatchLogsTargetDeliveryResult(result)
	var targetRejection *pkgcloudwatchlogs.RejectedLogEventError
	if target.Status != pkglogger.DeliveryFailed || !errors.As(target.Err, &targetRejection) ||
		targetRejection.Reason != pkgcloudwatchlogs.RejectedLogEventReasonTooNew {
		t.Fatalf("partial rejection cause missing from target: %+v", target)
	}
}

func TestCloudWatchLogsFlushManagerFlushContinuesAfterDurableFailureWithoutResendingCompletedRequests(
	t *testing.T,
) {
	handleErr := errors.New("second physical request durable handling failed")
	failureHandler := &recordingCloudWatchLogsFailureHandler{err: handleErr}
	composite := mustNewCloudWatchLogsFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	putCalls := 0
	client := &recordingCloudWatchLogsClient{
		putFunc: func(
			_ string,
			_ []pkgcloudwatchlogs.Event,
		) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
			putCalls++
			if putCalls == 2 {
				return nil, &pkgcloudwatchlogs.HTTPStatusError{
					Code:   http.StatusTooManyRequests,
					Status: "429 Too Many Requests",
				}
			}
			return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
		},
	}
	flushManager := newTestCloudWatchLogsFlushManagerWithStreamRouting(
		client,
		composite,
		"stream-a",
		pkgcloudwatchlogs.StreamRoutingHourly,
	)
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

	err := flushManager.Flush(context.Background(), []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "unstarted hour 14", base.Add(2*time.Hour)),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "completed hour 12", base),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "failed hour 13", base.Add(time.Hour)),
	})

	if !errors.Is(err, handleErr) {
		t.Fatalf("Flush() error = %v, want %v", err, handleErr)
	}
	if len(client.calls) != 3 {
		t.Fatalf("PutLogEvents request count = %d, want 3 independent attempts", len(client.calls))
	}
	for i, message := range []string{"completed hour 12", "failed hour 13", "unstarted hour 14"} {
		if len(client.calls[i].events) != 1 || !strings.Contains(client.calls[i].events[0].Message, message) {
			t.Fatalf("request %d = %+v, want only %q", i, client.calls[i], message)
		}
	}
	result, called := failureHandler.result()
	if !called || len(result.RetryableRecords) != 1 || result.RetryableRecords[0].Payload["message"] != "failed hour 13" {
		t.Fatalf("failure handling must contain only the rejected request: %+v", result)
	}
}

func TestCloudWatchLogsFlushManagerPutEntryKeepsSuccessWhenContextCanceledAfterHTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &recordingCloudWatchLogsClient{
		onPut: cancel,
	}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	res, err := flushManager.putEntryAsSingleRequest(
		ctx,
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "success before context cancellation"),
	)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v, want nil", err)
	}
	if res.HasSendFailure() {
		t.Fatalf("result has failure: %+v", res)
	}
	if len(client.events) != 1 {
		t.Fatalf("sent events = %d, want 1", len(client.events))
	}
}

func TestCloudWatchLogsFlushManagerPutEntryClassifiesNilOutputAsQuarantine(t *testing.T) {
	client := &recordingCloudWatchLogsClient{returnNilOutput: true}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	res, err := flushManager.putEntryAsSingleRequest(
		context.Background(),
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "nil output"),
	)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v, want classified result", err)
	}
	if len(res.QuarantineRecords) != 1 || len(res.DeadRecords) != 0 {
		t.Fatalf(
			"records = quarantine:%d dead:%d, want 1/0",
			len(res.QuarantineRecords),
			len(res.DeadRecords),
		)
	}
	if quarantined := res.QuarantineRecords[0]; !errors.Is(quarantined.Cause, pkgcloudwatchlogs.ErrNilPutLogEventsOutput) ||
		quarantined.FailureReason != quarantined.Cause.Error() {
		t.Fatalf("quarantined record = %+v, want the nil output error kept as its cause", quarantined)
	}
	if len(client.events) != 1 {
		t.Fatalf("sent events = %d, want 1", len(client.events))
	}
}

func TestCloudWatchLogsFlushManagerPutEntryClassifiesCircuitBreakerOpenAsRetryable(t *testing.T) {
	circuitBreakerErr := retryable.NewCircuitBreakerOpenError(
		"cloudwatch logs send",
		time.Now().Add(time.Minute),
	)
	client := &recordingCloudWatchLogsClient{err: circuitBreakerErr}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	res, err := flushManager.putEntryAsSingleRequest(
		context.Background(),
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "circuit breaker open"),
	)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v, want classified result", err)
	}
	if len(res.RetryableRecords) != 1 || len(res.DeadRecords) != 0 {
		t.Fatalf(
			"records = retryable:%d dead:%d, want 1/0",
			len(res.RetryableRecords),
			len(res.DeadRecords),
		)
	}
	if got := res.RetryableRecords[0].FailureCode; got != loggerfailure.FailureCode(retryable.RetryReasonCircuitBreakerOpen) {
		t.Fatalf("FailureCode = %q, want %q", got, retryable.RetryReasonCircuitBreakerOpen)
	}
}

func TestCloudWatchLogsFlushManagerPutEntryQuarantinesUnknownError(t *testing.T) {
	unknownErr := errors.New("unclassified result")
	client := &recordingCloudWatchLogsClient{err: unknownErr}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	res, err := flushManager.putEntryAsSingleRequest(
		context.Background(),
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "unknown failure"),
	)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v, want classified result", err)
	}
	if len(res.DeadRecords) != 0 || len(res.QuarantineRecords) != 1 {
		t.Fatalf(
			"records = dead:%d quarantine:%d, want 0/1",
			len(res.DeadRecords),
			len(res.QuarantineRecords),
		)
	}
	if len(res.RetryableRecords) != 0 {
		t.Fatalf("unknown outcome was queued for automatic replay: %+v", res)
	}
}

func TestCloudWatchLogsFlushManagerFlushWithNoFallbackLogsPartialRejection(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	}()

	client := &recordingCloudWatchLogsClient{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooNewLogEventStartIndex: intPointer(0),
			},
		},
	}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	flushManager.FlushWithNoFallback(context.Background(), []*logging.LogEntry{
		newTestCloudWatchLogsEntry(pkglogger.LogLevelWarn, "fallback too new"),
	})

	got := buf.String()
	if !strings.Contains(got, "cloudwatch logs fallback logger: cloudwatch logs rejected 1 log event(s)") {
		t.Fatalf("fallback log output = %q, want partial rejection log", got)
	}
}

func TestCloudWatchLogsFlushManagerFlushWithNoFallbackLogsNilOutputSuccess(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	}()

	client := &recordingCloudWatchLogsClient{returnNilOutput: true}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)

	flushManager.FlushWithNoFallback(context.Background(), []*logging.LogEntry{
		newTestCloudWatchLogsEntry(pkglogger.LogLevelWarn, "fallback nil output"),
	})

	got := buf.String()
	if !strings.Contains(got, "nil PutLogEventsOutput") {
		t.Fatalf("fallback log output = %q, want nil output log", got)
	}
}

func TestFallbackLoggerEnqueuesAndFlushesWithNoFallback(t *testing.T) {
	flushManager := &recordingFallbackCloudWatchLogsFlushManager{}
	cfg := newTestCloudWatchLogsLoggerConfig(2, time.Hour, false)
	fallbackLogger := newCloudWatchLogsFallbackBatchTestLogger(cfg, flushManager)
	fallbackLogger.startFallbackWorker()
	first := newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "first fallback entry")
	second := newTestCloudWatchLogsEntry(pkglogger.LogLevelError, "second fallback entry")

	fallbackLogger.enqueueProcess(first)
	fallbackLogger.enqueueProcess(second)
	if err := fallbackLogger.Close(context.Background()); err != nil {
		t.Fatalf("close fallback logger: %v", err)
	}

	if flushManager.noFallbackCalls != 1 {
		t.Fatalf("expected one FlushWithNoFallback call, got %d", flushManager.noFallbackCalls)
	}
	if flushManager.putCalls != 0 {
		t.Fatalf("expected no Put calls, got %d", flushManager.putCalls)
	}
	if len(flushManager.noFallbackEntries) != 2 {
		t.Fatalf("expected two no-fallback entries, got %d", len(flushManager.noFallbackEntries))
	}
	if flushManager.noFallbackEntries[0] != first || flushManager.noFallbackEntries[1] != second {
		t.Fatal("expected no-fallback entries to be passed through in order")
	}
}

func TestCloudWatchLogsLoggerOptionsUseClockAndTimeProvider(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	clockTime := time.Date(2026, 6, 24, 1, 2, 3, 0, time.UTC)
	providerTime := time.Date(2026, 6, 24, 4, 5, 6, 0, time.UTC)
	logger, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(10, time.Hour, true),
		client,
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedCloudWatchLogsClock{t: clockTime}),
		WithTimeProvider(func() time.Time { return providerTime }),
	)
	if err != nil {
		t.Fatalf("NewCloudWatchLogsLogger() error = %v", err)
	}
	defer func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if err := handleTestInfo(context.Background(), logger, "option clock message"); err != nil {
		t.Fatalf("HandleLogEntry() error = %v", err)
	}
	if len(client.events) != 1 {
		t.Fatalf("sent events = %d, want 1", len(client.events))
	}
	if got := client.events[0].Timestamp; !got.Equal(clockTime) {
		t.Fatalf("event timestamp = %s, want %s", got, clockTime)
	}

	logger.updateLastFlushTime()
	if !logger.lastFlushTime.Equal(providerTime) {
		t.Fatalf("lastFlushTime = %s, want %s", logger.lastFlushTime, providerTime)
	}
}

func TestNewCloudWatchLogsLoggerAllowsBufferFlushMaxEventsAbovePutLogEventsLimit(t *testing.T) {
	logger, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(
			pkgcloudwatchlogs.MaxPutLogEventsCount+1,
			time.Second,
			false,
		),
		&recordingCloudWatchLogsClient{},
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewCloudWatchLogsLogger() error = %v, want logical buffer threshold to remain independent from the physical request limit", err)
	}
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewCloudWatchLogsLoggerRejectsNonPositiveBufferFlushMaxEvents(t *testing.T) {
	for _, maxEvents := range []int{-1, 0} {
		t.Run(fmt.Sprintf("max_events_%d", maxEvents), func(t *testing.T) {
			_, err := NewCloudWatchLogsLogger(
				context.Background(),
				newTestCloudWatchLogsLoggerConfig(maxEvents, time.Second, false),
				&recordingCloudWatchLogsClient{},
				newTestCloudWatchLogsFlushManagerConfig(),
				newTestCloudWatchLogsResourceProvider(),
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				&LoggerBase{},
			)
			requireCloudWatchLogsConfigFieldError(
				t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldBufferFlushMaxEvents, maxEvents,
			)
		})
	}
}

func TestNewCloudWatchLogsLoggerRejectsNonPositiveFlushInterval(t *testing.T) {
	_, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(1, 0, false),
		&recordingCloudWatchLogsClient{},
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	requireCloudWatchLogsConfigFieldError(
		t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldFlushInterval, time.Duration(0),
	)
}

func TestCloudWatchLogsLoggerOptionUsesFlushIntervalGenerator(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	flushInterval := 30 * time.Second
	var gotDuration time.Duration
	logger, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(10, flushInterval, false),
		client,
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithFlushIntervalGenerator(func(d time.Duration) *time.Ticker {
			gotDuration = d
			return time.NewTicker(time.Hour)
		}),
	)
	if err != nil {
		t.Fatalf("NewCloudWatchLogsLogger() error = %v", err)
	}
	defer func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if gotDuration != flushInterval {
		t.Fatalf("flush interval generator duration = %s, want %s", gotDuration, flushInterval)
	}
}

type recordingCloudWatchLogsClient struct {
	err             error
	output          *pkgcloudwatchlogs.PutLogEventsOutput
	returnNilOutput bool
	events          []pkgcloudwatchlogs.Event
	onPut           func()
	sentAts         []time.Time
	streamNames     []string
	calls           []recordingCloudWatchLogsPutCall
	putFunc         func(string, []pkgcloudwatchlogs.Event) (*pkgcloudwatchlogs.PutLogEventsOutput, error)
}

type recordingCloudWatchLogsPutCall struct {
	streamName string
	events     []pkgcloudwatchlogs.Event
	sentAt     time.Time
}

func (c *recordingCloudWatchLogsClient) PutLogEvents(
	ctx context.Context,
	request pkgcloudwatchlogs.PutLogEventsRequest,
) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
	c.sentAts = append(c.sentAts, request.AttemptedAt)
	c.streamNames = append(c.streamNames, request.LogStreamName)
	c.calls = append(c.calls, recordingCloudWatchLogsPutCall{
		streamName: request.LogStreamName,
		events:     append([]pkgcloudwatchlogs.Event(nil), request.Events...),
		sentAt:     request.AttemptedAt,
	})
	return c.send(ctx, request.LogStreamName, request.Events)
}

func (c *recordingCloudWatchLogsClient) send(
	_ context.Context,
	streamName string,
	events []pkgcloudwatchlogs.Event,
) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
	c.events = append(c.events, events...)
	if c.onPut != nil {
		c.onPut()
	}
	if c.putFunc != nil {
		return c.putFunc(streamName, events)
	}
	if c.returnNilOutput {
		return nil, c.err
	}
	if c.output != nil || c.err != nil {
		return c.output, c.err
	}
	return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
}

type recordingCloudWatchLogsFailureHandler struct {
	mu     sync.Mutex
	called bool
	res    loggerfailure.SendResult
	err    error
}

func (h *recordingCloudWatchLogsFailureHandler) HandleFailure(
	ctx context.Context,
	r loggerfailure.SendResult,
) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.called = true
	h.res = r
	return h.err
}

func (h *recordingCloudWatchLogsFailureHandler) result() (loggerfailure.SendResult, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.res, h.called
}

type blockingCloudWatchLogsFailureHandler struct {
	started     chan struct{}
	unblock     chan struct{}
	finished    chan struct{}
	releaseOnce sync.Once
	err         error
}

func newBlockingCloudWatchLogsFailureHandler() *blockingCloudWatchLogsFailureHandler {
	return &blockingCloudWatchLogsFailureHandler{
		started:  make(chan struct{}),
		unblock:  make(chan struct{}),
		finished: make(chan struct{}),
	}
}

func (h *blockingCloudWatchLogsFailureHandler) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	close(h.started)
	<-h.unblock
	close(h.finished)
	return h.err
}

func (h *blockingCloudWatchLogsFailureHandler) release() {
	h.releaseOnce.Do(func() {
		close(h.unblock)
	})
}

type recordingFallbackCloudWatchLogsFlushManager struct {
	putCalls          int
	noFallbackCalls   int
	noFallbackEntries []*logging.LogEntry
}

func (m *recordingFallbackCloudWatchLogsFlushManager) Flush(
	ctx context.Context,
	entries []*logging.LogEntry,
) error {
	return nil
}

func (m *recordingFallbackCloudWatchLogsFlushManager) Put(ctx context.Context, entry *logging.LogEntry) error {
	m.putCalls++
	return nil
}

func (m *recordingFallbackCloudWatchLogsFlushManager) PutWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error) {
	m.putCalls++
	return pkglogger.DeliveryReport{}, nil
}

func (m *recordingFallbackCloudWatchLogsFlushManager) FlushWithNoFallback(
	_ context.Context,
	entries []*logging.LogEntry,
) {
	m.noFallbackCalls++
	m.noFallbackEntries = append(m.noFallbackEntries, entries...)
}

func (m *recordingFallbackCloudWatchLogsFlushManager) Close(context.Context) error {
	return nil
}

func newTestCloudWatchLogsFlushManager(
	client pkgcloudwatchlogs.Client,
	failureHandler *flushfailure.CompositeFailureHandler,
) *CloudWatchLogsFlushManager {
	return newTestCloudWatchLogsFlushManagerWithStreamRouting(
		client,
		failureHandler,
		"stream-a",
		pkgcloudwatchlogs.StreamRoutingNone,
	)
}

func newTestCloudWatchLogsFlushManagerWithStreamRouting(
	client pkgcloudwatchlogs.Client,
	failureHandler *flushfailure.CompositeFailureHandler,
	baseStreamName string,
	routingGranularity pkgcloudwatchlogs.StreamRouting,
) *CloudWatchLogsFlushManager {
	loggerConfig := newTestCloudWatchLogsLoggerConfig(100, time.Hour, false)
	flushConfig := newTestCloudWatchLogsFlushManagerConfig()
	resourceProvider := newTestCloudWatchLogsResourceProvider()
	loggerBase := &LoggerBase{
		Clock:        fixedCloudWatchLogsClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)},
		LogFormatter: cloudWatchLogsTestPlainTextFormatter,
		config:       loggerConfig,
		Tracer:       observer.NopTracer{},
		Meter:        observer.NopMeter{},
	}
	streamNameResolver, err := infracloudwatchlogs.NewLogStreamNameResolver(
		baseStreamName,
		routingGranularity,
	)
	if err != nil {
		// test fixtureの固定設定なので、失敗は呼び出し対象ではなくfixture自体の不具合です。
		panic(err)
	}

	return NewCloudWatchLogsFlushManager(
		loggerBase,
		client,
		streamNameResolver,
		flushConfig,
		failureHandler,
		resourceProvider,
		observer.NopTracer{},
		observer.NopMeter{},
	)
}

func newTestCloudWatchLogsFlushManagerWithReportingFailureHandler(
	client pkgcloudwatchlogs.Client,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
) *CloudWatchLogsFlushManager {
	var baseFailureHandler *flushfailure.CompositeFailureHandler
	if failureHandler != nil {
		baseFailureHandler = failureHandler.CompositeFailureHandler
	}
	fm := newTestCloudWatchLogsFlushManager(client, baseFailureHandler)
	fm.FailureReportHandler = failureHandler
	return fm
}

func newTestCloudWatchLogsLoggerConfig(
	maxEvents int,
	flushInterval time.Duration,
	immediatePut bool,
) CloudWatchLogsConfigProvider {
	return newTestCloudWatchLogsLoggerConfigWithBufferLimits(
		maxEvents,
		64*1024*1024,
		maxEvents,
		flushInterval,
		immediatePut,
	)
}

func newTestCloudWatchLogsLoggerConfigWithBufferLimits(
	bufferFlushMaxEvents int,
	bufferFlushBytes int,
	fallbackQueueMaxEvents int,
	flushInterval time.Duration,
	immediatePut bool,
) CloudWatchLogsConfigProvider {
	return NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "runtime-app",
			Target:   "stg",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          3,
			ErrorTraceDepthDebugMode: 10,
		},
		FlushInterval:          flushInterval,
		BufferFlushMaxEvents:   bufferFlushMaxEvents,
		BufferFlushBytes:       bufferFlushBytes,
		FallbackQueueMaxEvents: fallbackQueueMaxEvents,
		NoBulkFlushModeEnabled: immediatePut,
	})
}

func newTestCloudWatchLogsFlushManagerConfig() CloudWatchLogsFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
		LogGroupName:  "/app/test",
		LogStreamName: "stream-a",
		Resource: pkglogger.Resource{
			ServiceName: "cloudwatch-test-app",
			HostName:    "cloudwatch-test-host",
			Environment: "prd",
			Cloud:       &pkglogger.CloudResourceInfo{},
		},
	}, pkgcloudwatchlogs.StreamRoutingNone)
}

func newTestCloudWatchLogsResourceProvider() resource.ResourceProvider {
	return resource.NewResourceAdapter(pkglogger.Resource{
		ServiceName: "cloudwatch-test-app",
		HostName:    "cloudwatch-test-host",
		Environment: "prd",
		Cloud:       &pkglogger.CloudResourceInfo{},
	})
}

func newTestCloudWatchLogsEntry(level pkglogger.LogLevel, message string) *logging.LogEntry {
	return newTestCloudWatchLogsEntryAt(level, message, time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC))
}

func newTestCloudWatchLogsEntryAt(level pkglogger.LogLevel, message string, timestamp time.Time) *logging.LogEntry {
	return &logging.LogEntry{
		OccurredAt: timestamp.UTC(),
		Level:      level,
		RequestID:  "req-cloudwatch-test",
		TraceID:    "trace-cloudwatch-test",
		SpanID:     "span-cloudwatch-test",
		Message:    message,
		Fields: map[string]any{
			"case": "cloudwatch-test",
		},
	}
}

func intPointer(v int) *int {
	return &v
}

type fixedCloudWatchLogsClock struct {
	t time.Time
}

func (c fixedCloudWatchLogsClock) Now() time.Time {
	return c.t
}

var _ clock.Clock = fixedCloudWatchLogsClock{}

type advancingCloudWatchLogsClock struct {
	mu   sync.Mutex
	next time.Time
}

func (c *advancingCloudWatchLogsClock) Now() time.Time {
	return c.nextTime()
}

func (c *advancingCloudWatchLogsClock) nextTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := c.next
	c.next = c.next.Add(time.Second)
	return t
}

var _ clock.Clock = (*advancingCloudWatchLogsClock)(nil)

func TestCloudWatchLogsFlushManagerQuarantinesUncertainResponses(t *testing.T) {
	for _, index := range []int{-1, 1, 2} {
		client := &recordingCloudWatchLogsClient{output: &pkgcloudwatchlogs.PutLogEventsOutput{RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{TooNewLogEventStartIndex: &index}}}
		manager := newTestCloudWatchLogsFlushManager(client, nil)
		result, err := manager.putEntryAsSingleRequest(context.Background(), newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "uncertain response"))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.QuarantineRecords) != 1 || len(result.RetryableRecords) != 0 || len(result.DeadRecords) != 0 {
			t.Fatalf("unexpected disposition: %+v", result)
		}
		if result.TerminalError == nil || len(client.events) != 1 {
			t.Fatalf("missing cause or backend attempt: %+v", result)
		}
	}
}
