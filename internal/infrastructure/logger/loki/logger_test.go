package loki

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	lokiretryclassifier "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func mustNewLokiFailureComposite(
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

func mustNewLokiReportingFailureComposite(
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

func TestLokiFlushManagerPushLogEntriesClassifiesIngestionBlockedAsRetryable(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   260,
			Status: "260 Ingestion Blocked",
			Body:   []byte("ingestion blocked"),
		},
	}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelError, "blocked log"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if !res.HasSendFailure() {
		t.Fatal("expected send result to have failure")
	}
	if res.TerminalError == nil {
		t.Fatal("expected TerminalError to be set")
	}
	if len(res.DeadRecords) != 0 {
		t.Fatalf("expected no dead records, got %d", len(res.DeadRecords))
	}
	if len(res.RetryableRecords) != 1 {
		t.Fatalf("expected one retryable record, got %d", len(res.RetryableRecords))
	}

	failed := res.RetryableRecords[0]
	if failed.StatusCode != 260 {
		t.Fatalf("expected status 260, got %d", failed.StatusCode)
	}
	if failed.FailureCode != loggerfailure.FailureCode(lokiretryclassifier.RetryReasonIngestionBlocked) {
		t.Fatalf("expected failure code %q, got %q", lokiretryclassifier.RetryReasonIngestionBlocked, failed.FailureCode)
	}
	labels := failed.Payload["labels"].(map[string]string)
	assertLokiLabels(t, labels, "runtime-app", "error", "prd")
	assertPushedLokiLabels(t, client, "runtime-app", "error", "prd")
}

func TestLokiFlushManagerPushLogEntriesQuarantinesBadRequest(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusBadRequest,
			Status: "400 Bad Request",
			Body:   []byte("invalid stream"),
		},
	}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelWarn, "bad request log"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if len(res.RetryableRecords) != 0 {
		t.Fatalf("expected no retryable records, got %d", len(res.RetryableRecords))
	}
	// 400は入力不備として内容を確認するまで隔離し、配送不能とは確定しません。
	if len(res.DeadRecords) != 0 {
		t.Fatalf("expected no dead records, got %d", len(res.DeadRecords))
	}
	if len(res.QuarantineRecords) != 1 {
		t.Fatalf("expected one quarantine record, got %d", len(res.QuarantineRecords))
	}

	failed := res.QuarantineRecords[0]
	if failed.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, failed.StatusCode)
	}
	if failed.FailureCode != loggerfailure.FailureCode(lokiretryclassifier.RetryReasonPushRejected) {
		t.Fatalf("expected failure code %q, got %q", lokiretryclassifier.RetryReasonPushRejected, failed.FailureCode)
	}
	// 明示的な拒否応答なので、送達結果は成否不明ではなく失敗です。
	if failed.DeliveryStatus != pkglogger.DeliveryFailed {
		t.Fatalf("expected delivery status %q, got %q", pkglogger.DeliveryFailed, failed.DeliveryStatus)
	}

	labels := failed.Payload["labels"].(map[string]string)
	assertLokiLabels(t, labels, "runtime-app", "warn", "prd")
}

func TestLokiFlushManagerPushLogEntriesClassifiesCircuitBreakerOpenAsRetryable(t *testing.T) {
	circuitBreakerErr := retryable.NewCircuitBreakerOpenError(
		"loki push",
		time.Now().Add(time.Minute),
	)
	client := &recordingFlushManagerLokiClient{pushErr: circuitBreakerErr}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelError, "circuit breaker open"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if !errors.Is(res.TerminalError, circuitBreakerErr) {
		t.Fatalf("TerminalError = %v, want %v", res.TerminalError, circuitBreakerErr)
	}
	if len(res.RetryableRecords) != 1 {
		t.Fatalf("expected one retryable record, got %d", len(res.RetryableRecords))
	}
	if len(res.DeadRecords) != 0 {
		t.Fatalf("expected no dead records, got %d", len(res.DeadRecords))
	}
	if got := res.RetryableRecords[0].FailureCode; got != loggerfailure.FailureCode(retryable.RetryReasonCircuitBreakerOpen) {
		t.Fatalf("FailureCode = %q, want %q", got, retryable.RetryReasonCircuitBreakerOpen)
	}
}

func TestLokiFlushManagerPushLogEntriesQuarantinesUnknownError(t *testing.T) {
	unknownErr := errors.New("unclassified result")
	client := &recordingFlushManagerLokiClient{pushErr: unknownErr}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelError, "unknown failure"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if !errors.Is(res.TerminalError, unknownErr) {
		t.Fatalf("TerminalError = %v, want %v", res.TerminalError, unknownErr)
	}
	if len(res.DeadRecords) != 0 {
		t.Fatalf("expected no dead records, got %d", len(res.DeadRecords))
	}
	if len(res.QuarantineRecords) != 1 {
		t.Fatalf("expected one quarantine record, got %d", len(res.QuarantineRecords))
	}
	if len(res.RetryableRecords) != 0 {
		t.Fatalf("unknown outcome was queued for automatic replay: %+v", res)
	}
}

func TestLokiFlushManagerQuarantinesBadRequestWithoutRecordIsolation(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusBadRequest,
			Status: "400 Bad Request",
			Body:   []byte("request rejected"),
		},
	}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelWarn, "first log"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "second log"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	// Lokiは400でも有効なstreamを受理している可能性があり、分けて再送すると重複し得ます。
	if client.pushCalls != 1 {
		t.Fatalf("Push call count = %d, want 1; rejected request must not be resent to isolate records", client.pushCalls)
	}
	if len(res.RetryableRecords) != 0 || len(res.DeadRecords) != 0 {
		t.Fatalf("expected only quarantine records, got retryable=%d dead=%d", len(res.RetryableRecords), len(res.DeadRecords))
	}
	if len(res.QuarantineRecords) != len(entries) {
		t.Fatalf("expected %d quarantine records, got %d", len(entries), len(res.QuarantineRecords))
	}

	wantMessages := []string{"first log", "second log"}
	for index, failed := range res.QuarantineRecords {
		if failed.StatusCode != http.StatusBadRequest {
			t.Fatalf("quarantined failure[%d] status = %d, want %d", index, failed.StatusCode, http.StatusBadRequest)
		}
		if got := failed.Payload["message"]; got != wantMessages[index] {
			t.Fatalf("quarantined failure[%d] message = %v, want %q", index, got, wantMessages[index])
		}
	}
}

func TestLokiFlushManagerSplitsRequestRejectedAsTooLarge(t *testing.T) {
	var pushed [][]string
	client := &recordingFlushManagerLokiClient{}
	client.pushFunc = func(streams []pkgloki.Stream) error {
		var messages []string
		for _, stream := range streams {
			for _, entry := range stream.Entries {
				messages = append(messages, entry.Line)
			}
		}
		pushed = append(pushed, messages)
		// 中継先の上限が計画時の上限より小さく、二件以上のpushと単独の大きなentryを拒否します。
		if len(messages) > 1 || strings.Contains(messages[0], "too large even alone") {
			return &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		return nil
	}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "first log"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "too large even alone"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "third log"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	// 413ではどのentryも受理されていないため、分けて再送し、正常なentryを配送します。
	// 分割の順序や回数ではなく、正常なentryがそれぞれ一度だけ受理されることを検証します。
	// 二分割では最大でも 2*入力件数-1 回の送信で単独のentryまで分けられます。
	if maxCalls := 2*len(entries) - 1; len(pushed) > maxCalls {
		t.Fatalf("Push calls = %d (%v), want at most %d", len(pushed), pushed, maxCalls)
	}
	for _, message := range []string{"first log", "third log"} {
		accepted := 0
		for _, messages := range pushed {
			// このclientは単独で送られた正常なentryだけを受理します。
			if len(messages) == 1 && strings.Contains(messages[0], message) {
				accepted++
			}
		}
		if accepted != 1 {
			t.Fatalf("entry %q accepted %d times (%v), want exactly once", message, accepted, pushed)
		}
	}
	if res.TerminalError != nil {
		t.Fatalf("TerminalError = %v, want nil after partially successful split pushes", res.TerminalError)
	}
	if len(res.RetryableRecords) != 0 || len(res.DeadRecords) != 0 {
		t.Fatalf("expected only quarantine records, got retryable=%d dead=%d", len(res.RetryableRecords), len(res.DeadRecords))
	}
	// 一件でも拒否されたentryは、上限設定の変更で配送できる可能性があるため隔離します。
	if len(res.QuarantineRecords) != 1 {
		t.Fatalf("expected one quarantine record, got %d", len(res.QuarantineRecords))
	}
	quarantined := res.QuarantineRecords[0]
	if quarantined.Payload["message"] != "too large even alone" || quarantined.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("quarantined record = %+v, want the oversized entry with HTTP 413", quarantined)
	}
	if quarantined.FailureCode != loggerfailure.FailureCode(retryable.RetryReasonRequestTooLarge) {
		t.Fatalf("FailureCode = %q, want %q", quarantined.FailureCode, retryable.RetryReasonRequestTooLarge)
	}
}

func TestLokiFlushManagerKeepsUnsentSplitEntriesRetryableWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &recordingFlushManagerLokiClient{}
	client.pushFunc = func(streams []pkgloki.Stream) error {
		if lokiEntryCount(streams) > 1 {
			return &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		// 分割後の一件目を送信した直後に、呼び出し元が停止します。
		cancel()
		return nil
	}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "sent after split"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "unsent after split"),
	}

	res, err := flushManager.pushLogEntries(ctx, entries)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pushLogEntries error = %v, want context.Canceled", err)
	}
	if client.pushCalls != 2 {
		t.Fatalf("Push calls = %d, want 2", client.pushCalls)
	}
	// 413ではどのentryも受理されていないため、未送信の後半は確定した413を保持して再送対象にします。
	if len(res.RetryableRecords) != 1 || len(res.QuarantineRecords) != 0 || len(res.DeadRecords) != 0 {
		t.Fatalf("records retryable=%d quarantine=%d dead=%d, want only the unsent entry retryable",
			len(res.RetryableRecords), len(res.QuarantineRecords), len(res.DeadRecords))
	}
	unsent := res.RetryableRecords[0]
	if unsent.Payload["message"] != "unsent after split" || unsent.StatusCode != http.StatusRequestEntityTooLarge ||
		unsent.DeliveryStatus != pkglogger.DeliveryFailed ||
		unsent.FailureCode != loggerfailure.FailureCode(retryable.RetryReasonRequestNotSent) {
		t.Fatalf("unsent record = %+v, want the confirmed HTTP 413 kept on an unsent retryable record", unsent)
	}
	// 前半は受理済みのため、TerminalErrorで部分的な受理結果を消しません。
	if res.TerminalError != nil || res.SuccessCount() != 1 {
		t.Fatalf("TerminalError = %v, SuccessCount = %d, want nil and the accepted first half", res.TerminalError, res.SuccessCount())
	}
}

func lokiEntryCount(streams []pkgloki.Stream) int {
	count := 0
	for _, stream := range streams {
		count += len(stream.Entries)
	}
	return count
}

func TestLokiFlushManagerRejectsOversizedLineBeforeNetworkAndSendsValidEntry(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelError, strings.Repeat("x", 4096)),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "valid log"),
	}
	records := flushManager.convertLogEntriesToEntryRecords(entries)
	validLineBytes := len([]byte(records[1].pushEntry.Line))
	oversizedLineBytes := len([]byte(records[0].pushEntry.Line))
	if validLineBytes >= oversizedLineBytes {
		t.Fatalf("test precondition: valid line bytes = %d, oversized line bytes = %d", validLineBytes, oversizedLineBytes)
	}
	flushManager.LoggerBase.config = newTestLokiLoggerConfigWithPushLimits(
		validLineBytes,
		oversizedLineBytes*2,
	)

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if client.pushCalls != 1 {
		t.Fatalf("Push call count = %d, want 1 for the valid entry only", client.pushCalls)
	}
	if got := pushedEntryCount(client.pushRequests[0]); got != 1 {
		t.Fatalf("pushed entry count = %d, want 1", got)
	}
	if got := client.pushRequests[0][0].Entries[0].Line; got != records[1].pushEntry.Line {
		t.Fatalf("pushed line = %q, want valid line %q", got, records[1].pushEntry.Line)
	}
	if len(res.RetryableRecords) != 0 {
		t.Fatalf("retryable records = %d, want 0", len(res.RetryableRecords))
	}
	if len(res.QuarantineRecords) != 1 {
		t.Fatalf("quarantine records = %d, want 1", len(res.QuarantineRecords))
	}
	if got := res.QuarantineRecords[0].FailureCode; got != FailureCodeLogLineTooLarge {
		t.Fatalf("failure code = %q, want %q", got, FailureCodeLogLineTooLarge)
	}
	if got := res.QuarantineRecords[0].Payload["message"]; got != entries[0].Message {
		t.Fatalf("rejected message = %v, want %q", got, entries[0].Message)
	}
	if got := res.SuccessCount(); got != 1 {
		t.Fatalf("SuccessCount() = %d, want 1", got)
	}
}

func TestLokiFlushManagerSplitsRequestsByConfiguredByteLimitBeforeNetwork(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "first planned request"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "second planned request"),
	}
	records := flushManager.convertLogEntriesToEntryRecords(entries)
	firstSize, err := pkgloki.PushRequestSize([]pkgloki.Stream{{
		Labels:  records[0].labels,
		Entries: []pkgloki.Entry{records[0].pushEntry},
	}})
	if err != nil {
		t.Fatalf("measure first request: %v", err)
	}
	secondSize, err := pkgloki.PushRequestSize([]pkgloki.Stream{{
		Labels:  records[1].labels,
		Entries: []pkgloki.Entry{records[1].pushEntry},
	}})
	if err != nil {
		t.Fatalf("measure second request: %v", err)
	}
	requestLimit := firstSize
	if secondSize > requestLimit {
		requestLimit = secondSize
	}
	combinedSize, err := pkgloki.PushRequestSize([]pkgloki.Stream{{
		Labels:  records[0].labels,
		Entries: []pkgloki.Entry{records[0].pushEntry, records[1].pushEntry},
	}})
	if err != nil {
		t.Fatalf("measure combined request: %v", err)
	}
	if combinedSize <= requestLimit {
		t.Fatalf("test precondition: combined size = %d, request limit = %d", combinedSize, requestLimit)
	}
	maxLineBytes := len([]byte(records[0].pushEntry.Line))
	if secondLineBytes := len([]byte(records[1].pushEntry.Line)); secondLineBytes > maxLineBytes {
		maxLineBytes = secondLineBytes
	}
	flushManager.LoggerBase.config = newTestLokiLoggerConfigWithPushLimits(maxLineBytes, requestLimit)

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if res.HasSendFailure() {
		t.Fatalf("SendResult = %+v, want all records successful", res)
	}
	if client.pushCalls != 2 {
		t.Fatalf("Push call count = %d, want 2 preplanned requests", client.pushCalls)
	}
	if len(client.pushRequests) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(client.pushRequests))
	}
	for index, request := range client.pushRequests {
		if got := pushedEntryCount(request); got != 1 {
			t.Fatalf("request[%d] entry count = %d, want 1", index, got)
		}
		size, sizeErr := pkgloki.PushRequestSize(request)
		if sizeErr != nil {
			t.Fatalf("measure request[%d]: %v", index, sizeErr)
		}
		if size > requestLimit {
			t.Fatalf("request[%d] size = %d, limit = %d", index, size, requestLimit)
		}
	}
}

func TestLokiFlushManagerKeepsMultiEntryAuthorizationFailurePermanent(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusUnauthorized,
			Status: "401 Unauthorized",
		},
	}
	flushManager := newTestLokiFlushManager(client)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelWarn, "first log"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "second log"),
	}

	res, err := flushManager.pushLogEntries(context.Background(), entries)

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if len(res.RetryableRecords) != 0 {
		t.Fatalf("expected no retryable records, got %d", len(res.RetryableRecords))
	}
	if len(res.DeadRecords) != len(entries) {
		t.Fatalf("expected %d dead records, got %d", len(entries), len(res.DeadRecords))
	}
}

func TestLokiFlushManagerLabelsForLevelIncludesProfileStaticLabels(t *testing.T) {
	flushManager := newTestLokiFlushManager(&recordingFlushManagerLokiClient{})
	generatedLabels := flushManager.labelsForLevel(pkglogger.LogLevelWarn)
	flushManager.config = NewFlushManagerConfigAdapter(pkglogger.LokiFlushManagerConfig{
		StaticLabels: map[string]string{
			"event_scope": "appendlog_lifecycle",
			"appName":     "ignored-app",
			"loglevel":    "ignored-level",
			"env":         "ignored-env",
		},
	})

	labels := flushManager.labelsForLevel(pkglogger.LogLevelWarn)
	if got := labels["event_scope"]; got != "appendlog_lifecycle" {
		t.Fatalf("event_scope label = %q", got)
	}
	for _, key := range []string{"appName", "loglevel", "env"} {
		if got := labels[key]; got != generatedLabels[key] {
			t.Fatalf("generated label %q = %q, want %q", key, got, generatedLabels[key])
		}
	}
}

func TestLokiFlushManagerPushInvokesFailureHandlerWithRetryableResult(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   260,
			Status: "260 Ingestion Blocked",
		},
	}
	failureHandler := &recordingLokiFailureHandler{}
	composite := mustNewLokiFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	flushManager := newTestLokiFlushManagerWithFailureHandler(client, composite)

	err := flushManager.Push(context.Background(), newTestLokiLogEntry(pkglogger.LogLevelInfo, "push log"))

	if err != nil {
		t.Fatalf("Push returned error: %v", err)
	}
	got, ok := failureHandler.waitResult(t)
	if !ok {
		t.Fatal("expected failure handler to be called")
	}
	if len(got.RetryableRecords) != 1 {
		t.Fatalf("expected one retryable record, got %d", len(got.RetryableRecords))
	}
	if len(got.DeadRecords) != 0 {
		t.Fatalf("expected no dead records, got %d", len(got.DeadRecords))
	}
}

func TestLokiFlushManagerUsesSingleFailureHandler(t *testing.T) {
	handler := &recordingLokiFailureHandler{}
	flushManager := newTestLokiFlushManagerWithFailureHandler(
		&recordingFlushManagerLokiClient{},
		mustNewLokiFailureComposite(t,
			[]flushfailure.FailureHandler{handler},
		),
	)
	result := loggerfailure.SendResult{Meta: loggerfailure.Meta{Backend: deliveryBackendLoki}}

	if err := flushManager.handleFailure(context.Background(), result); err != nil {
		t.Fatalf("handleFailure() error = %v", err)
	}

	if got := handler.callCount(); got != 1 {
		t.Fatalf("failure handler calls = %d, want 1", got)
	}
}

func TestLokiFlushManagerPushReturnsSynchronousFailureHandlerError(t *testing.T) {
	handlerErr := errors.New("loki durable write failed")
	handler := &recordingLokiFailureHandler{err: handlerErr}
	flushManager := newTestLokiFlushManagerWithFailureHandler(
		&recordingFlushManagerLokiClient{
			pushErr: &pkgloki.HTTPStatusError{
				Code:   http.StatusServiceUnavailable,
				Status: "503 Service Unavailable",
			},
		},
		mustNewLokiFailureComposite(t,
			[]flushfailure.FailureHandler{handler},
		),
	)

	err := flushManager.Push(
		context.Background(),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "synchronous durable failure"),
	)

	if !errors.Is(err, handlerErr) {
		t.Fatalf("Push() error = %v, want handler error %v", err, handlerErr)
	}
	if got := handler.callCount(); got != 1 {
		t.Fatalf("failure handler calls = %d, want 1", got)
	}
}

// durable処理を完了できない場合は、failure handlerのerrorだけでなくLokiの拒否そのものを
// errors.Asで判別できるように返します。durable処理が完了した場合は従来どおり成功とします。
func TestLokiFlushManagerKeepsBackendCauseWhenDurableHandlingFails(t *testing.T) {
	rejection := &pkgloki.HTTPStatusError{Code: http.StatusForbidden, Status: "403 Forbidden"}
	handlerErr := errors.New("loki durable write failed")
	for _, mode := range []string{"push", "flush"} {
		for _, test := range []struct {
			name           string
			handler        flushfailure.FailureHandler
			wantHandlerErr error
		}{
			{name: "without failure handler", wantHandlerErr: flushfailure.ErrNoFailureHandler},
			{name: "failure handler error", handler: &recordingLokiFailureHandler{err: handlerErr}, wantHandlerErr: handlerErr},
			{name: "durable handling succeeded", handler: &recordingLokiFailureHandler{}},
		} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				var composite *flushfailure.CompositeFailureHandler
				if test.handler != nil {
					composite = mustNewLokiFailureComposite(t, []flushfailure.FailureHandler{test.handler})
				}
				flushManager := newTestLokiFlushManagerWithFailureHandler(
					&recordingFlushManagerLokiClient{pushErr: rejection},
					composite,
				)
				entry := newTestLokiLogEntry(pkglogger.LogLevelError, "rejected by Loki")

				var err error
				if mode == "push" {
					err = flushManager.Push(context.Background(), entry)
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
				var statusErr *pkgloki.HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
					t.Fatalf("error = %v, want the Loki HTTP 403 rejection", err)
				}
			})
		}
	}
}

// 一部のrequestが受理された場合はTerminalErrorを設定しませんが、退避処理を完了できなかった
// ときも、拒否されたrequestの元のerrorを返してerrors.Asで判別できることを確認します。
func TestLokiFlushManagerKeepsBackendCauseForPartiallyAcceptedFlush(t *testing.T) {
	rejection := &pkgloki.HTTPStatusError{Code: http.StatusForbidden, Status: "403 Forbidden"}
	client := &recordingFlushManagerLokiClient{}
	client.pushFunc = func([]pkgloki.Stream) error {
		if client.pushCalls == 2 {
			return rejection
		}
		return nil
	}
	handlerErr := errors.New("loki durable write failed")
	handler := &recordingLokiFailureHandler{err: handlerErr}
	flushManager := newTestLokiFlushManagerWithFailureHandler(
		client,
		mustNewLokiFailureComposite(t, []flushfailure.FailureHandler{handler}),
	)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "accepted request"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "rejected request"),
	}
	useOneLokiPushRequestPerEntry(t, flushManager, entries)

	err := flushManager.Flush(context.Background(), entries)

	if client.pushCalls != 2 {
		t.Fatalf("Push calls = %d, want one request per entry", client.pushCalls)
	}
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Flush() error = %v, want durable handling error %v", err, handlerErr)
	}
	var statusErr *pkgloki.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
		t.Fatalf("Flush() error = %v, want the Loki HTTP 403 rejection", err)
	}
	res, called := handler.result()
	if !called {
		t.Fatal("failure handler was not invoked")
	}
	// 受理済みの一件目は退避対象に含めず、部分的な受理結果も消しません。
	if res.TerminalError != nil || res.FailedCount() != 1 || res.SuccessCount() != 1 {
		t.Fatalf("SendResult terminal=%v failed=%d success=%d, want only the rejected entry failed",
			res.TerminalError, res.FailedCount(), res.SuccessCount())
	}
}

// useOneLokiPushRequestPerEntryは、各entryは単独で上限内に収まり、二件をまとめると上限を
// 超えるようにpush上限を設定し、entryごとに別のrequestへ分割させます。
func useOneLokiPushRequestPerEntry(t *testing.T, flushManager *LokiFlushManager, entries []*logging.LogEntry) {
	t.Helper()
	records := flushManager.convertLogEntriesToEntryRecords(entries)
	requestLimit, maxLineBytes, total := 0, 0, 0
	for _, record := range records {
		size, err := pkgloki.PushRequestSize([]pkgloki.Stream{{
			Labels:  record.labels,
			Entries: []pkgloki.Entry{record.pushEntry},
		}})
		if err != nil {
			t.Fatalf("measure single entry request: %v", err)
		}
		requestLimit = max(requestLimit, size)
		maxLineBytes = max(maxLineBytes, len(record.pushEntry.Line))
		total += size
	}
	if total <= requestLimit {
		t.Fatalf("test precondition: combined size %d must exceed request limit %d", total, requestLimit)
	}
	flushManager.LoggerBase.config = newTestLokiLoggerConfigWithPushLimits(maxLineBytes, requestLimit)
}

func TestLokiFlushManagerFlushWaitsForFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handleErr := errors.New("loki buffered durable handling failed")
		handler := &blockingLokiFailureHandler{
			started: make(chan struct{}),
			release: make(chan struct{}),
			err:     handleErr,
		}
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(handler.releaseHandling)
		flushManager := newTestLokiFlushManagerWithFailureHandler(
			&recordingFlushManagerLokiClient{
				pushErr: &pkgloki.HTTPStatusError{
					Code:   http.StatusServiceUnavailable,
					Status: "503 Service Unavailable",
				},
			},
			mustNewLokiFailureComposite(t,
				[]flushfailure.FailureHandler{handler},
			),
		)

		returned := make(chan error, 1)
		go func() {
			returned <- flushManager.Flush(
				context.Background(),
				[]*logging.LogEntry{
					newTestLokiLogEntry(pkglogger.LogLevelInfo, "asynchronous durable failure"),
				},
			)
		}()

		select {
		case <-handler.started:
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("failure handler did not start")
		}

		synctest.Wait()
		select {
		case err := <-returned:
			t.Fatalf("Flush() returned before failure handler completion: %v", err)
		default:
		}

		handler.releaseHandling()
		if err := <-returned; !errors.Is(err, handleErr) {
			t.Fatalf("Flush() error = %v, want durable handling error", err)
		}
	})
}

func TestLokiFlushManagerCloseWaitsForAcceptedDurableFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := &blockingLokiFailureHandler{
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(handler.releaseHandling)
		flushManager := newTestLokiFlushManagerWithFailureHandler(
			&recordingFlushManagerLokiClient{
				pushErr: &pkgloki.HTTPStatusError{
					Code:   http.StatusServiceUnavailable,
					Status: "503 Service Unavailable",
				},
			},
			mustNewLokiFailureComposite(t,
				[]flushfailure.FailureHandler{handler},
			),
		)

		flushDone := make(chan error, 1)
		go func() {
			flushDone <- flushManager.Flush(
				context.Background(),
				[]*logging.LogEntry{
					newTestLokiLogEntry(pkglogger.LogLevelInfo, "durable failure before close"),
				},
			)
		}()

		select {
		case <-handler.started:
		case <-time.After(lifecycleHangTimeout):
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

		handler.releaseHandling()
		if err := <-flushDone; err != nil {
			t.Fatalf("Flush() error = %v", err)
		}

		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("Close() did not return after durable failure handling completed")
		}
	})
}

func TestLokiFlushManagerFailureDocumentKeepsPushedEntry(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   260,
			Status: "260 Ingestion Blocked",
		},
	}
	flushManager := newTestLokiFlushManager(client)
	entry := newTestLokiLogEntry(pkglogger.LogLevelError, "retryable loki")
	entry.OccurredAt = time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)

	res, err := flushManager.pushLogEntries(context.Background(), []*logging.LogEntry{entry})

	if err != nil {
		t.Fatalf("pushLogEntries returned error: %v", err)
	}
	if len(client.streams) != 1 || len(client.streams[0].Entries) != 1 {
		t.Fatalf("expected one pushed entry, got streams=%d", len(client.streams))
	}
	if len(res.RetryableRecords) != 1 {
		t.Fatalf("expected one retryable record, got %d", len(res.RetryableRecords))
	}
	pushedEntry := client.streams[0].Entries[0]
	doc := res.RetryableRecords[0].Payload
	if got, want := doc["timestamp"], pushedEntry.Timestamp.Format(time.RFC3339Nano); got != want {
		t.Fatalf("failure document timestamp = %v, want pushed timestamp %s", got, want)
	}
	if got, want := doc["line"], pushedEntry.Line; got != want {
		t.Fatalf("failure document line = %v, want pushed line %q", got, want)
	}
}

func TestLokiFlushManagerFlushWithNoFallbackBatchesWithoutFailureHandler(t *testing.T) {
	client := &recordingFlushManagerLokiClient{
		pushErr: errors.New("loki unavailable"),
	}
	failureHandler := &recordingLokiFailureHandler{}
	composite := mustNewLokiFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	)
	flushManager := newTestLokiFlushManagerWithFailureHandler(client, composite)

	flushManager.FlushWithNoFallback(context.Background(), []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "first fallback log"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "second fallback log"),
	})

	if client.pushCalls != 1 {
		t.Fatalf("expected one push call, got %d", client.pushCalls)
	}
	if _, ok := failureHandler.result(); ok {
		t.Fatal("expected no fallback failure handler call")
	}
}

func TestFallbackLoggerEnqueuesAndFlushesWithNoFallback(t *testing.T) {
	flushManager := &recordingFallbackFlushManager{}
	cfg := newTestLokiLoggerConfig(2, time.Hour, false)
	fallbackLogger := newLokiFallbackBatchTestLogger(cfg, flushManager)
	fallbackLogger.startFallbackWorker()
	first := newTestLokiLogEntry(pkglogger.LogLevelError, "first fallback entry")
	second := newTestLokiLogEntry(pkglogger.LogLevelError, "second fallback entry")

	fallbackLogger.enqueueProcess(first)
	fallbackLogger.enqueueProcess(second)
	if err := fallbackLogger.Close(context.Background()); err != nil {
		t.Fatalf("close fallback logger: %v", err)
	}

	if flushManager.noFallbackCalls != 1 {
		t.Fatalf("expected one FlushWithNoFallback call, got %d", flushManager.noFallbackCalls)
	}
	if flushManager.pushCalls != 0 {
		t.Fatalf("expected no Push calls, got %d", flushManager.pushCalls)
	}
	if len(flushManager.noFallbackEntries) != 2 {
		t.Fatalf("expected two no-fallback entries, got %d", len(flushManager.noFallbackEntries))
	}
	if flushManager.noFallbackEntries[0] != first || flushManager.noFallbackEntries[1] != second {
		t.Fatal("expected no-fallback entries to be passed through in order")
	}
}

func TestLokiLoggerOptionsUseClockAndTimeProvider(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	clockTime := time.Date(2026, 6, 24, 1, 2, 3, 0, time.UTC)
	providerTime := time.Date(2026, 6, 24, 4, 5, 6, 0, time.UTC)

	logger, err := NewLokiLogger(
		context.Background(),
		newTestLokiLoggerConfig(10, time.Hour, true),
		client,
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedLokiClock{t: clockTime}),
		WithTimeProvider(func() time.Time { return providerTime }),
	)
	if err != nil {
		t.Fatalf("NewLokiLogger returned error: %v", err)
	}
	defer func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	}()

	if err := logger.testInfo(context.Background(), "option clock message"); err != nil {
		t.Fatalf("Info returned error: %v", err)
	}
	if client.pushCalls != 1 {
		t.Fatalf("expected one push call, got %d", client.pushCalls)
	}
	if len(client.streams) != 1 || len(client.streams[0].Entries) != 1 {
		t.Fatalf("expected one pushed entry, got streams=%d entries=%d", len(client.streams), len(client.streams[0].Entries))
	}
	if !client.streams[0].Entries[0].Timestamp.Equal(clockTime) {
		t.Fatalf("expected pushed timestamp %s, got %s", clockTime, client.streams[0].Entries[0].Timestamp)
	}

	logger.updateLastFlushTime()
	if !logger.lastFlushTime.Equal(providerTime) {
		t.Fatalf("expected lastFlushTime %s, got %s", providerTime, logger.lastFlushTime)
	}
}

func TestLokiLoggerOptionUsesFlushIntervalGenerator(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	flushInterval := 30 * time.Second
	var gotDuration time.Duration

	logger, err := NewLokiLogger(
		context.Background(),
		newTestLokiLoggerConfig(10, flushInterval, false),
		client,
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
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
		t.Fatalf("NewLokiLogger returned error: %v", err)
	}
	defer func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	}()

	if gotDuration != flushInterval {
		t.Fatalf("expected flush interval %s, got %s", flushInterval, gotDuration)
	}
}

type recordingFlushManagerLokiClient struct {
	pushErr      error
	pushCalls    int
	streams      []pkgloki.Stream
	pushRequests [][]pkgloki.Stream
	// pushFuncを設定すると、記録したstreamごとの結果をpushErrの代わりに返します。
	pushFunc func([]pkgloki.Stream) error
}

func (c *recordingFlushManagerLokiClient) Push(ctx context.Context, streams []pkgloki.Stream) error {
	c.pushCalls++
	recorded := cloneLokiStreams(streams)
	c.streams = recorded
	c.pushRequests = append(c.pushRequests, recorded)
	if c.pushFunc != nil {
		return c.pushFunc(recorded)
	}
	return c.pushErr
}

func (c *recordingFlushManagerLokiClient) PushEntry(ctx context.Context, labels map[string]string, entry pkgloki.Entry) error {
	return c.Push(ctx, []pkgloki.Stream{{Labels: labels, Entries: []pkgloki.Entry{entry}}})
}

func (c *recordingFlushManagerLokiClient) PushLog(ctx context.Context, labels map[string]string, line string) error {
	return c.PushEntry(ctx, labels, pkgloki.Entry{Line: line})
}

type recordingLokiFailureHandler struct {
	mu     sync.Mutex
	called bool
	calls  int
	res    loggerfailure.SendResult
	done   chan struct{}
	err    error
}

func (h *recordingLokiFailureHandler) HandleFailure(
	ctx context.Context,
	r loggerfailure.SendResult,
) error {
	h.mu.Lock()
	h.called = true
	h.calls++
	h.res = r
	done := h.done
	err := h.err
	h.mu.Unlock()
	if done != nil {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	return err
}

func (h *recordingLokiFailureHandler) result() (loggerfailure.SendResult, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.res, h.called
}

func (h *recordingLokiFailureHandler) waitResult(t *testing.T) (loggerfailure.SendResult, bool) {
	t.Helper()

	h.mu.Lock()
	if h.called {
		result := h.res
		h.mu.Unlock()
		return result, true
	}
	if h.done == nil {
		h.done = make(chan struct{}, 1)
	}
	done := h.done
	h.mu.Unlock()

	select {
	case <-done:
		return h.result()
	case <-time.After(lifecycleHangTimeout):
		return loggerfailure.SendResult{}, false
	}
}

func (h *recordingLokiFailureHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

type blockingLokiFailureHandler struct {
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	err         error
}

// releaseHandlingは、待機中のHandleFailureを一度だけ解放します。
func (h *blockingLokiFailureHandler) releaseHandling() {
	h.releaseOnce.Do(func() { close(h.release) })
}

func (h *blockingLokiFailureHandler) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	h.once.Do(func() {
		close(h.started)
	})
	<-h.release
	return h.err
}

type recordingFallbackFlushManager struct {
	pushCalls         int
	noFallbackCalls   int
	noFallbackEntries []*logging.LogEntry
}

func (m *recordingFallbackFlushManager) Flush(ctx context.Context, entries []*logging.LogEntry) error {
	return nil
}

func (m *recordingFallbackFlushManager) Push(ctx context.Context, entry *logging.LogEntry) error {
	m.pushCalls++
	return nil
}

func (m *recordingFallbackFlushManager) PushWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error) {
	m.pushCalls++
	return pkglogger.DeliveryReport{}, nil
}

func (m *recordingFallbackFlushManager) FlushWithNoFallback(
	_ context.Context,
	entries []*logging.LogEntry,
) {
	m.noFallbackCalls++
	m.noFallbackEntries = append(m.noFallbackEntries, entries...)
}

func (m *recordingFallbackFlushManager) Close(context.Context) error {
	return nil
}

func newTestLokiFlushManager(client Client) *LokiFlushManager {
	return newTestLokiFlushManagerWithFailureHandler(client, nil)
}

func newTestLokiFlushManagerWithFailureHandler(
	client Client,
	failureHandler *flushfailure.CompositeFailureHandler,
) *LokiFlushManager {
	loggerConfig := newTestLokiLoggerConfig(100, time.Hour, false)
	flushConfig := newTestLokiFlushManagerConfig()
	resourceProvider := newTestLokiResourceProvider()
	loggerBase := &LoggerBase{
		Clock:        fixedLokiClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)},
		LogFormatter: lokiTestPlainTextFormatter,
		config:       loggerConfig,
		Tracer:       observer.NopTracer{},
		Meter:        observer.NopMeter{},
	}

	return NewLokiFlushManager(
		loggerBase,
		client,
		flushConfig,
		failureHandler,
		resourceProvider,
		observer.NopTracer{},
		observer.NopMeter{},
	)
}

func newTestLokiFlushManagerWithReportingFailureHandler(
	client Client,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
) *LokiFlushManager {
	var baseFailureHandler *flushfailure.CompositeFailureHandler
	if failureHandler != nil {
		baseFailureHandler = failureHandler.CompositeFailureHandler
	}
	fm := newTestLokiFlushManagerWithFailureHandler(client, baseFailureHandler)
	fm.FailureReportHandler = failureHandler
	return fm
}

func newTestLokiLoggerConfig(maxEntries int, flushInterval time.Duration, immediatePush bool) LokiConfigProvider {
	return newTestLokiLoggerConfigWithBufferLimits(
		maxEntries,
		1024*1024,
		maxEntries,
		flushInterval,
		immediatePush,
	)
}

func newTestLokiLoggerConfigWithBufferLimits(
	maxEntries int,
	bufferFlushBytes int,
	fallbackQueueMaxEntries int,
	flushInterval time.Duration,
	immediatePush bool,
) LokiConfigProvider {
	return NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
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
		FlushInterval:           flushInterval,
		MaxEntries:              maxEntries,
		BufferFlushBytes:        bufferFlushBytes,
		FallbackQueueMaxEntries: fallbackQueueMaxEntries,
		MaxLineBytes:            256 * 1024,
		MaxPushRequestBytes:     4 * 1024 * 1024,
		NoBulkFlushModeEnabled:  immediatePush,
	})
}

func newTestLokiLoggerConfigWithPushLimits(maxLineBytes, maxPushRequestBytes int) LokiConfigProvider {
	return NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
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
		MaxEntries:              100,
		BufferFlushBytes:        1024 * 1024,
		FallbackQueueMaxEntries: 100,
		MaxLineBytes:            maxLineBytes,
		MaxPushRequestBytes:     maxPushRequestBytes,
	})
}

func cloneLokiStreams(streams []pkgloki.Stream) []pkgloki.Stream {
	cloned := make([]pkgloki.Stream, 0, len(streams))
	for _, stream := range streams {
		cloned = append(cloned, pkgloki.Stream{
			Labels:  stream.Labels,
			Entries: append([]pkgloki.Entry(nil), stream.Entries...),
		})
	}
	return cloned
}

func pushedEntryCount(streams []pkgloki.Stream) int {
	count := 0
	for _, stream := range streams {
		count += len(stream.Entries)
	}
	return count
}

func newTestLokiFlushManagerConfig() LokiFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.LokiFlushManagerConfig{
		Resource: pkglogger.Resource{
			ServiceName: "loki-test-app",
			HostName:    "loki-test-host",
			Environment: "prd",
			Cloud:       &pkglogger.CloudResourceInfo{},
		},
	})
}

func newTestLokiResourceProvider() resource.ResourceProvider {
	return resource.NewResourceAdapter(pkglogger.Resource{
		ServiceName: "loki-test-app",
		HostName:    "loki-test-host",
		Environment: "prd",
		Cloud:       &pkglogger.CloudResourceInfo{},
	})
}

func newTestLokiLogEntry(level pkglogger.LogLevel, message string) *logging.LogEntry {
	return &logging.LogEntry{
		OccurredAt: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC),
		Level:      level,
		RequestID:  "req-loki-test",
		TraceID:    "trace-loki-test",
		SpanID:     "span-loki-test",
		Message:    message,
		Fields: map[string]any{
			"case": "loki-test",
		},
	}
}

type fixedLokiClock struct {
	t time.Time
}

func (c fixedLokiClock) Now() time.Time {
	return c.t
}

var _ clock.Clock = fixedLokiClock{}

func assertLokiLabels(t *testing.T, labels map[string]string, appName, level, env string) {
	t.Helper()
	if labels["appName"] != appName {
		t.Fatalf("expected appName label %q, got %q", appName, labels["appName"])
	}
	if labels["loglevel"] != level {
		t.Fatalf("expected loglevel label %q, got %q", level, labels["loglevel"])
	}
	if labels["env"] != env {
		t.Fatalf("expected env label %q, got %q", env, labels["env"])
	}
}

func assertPushedLokiLabels(t *testing.T, client *recordingFlushManagerLokiClient, appName, level, env string) {
	t.Helper()
	if client.pushCalls != 1 {
		t.Fatalf("expected one push call, got %d", client.pushCalls)
	}
	if len(client.streams) != 1 {
		t.Fatalf("expected one pushed stream, got %d", len(client.streams))
	}
	assertLokiLabels(t, client.streams[0].Labels, appName, level, env)
}
