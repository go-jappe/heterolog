package cloudwatchlogs

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchLogsFlushManagerSplitsRequestRejectedAsTooLarge(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	client.putFunc = func(_ string, events []pkgcloudwatchlogs.Event) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
		// 中継先の上限がAWSの上限より小さく、二件以上のrequestと単独の大きなeventを拒否します。
		if len(events) > 1 || strings.Contains(events[0].Message, "too large even alone") {
			return nil, &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
	}
	failureHandler := &recordingCloudWatchLogsFailureHandler{}
	flushManager := newTestCloudWatchLogsFlushManager(client, mustNewCloudWatchLogsFailureComposite(t,
		[]flushfailure.FailureHandler{failureHandler},
	))
	now := time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)
	entries := []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "first event", now.Add(-3*time.Second)),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too large even alone", now.Add(-2*time.Second)),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "third event", now.Add(-time.Second)),
	}

	if err := flushManager.Flush(context.Background(), entries); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// 413ではどのeventも受理されていないため、分けて再送し、正常なeventを配送します。
	// 分割の順序や回数ではなく、正常なeventがそれぞれ一度だけ受理されることを検証します。
	// 二分割では最大でも 2*入力件数-1 回の送信で単独のeventまで分けられます。
	if maxCalls := 2*len(entries) - 1; len(client.calls) > maxCalls {
		t.Fatalf("PutLogEvents calls = %d, want at most %d", len(client.calls), maxCalls)
	}
	accepted := make(map[string]int)
	for _, call := range client.calls {
		if call.streamName != "stream-a" || !call.sentAt.Equal(client.calls[0].sentAt) {
			t.Fatalf("split call = %+v, want the routed stream and the first attempt time", call)
		}
		if len(call.events) == 1 && !strings.Contains(call.events[0].Message, "too large even alone") {
			accepted[call.events[0].Message]++
		}
	}
	for _, message := range []string{"first event", "third event"} {
		count := 0
		for acceptedMessage, n := range accepted {
			if strings.Contains(acceptedMessage, message) {
				count += n
			}
		}
		if count != 1 {
			t.Fatalf("event %q accepted %d times, want exactly once (accepted=%v)", message, count, accepted)
		}
	}
	res, called := failureHandler.result()
	if !called {
		t.Fatal("failure handler was not invoked for the rejected event")
	}
	if len(res.RetryableRecords) != 0 || len(res.DeadRecords) != 0 || len(res.QuarantineRecords) != 1 {
		t.Fatalf("records retryable=%d dead=%d quarantine=%d, want only the oversized event quarantined",
			len(res.RetryableRecords), len(res.DeadRecords), len(res.QuarantineRecords))
	}
	// 一件でも拒否されたeventは、上限設定の変更で配送できる可能性があるため単独で隔離します。
	quarantined := res.QuarantineRecords[0]
	if quarantined.StatusCode != http.StatusRequestEntityTooLarge ||
		quarantined.FailureCode != loggerfailure.FailureCode(retryable.RetryReasonRequestTooLarge) ||
		quarantined.Payload["message"] != "too large even alone" {
		t.Fatalf("quarantined record = %+v, want the oversized event with HTTP 413 request_too_large", quarantined)
	}
	if res.TerminalError != nil {
		t.Fatalf("TerminalError = %v, want nil for explicit rejections", res.TerminalError)
	}
}

func TestCloudWatchLogsFlushManagerKeepsUnsentSplitEventsRetryableWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &recordingCloudWatchLogsClient{}
	client.putFunc = func(_ string, events []pkgcloudwatchlogs.Event) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
		if len(events) > 1 {
			return nil, &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		// 分割後の一件目を送信した直後に、呼び出し元が停止します。
		cancel()
		return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
	}
	flushManager := newTestCloudWatchLogsFlushManager(client, nil)
	now := time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)
	records := []cloudWatchLogsEventRecord{
		{inputIndex: 0, entry: newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "sent after split", now.Add(-2*time.Second))},
		{inputIndex: 1, entry: newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "unsent after split", now.Add(-time.Second))},
	}
	for index := range records {
		records[index].event = pkgcloudwatchlogs.Event{Timestamp: records[index].entry.OccurredAt, Message: records[index].entry.Message}
	}

	res, err := flushManager.putCloudWatchLogsRequest(
		ctx,
		loggerfailure.Meta{AttemptedAt: now, InputCount: len(records)},
		"stream-a",
		records,
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("putCloudWatchLogsRequest() error = %v, want context.Canceled", err)
	}
	if len(client.calls) != 2 {
		t.Fatalf("PutLogEvents calls = %d, want 2", len(client.calls))
	}
	// 413ではどのeventも受理されていないため、未送信の後半は確定した413を保持して再送対象にします。
	if len(res.RetryableRecords) != 1 || len(res.QuarantineRecords) != 0 || len(res.DeadRecords) != 0 {
		t.Fatalf("records retryable=%d quarantine=%d dead=%d, want only the unsent event retryable",
			len(res.RetryableRecords), len(res.QuarantineRecords), len(res.DeadRecords))
	}
	unsent := res.RetryableRecords[0]
	if unsent.StatusCode != http.StatusRequestEntityTooLarge || unsent.DeliveryStatus != pkglogger.DeliveryFailed ||
		unsent.FailureCode != loggerfailure.FailureCode(retryable.RetryReasonRequestNotSent) {
		t.Fatalf("unsent record = %+v, want the confirmed HTTP 413 kept on an unsent retryable record", unsent)
	}
	// 前半は受理済みのため、TerminalErrorで部分的な受理結果を消しません。
	if res.TerminalError != nil || res.SuccessCount() != 1 {
		t.Fatalf("TerminalError = %v, SuccessCount = %d, want nil and the accepted first half", res.TerminalError, res.SuccessCount())
	}
}
