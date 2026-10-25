package cloudwatchlogs

import (
	"context"
	"net/http"
	"testing"
	"time"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestConvertEventRecordToDocumentRecordsRoutedAttemptedStream(t *testing.T) {
	t.Parallel()

	cfg := NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
		LogGroupName:  "/app/test",
		LogStreamName: "container/task-1",
	}, streamrouting.Hourly)
	fm := &CloudWatchLogsFlushManager{config: cfg}
	location := time.FixedZone("JST", 9*60*60)
	attemptedAt := time.Date(2026, time.September, 1, 0, 30, 0, 0, location)
	record := cloudWatchLogsEventRecord{event: pkgcloudwatchlogs.Event{
		Timestamp: attemptedAt.Add(-time.Minute),
		Message:   "failed event",
	}}

	document := fm.convertEventRecordToDocument(record, "container/task-1/2026/09/01/00")

	if got := document["log_group"]; got != "/app/test" {
		t.Fatalf("log_group = %#v, want %q", got, "/app/test")
	}
	if got := document["log_stream"]; got != "container/task-1/2026/09/01/00" {
		t.Fatalf(
			"log_stream = %#v, want %q",
			got,
			"container/task-1/2026/09/01/00",
		)
	}
}

func TestConvertEventRecordToDocumentUsesEventTimezoneWhenStreamIsNotYetAssigned(t *testing.T) {
	t.Parallel()

	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	resolver, err := infracloudwatchlogs.NewLogStreamNameResolver(
		"container/task-1",
		streamrouting.Hourly,
	)
	if err != nil {
		t.Fatalf("NewLogStreamNameResolver() error = %v", err)
	}
	fm := &CloudWatchLogsFlushManager{
		config: NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
			LogGroupName:  "/app/test",
			LogStreamName: "container/task-1",
		}, streamrouting.Hourly),
		streamNameResolver: resolver,
	}
	record := cloudWatchLogsEventRecord{event: pkgcloudwatchlogs.Event{
		Timestamp: time.Date(2026, time.September, 1, 0, 30, 0, 0, location),
		Message:   "invalid before sending",
	}}

	document := fm.convertEventRecordToDocument(record, "")

	if got := document["log_stream"]; got != "container/task-1/2026/09/01/00" {
		t.Fatalf("log_stream = %#v, want %q", got, "container/task-1/2026/09/01/00")
	}
}

func TestPutLogEntriesUsesOneAttemptedAtForClientAndFailureDocument(t *testing.T) {
	t.Parallel()

	attemptedAt := time.Date(
		2026,
		time.August,
		31,
		23,
		59,
		59,
		0,
		time.FixedZone("JST", 9*60*60),
	)
	client := &recordingCloudWatchLogsClient{
		err: &pkgcloudwatchlogs.HTTPStatusError{
			Code:   http.StatusBadRequest,
			Status: "400 Bad Request",
		},
	}
	fm := newTestCloudWatchLogsFlushManagerWithStreamRouting(
		client,
		nil,
		"container/task-1",
		streamrouting.Hourly,
	)
	fm.Clock = fixedCloudWatchLogsClock{t: attemptedAt}
	fm.config = NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
		LogGroupName:  "/app/test",
		LogStreamName: "container/task-1",
	}, streamrouting.Hourly)

	result, err := fm.putEntryAsSingleRequest(
		context.Background(),
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "failed at boundary"),
	)

	if err != nil {
		t.Fatalf("putEntryAsSingleRequest() error = %v", err)
	}
	if len(client.sentAts) != 1 || !client.sentAts[0].Equal(attemptedAt) {
		t.Fatalf("client sentAts = %v, want [%s]", client.sentAts, attemptedAt)
	}
	if !result.Meta.AttemptedAt.Equal(attemptedAt) {
		t.Fatalf("result AttemptedAt = %s, want %s", result.Meta.AttemptedAt, attemptedAt)
	}
	if len(result.DeadRecords) != 1 {
		t.Fatalf("dead records = %d, want 1", len(result.DeadRecords))
	}
	if got := result.DeadRecords[0].Payload["log_stream"]; got != "container/task-1/2026/06/23/12" {
		t.Fatalf("failure log_stream = %#v, want %q", got, "container/task-1/2026/06/23/12")
	}
}
