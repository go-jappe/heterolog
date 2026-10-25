package cloudwatchlogs

import (
	"fmt"
	"time"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

// cloudWatchLogsPutLogEventOverheadBytesはCloudWatch LogsがPutLogEventsの
// request size計算でeventごとに加算するoverheadです。
const cloudWatchLogsPutLogEventOverheadBytes = 26

// cloudWatchLogsEventRecord は送信 event と元 LogEntry の対応を保持します。
// PutLogEvents の部分拒否 index は送信配列基準なので、sort 後もこの対応を崩さないことが重要です。
type cloudWatchLogsEventRecord struct {
	// inputIndexは、このrecordを生成したLogEntryの入力slice内indexです。
	// backend plannerがstream分類・並べ替え・request分割した後も、planner rejectionと
	// backendの部分拒否を元のLogEntryへ対応づけるために保持します。
	inputIndex int
	entry      *logging.LogEntry
	event      pkgcloudwatchlogs.Event
}

type invalidCloudWatchLogsEventRecord struct {
	record cloudWatchLogsEventRecord
	err    error
}

// convertLogEntriesToEventRecordsはCloudWatch Logs event、元LogEntry、入力indexを
// 同じrecordへまとめます。request計画と送信失敗recordの生成が同じ対応を使います。
func convertLogEntriesToEventRecords(
	entries []*logging.LogEntry,
	logFormatter formatter.PlainTextFormatter,
) []cloudWatchLogsEventRecord {
	if len(entries) == 0 {
		return nil
	}

	records := make([]cloudWatchLogsEventRecord, 0, len(entries))
	for inputIndex, entry := range entries {
		occurredAt := entryTimestamp(entry)
		records = append(records, cloudWatchLogsEventRecord{
			inputIndex: inputIndex,
			entry:      entry,
			event: pkgcloudwatchlogs.Event{
				Timestamp: occurredAt,
				Message: formatCloudWatchLogsEntryAt(
					entry,
					occurredAt,
					logFormatter,
				),
			},
		})
	}
	return records
}

// formatCloudWatchLogsEntryAtは、buffer待機後も本文のtimestampをevent発生日時へ固定します。
// 元LogEntryは変更せず、CloudWatch Logsへ送る一時copyへ発生時刻を設定します。
func formatCloudWatchLogsEntryAt(
	entry *logging.LogEntry,
	occurredAt time.Time,
	logFormatter formatter.PlainTextFormatter,
) string {
	if entry == nil {
		return ""
	}
	if occurredAt.IsZero() {
		return logFormatter.Format(entry)
	}
	formattedEntry := *entry
	formattedEntry.OccurredAt = occurredAt
	return logFormatter.Format(&formattedEntry)
}

// cloudWatchLogsBufferedEntryBytesはbufferの早期flush判定に使う、AWSの
// PutLogEvents request size公式と同じmessage UTF-8 bytes + 26 bytes/eventを返します。
// BufferFlushBytesはsoft thresholdであり、物理requestの分割はplannerが別途行います。
func cloudWatchLogsBufferedEntryBytes(
	entry *logging.LogEntry,
	logFormatter formatter.PlainTextFormatter,
) int {
	occurredAt := entryTimestamp(entry)
	return jsonutil.UTF8StringBytes(formatCloudWatchLogsEntryAt(entry, occurredAt, logFormatter)) +
		cloudWatchLogsPutLogEventOverheadBytes
}

func cloudWatchLogsBufferedEntriesBytes(
	entries []*logging.LogEntry,
	logFormatter formatter.PlainTextFormatter,
) int {
	total := 0
	for _, entry := range entries {
		total += cloudWatchLogsBufferedEntryBytes(entry, logFormatter)
	}
	return total
}

// splitValidCloudWatchLogsEventRecords は record 単位で確定できる不正 event を送信対象から外します。
func splitValidCloudWatchLogsEventRecords(
	records []cloudWatchLogsEventRecord,
) ([]cloudWatchLogsEventRecord, []invalidCloudWatchLogsEventRecord) {
	valid := make([]cloudWatchLogsEventRecord, 0, len(records))
	invalid := make([]invalidCloudWatchLogsEventRecord, 0)

	for _, record := range records {
		if err := validateCloudWatchLogsEventRecord(record); err != nil {
			invalid = append(invalid, invalidCloudWatchLogsEventRecord{
				record: record,
				err:    err,
			})
			continue
		}
		valid = append(valid, record)
	}
	return valid, invalid
}

func validateCloudWatchLogsEventRecord(record cloudWatchLogsEventRecord) error {
	if record.entry != nil && record.entry.Message == "" {
		return infracloudwatchlogs.ClassifyEventMessage("")
	}
	return infracloudwatchlogs.ClassifyEventMessage(record.event.Message)
}

// cloudWatchLogsEventsFromRecords は送信用の Event slice だけを取り出します。
func cloudWatchLogsEventsFromRecords(records []cloudWatchLogsEventRecord) []pkgcloudwatchlogs.Event {
	if len(records) == 0 {
		return nil
	}

	events := make([]pkgcloudwatchlogs.Event, 0, len(records))
	for _, record := range records {
		events = append(events, record.event)
	}
	return events
}

// adaptCloudWatchLogsRecordsToRequestPlanはLogger固有のrecordをbackend plannerの入力へ変換し、
// stream routingまたはAWSの一request制限で確定したphysical requestを返します。
// 単独でも送信できないeventは、元LogEntryを保持したinvalid recordへ戻します。
func adaptCloudWatchLogsRecordsToRequestPlan(
	resolver *infracloudwatchlogs.LogStreamNameResolver,
	records []cloudWatchLogsEventRecord,
) (
	[]infracloudwatchlogs.PlannedPutLogEventsRequest,
	[]invalidCloudWatchLogsEventRecord,
	error,
) {
	indexedEvents := make([]infracloudwatchlogs.IndexedEvent, len(records))
	recordByInputIndex := make(map[int]cloudWatchLogsEventRecord, len(records))
	for index, record := range records {
		indexedEvents[index] = infracloudwatchlogs.IndexedEvent{
			InputIndex: record.inputIndex,
			Event:      record.event,
		}
		recordByInputIndex[record.inputIndex] = record
	}

	requests, invalidEvents, err := infracloudwatchlogs.PlanPutLogEventsRequests(
		resolver,
		indexedEvents,
	)
	if err != nil {
		return nil, nil, err
	}

	invalidRecords := make([]invalidCloudWatchLogsEventRecord, 0, len(invalidEvents))
	for _, invalidEvent := range invalidEvents {
		record, exists := recordByInputIndex[invalidEvent.InputIndex]
		if !exists {
			return nil, nil, fmt.Errorf(
				"cloudwatch logs request planner returned unknown invalid input index %d",
				invalidEvent.InputIndex,
			)
		}
		invalidRecords = append(invalidRecords, invalidCloudWatchLogsEventRecord{
			record: record,
			err:    invalidEvent.Err,
		})
	}
	return requests, invalidRecords, nil
}

// cloudWatchLogsRecordsForPlannedRequestはplannerが保持した元入力indexから、送信順の
// logger recordを復元します。AWS responseのrejection indexはこの返り値の順序に対応します。
func cloudWatchLogsRecordsForPlannedRequest(
	inputRecords []cloudWatchLogsEventRecord,
	request infracloudwatchlogs.PlannedPutLogEventsRequest,
) ([]cloudWatchLogsEventRecord, error) {
	records := make([]cloudWatchLogsEventRecord, 0, len(request.Events))
	for _, indexedEvent := range request.Events {
		if indexedEvent.InputIndex < 0 || indexedEvent.InputIndex >= len(inputRecords) {
			return nil, fmt.Errorf(
				"cloudwatch logs request planner returned unknown input index %d",
				indexedEvent.InputIndex,
			)
		}
		record := inputRecords[indexedEvent.InputIndex]
		if record.inputIndex != indexedEvent.InputIndex {
			return nil, fmt.Errorf(
				"cloudwatch logs input record at position %d has input index %d",
				indexedEvent.InputIndex,
				record.inputIndex,
			)
		}
		records = append(records, record)
	}
	return records, nil
}

// entryTimestampはログ生成時に確定したOccurredAtを優先します。
// 既存の呼び出し元などがOccurredAtを設定していない場合だけ現在時刻で補完します。
func entryTimestamp(entry *logging.LogEntry) time.Time {
	return logging.ResolveEntryOccurredAt(entry)
}
