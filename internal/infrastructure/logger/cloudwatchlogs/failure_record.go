package cloudwatchlogs

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// buildFailedRecordsFromPutError は PutLogEvents 全体が失敗した場合に、入力全件を失敗 record 化します。
// RetryableRecords、DeadRecords、QuarantineRecordsへの振り分けは呼出し元がRetryDecisionを見て行います。
func (fm *CloudWatchLogsFlushManager) buildFailedRecordsFromPutError(
	attemptedAt time.Time,
	logStreamName string,
	eventRecords []cloudWatchLogsEventRecord,
	dec retryable.RetryDecision,
	err error,
) []loggerfailure.FailedRecord {
	// CloudWatch Logsが再送可能な応答でRetry-Afterを返した場合は、spool再送もその時刻まで待たせます。
	retryAfter := loggerfailure.RetryAfterSince(attemptedAt, fm.Clock, dec)
	records := make([]loggerfailure.FailedRecord, 0, len(eventRecords))
	for _, eventRecord := range eventRecords {
		doc := fm.convertEventRecordToDocument(eventRecord, logStreamName)
		records = append(records, loggerfailure.FailedRecord{
			Cause:          err,
			DeliveryStatus: loggerfailure.RequestFailureDeliveryStatus(dec),
			FailedAt:       attemptedAt,
			Operation:      "cloudwatch_logs_put",
			StatusCode:     dec.Code,
			FailureCode:    loggerfailure.FailureCode(dec.Reason),
			FailureReason:  err.Error(),
			Payload:        doc,
			RetryAfter:     retryAfter,
		})
	}
	return records
}

// buildFailedRecordsFromInvalidEvents は record 単位で検出できる不正 event を quarantine record 化します。
func (fm *CloudWatchLogsFlushManager) buildFailedRecordsFromInvalidEvents(
	attemptedAt time.Time,
	invalidRecords []invalidCloudWatchLogsEventRecord,
) []loggerfailure.FailedRecord {
	records := make([]loggerfailure.FailedRecord, 0, len(invalidRecords))
	for _, invalidRecord := range invalidRecords {
		doc := fm.convertEventRecordToDocument(invalidRecord.record, "")
		records = append(records, loggerfailure.FailedRecord{
			Cause:          invalidRecord.err,
			DeliveryStatus: pkglogger.DeliveryRejected,
			FailedAt:       attemptedAt,
			Operation:      "cloudwatch_logs_put",
			StatusCode:     0,
			FailureCode:    eventValidationFailureCode(invalidRecord.err),
			FailureReason:  invalidRecord.err.Error(),
			Payload:        doc,
		})
	}
	return records
}

func eventValidationFailureCode(err error) loggerfailure.FailureCode {
	var validationErr *infracloudwatchlogs.EventValidationError
	if errors.As(err, &validationErr) && validationErr.FailureCode != "" {
		return loggerfailure.FailureCode(validationErr.FailureCode)
	}
	return loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeInvalidEvent)
}

// addRejectedLogEventFailuresはHTTP 200内のevent拒否をsend resultへ追加します。
// CloudWatch Logsが返す拒否範囲を送信時のevent indexへ対応させ、too_old／expiredだけを
// DeadRecords、too_newだけをRetryableRecordsにします。拒否範囲外のeventは成功として残します。
func (fm *CloudWatchLogsFlushManager) addRejectedLogEventFailures(
	result *loggerfailure.SendResult,
	eventRecords []cloudWatchLogsEventRecord,
	logStreamName string,
	output *pkgcloudwatchlogs.PutLogEventsOutput,
) {
	if result == nil || output == nil || !output.HasRejectedLogEvents() {
		return
	}

	rejections := output.RejectedLogEventsInfo.RejectedEvents(len(eventRecords))
	if len(rejections) == 0 {
		return
	}
	for _, rejection := range rejections {
		if rejection.Index < 0 || rejection.Index >= len(eventRecords) {
			continue
		}
		failedRecord := fm.buildFailedRecordFromRejectedLogEvent(
			result.Meta.AttemptedAt,
			logStreamName,
			eventRecords[rejection.Index],
			rejection,
		)
		if infracloudwatchlogs.ClassifyRejectedLogEvent(rejection).Retry {
			result.RetryableRecords = append(result.RetryableRecords, failedRecord)
			continue
		}
		result.DeadRecords = append(result.DeadRecords, failedRecord)
	}
}

// buildFailedRecordFromRejectedLogEvent は CloudWatch Logs の部分拒否 1 件を durable 保存用 document にします。
func (fm *CloudWatchLogsFlushManager) buildFailedRecordFromRejectedLogEvent(
	attemptedAt time.Time,
	logStreamName string,
	eventRecord cloudWatchLogsEventRecord,
	rejection pkgcloudwatchlogs.RejectedLogEvent,
) loggerfailure.FailedRecord {
	doc := fm.convertEventRecordToDocument(eventRecord, logStreamName)
	failureCode := loggerfailure.FailureCode(
		infracloudwatchlogs.RejectedLogEventFailureCode(rejection.Reason),
	)
	// FailedRecord.Causeへ保持し、配送結果や退避処理の失敗時に返すerrorから
	// errors.Asで拒否理由を判別できるようにします。
	cause := &pkgcloudwatchlogs.RejectedLogEventError{Index: rejection.Index, Reason: rejection.Reason}
	return loggerfailure.FailedRecord{
		Cause:          cause,
		DeliveryStatus: pkglogger.DeliveryFailed,
		FailedAt:       attemptedAt,
		Operation:      "cloudwatch_logs_put",
		StatusCode:     http.StatusOK,
		FailureCode:    failureCode,
		FailureReason:  cause.Error(),
		Payload:        doc,
	}
}

// logRejectedCloudWatchLogsOutput は fallback logger で部分拒否を黙殺しないための最小通知を出します。
// fallback 経路では durable fallback を再帰させないため、ここでは標準 log への出力に留めます。
func logRejectedCloudWatchLogsOutput(prefix string, output *pkgcloudwatchlogs.PutLogEventsOutput, eventCount int) {
	if output == nil {
		return
	}
	if output.HasRejectedLogEvents() {
		rejections := output.RejectedLogEventsInfo.RejectedEvents(eventCount)
		if len(rejections) > 0 {
			log.Printf("%s: cloudwatch logs rejected %d log event(s): %v", prefix, len(rejections), rejections)
		}
	}
	if output.RejectedEntityInfo != nil && output.RejectedEntityInfo.ErrorType != "" {
		log.Printf("%s: cloudwatch logs rejected entity: %s", prefix, output.RejectedEntityInfo.ErrorType)
	}
}

// convertEventRecordToDocument は CloudWatch Logs 送信失敗を durable 保存するための document を作ります。
// timestamp と line は、初回 PutLogEvents に渡した Event と同じ値を保存します。
// log_groupは失敗時の送信先を記録する診断情報です。log_streamはrouting granularity有効時も
// 実際のPutLogEvents requestへ指定した値を保存し、spool再送時の送信先として使います。
// これにより、再送時の設定や時刻が変わっても初回requestと同じstreamへ送信できます。
func (fm *CloudWatchLogsFlushManager) convertEventRecordToDocument(
	record cloudWatchLogsEventRecord,
	logStreamName string,
) map[string]any {
	if logStreamName == "" {
		logStreamName = fm.logStreamNameAt(record.event.Timestamp)
	}
	doc := map[string]any{
		"log_group":  fm.logGroupName(),
		"log_stream": logStreamName,
		"timestamp":  record.event.Timestamp.Format(time.RFC3339Nano),
		"line":       record.event.Message,
	}

	entry := record.entry
	if entry == nil {
		return doc
	}

	doc["level"] = entry.Level.String()
	doc["message"] = entry.Message
	doc["request_id"] = entry.RequestID
	doc["trace_id"] = entry.TraceID
	doc["span_id"] = entry.SpanID
	if len(entry.Fields) > 0 {
		doc["fields"] = entry.Fields
	}
	if entry.Err != nil {
		doc["error"] = entry.Err.Error()
	}
	if len(entry.FuncName) > 0 {
		doc["stack_trace"] = logging.BuildStackTraceArray(*entry)
	}
	return doc
}

// logGroupName は failure document に記録する送信先 log group 名を返します。
func (fm *CloudWatchLogsFlushManager) logGroupName() string {
	if fm.config != nil {
		return fm.config.LogGroupName()
	}
	return ""
}

// logStreamNameAtはevent発生日時を構築時に決定したtimezoneで解釈し、
// failure documentに記録する実log stream名を返します。resolverが使えない場合も
// 診断情報を失わないよう、設定されたbase名へfallbackします。
func (fm *CloudWatchLogsFlushManager) logStreamNameAt(occurredAt time.Time) string {
	if fm.config == nil {
		return ""
	}
	if fm.streamNameResolver == nil {
		return fm.config.LogStreamName()
	}
	resolved, err := fm.streamNameResolver.ResolveLogStreamNameForEvent(occurredAt)
	if err != nil {
		return fm.config.LogStreamName()
	}
	return resolved
}
