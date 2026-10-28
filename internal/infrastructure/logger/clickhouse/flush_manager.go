package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	clickhouseerrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse/errors"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/google/uuid"
)

type FlushManager interface {
	Flush(ctx context.Context, entries []*logging.LogEntry) error
	FlushWithNoFallback(ctx context.Context, entries []*logging.LogEntry)
	Close(ctx context.Context) error
}

type ClickHouseFlushManager struct {
	*LoggerBase
	client         Client
	config         ClickHouseFlushManagerRuntimeConfigProvider
	FailureHandler *flushfailure.CompositeFailureHandler
	resource       resource.ResourceProvider
	Tracer         pkgobserver.Tracer
	Meter          pkgobserver.Meter
}

// NewClickHouseFlushManagerはClickHouse送信用FlushManagerを生成します。
// loggerBaseはClickHouse loggerのconstructorでLogFormatterまで初期化済みでなければなりません。
func NewClickHouseFlushManager(
	loggerBase *LoggerBase,
	client Client,
	cfg ClickHouseFlushManagerRuntimeConfigProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	resourceProvider resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *ClickHouseFlushManager {
	return &ClickHouseFlushManager{
		LoggerBase:     loggerBase,
		client:         client,
		config:         cfg,
		FailureHandler: failureHandler,
		resource:       resourceProvider,
		Tracer:         observer.EnsureTracer(tracer),
		Meter:          observer.EnsureMeter(meter),
	}
}

func (fm *ClickHouseFlushManager) Flush(ctx context.Context, entries []*logging.LogEntry) error {
	return fm.flushPhysicalInsertRequests(ctx, entries, fm.handleFailure, true)
}

// FlushWithNoFallbackはfallback logger専用に、一回のsnapshotをfailure handlerへ
// 渡さず送信します。送信・計画の失敗は標準loggerへ記録し、snapshotは再投入しません。
func (fm *ClickHouseFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	if err := fm.flushPhysicalInsertRequests(ctx, entries, fm.logDroppedFallbackFailure, false); err != nil {
		log.Printf("failed to flush ClickHouse fallback logger: %v", err)
	}
}

// flushPhysicalInsertRequestsは一回限りのlogger snapshotをhard limit内の物理INSERTへ
// 分割し、requestごとに固有のdeduplication tokenを割り当てます。失敗recordの処理方法は
// failure callbackが決め、errorは集約して返します。snapshotは呼び出し元へ返しません。
// 送信失敗をdurable処理できなかった場合は、failure callbackのerrorとbackendの送信失敗の
// 原因を併せて返します。countRecordsがtrueなら、送信の結果をrecordの件数のmetricsに
// 数えます。fallback loggerが送る内部logは数えません。
func (fm *ClickHouseFlushManager) flushPhysicalInsertRequests(
	ctx context.Context,
	entries []*logging.LogEntry,
	handleFailure func(context.Context, loggerfailure.SendResult) error,
	countRecords bool,
) error {
	if len(entries) == 0 {
		return nil
	}
	if fm == nil || fm.LoggerBase == nil || fm.LoggerBase.config == nil {
		return fmt.Errorf("ClickHouse logger config is not initialized")
	}
	if handleFailure == nil {
		return fmt.Errorf("ClickHouse failure callback is not initialized")
	}

	entryRecords := fm.convertLogEntriesToEntryRecords(entries)
	records := clickHouseLogRecordsFromEntryRecords(entryRecords)
	plan, err := infraclickhouse.PlanInsertRequests(records, fm.LoggerBase.config.MaxInsertRequestBytes())
	if err != nil {
		return err
	}

	var errs []error

	if len(plan.Rejected) > 0 {
		result := fm.buildPreSendRejectedResult(ctx, entryRecords, plan.Rejected)
		if countRecords {
			telemetry.RecordSendResult(ctx, fm.Meter, result)
		}
		if err := joinUnhandledSendFailure(nil, result, handleFailure(ctx, result)); err != nil {
			errs = append(errs, err)
		}
	}

	if len(plan.Requests) > 0 {
		if err := fm.validateClient(ctx); err != nil {
			errs = append(errs, err)
			return errors.Join(errs...)
		}
	}

	for _, request := range plan.Requests {
		requestEntryRecords := make([]clickHouseLogEntryRecord, 0, len(request.InputIndexes))
		for _, inputIndex := range request.InputIndexes {
			requestEntryRecords = append(requestEntryRecords, entryRecords[inputIndex])
		}
		result, err := fm.insertEntryRecords(
			ctx,
			requestEntryRecords,
			newClickHouseInsertDeduplicationToken(),
		)
		if countRecords {
			telemetry.RecordSendResult(ctx, fm.Meter, result)
		}
		if result.HasFailedRecords() {
			err = joinUnhandledSendFailure(err, result, handleFailure(ctx, result))
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// joinUnhandledSendFailureは送信失敗recordをdurable処理へ渡した結果をerrへ結合します。
// durable処理を完了できなかった場合（failure handler未設定を含む）は、failure handlerの
// errorだけでは送信先の拒否理由が失われるため、backendの送信失敗の原因も加え、
// errors.Is／errors.Asで判別できるようにします。durable処理が完了した場合はerrだけを返します。
func joinUnhandledSendFailure(err error, res loggerfailure.SendResult, failureErr error) error {
	if failureErr == nil {
		return err
	}
	return errors.Join(err, sendFailureCause(err, res), failureErr)
}

// sendFailureCauseはSendResultが保持するbackendの送信失敗の原因を一つ返します。
// TerminalErrorを優先し、ない場合は元のerrorを保持する最初のrecordのCauseを使います。
// errが既に同じ原因を含む場合は、重複させないようnilを返します。
func sendFailureCause(err error, res loggerfailure.SendResult) error {
	cause := res.TerminalError
	if cause == nil {
		cause = firstFailedRecordCause(res)
	}
	if cause == nil || errors.Is(err, cause) {
		return nil
	}
	return cause
}

// firstFailedRecordCauseは元のerrorを保持する最初のfailed recordのCauseを返します。
func firstFailedRecordCause(res loggerfailure.SendResult) error {
	for _, records := range [][]loggerfailure.FailedRecord{
		res.RetryableRecords,
		res.DeadRecords,
		res.QuarantineRecords,
	} {
		for _, record := range records {
			if record.Cause != nil {
				return record.Cause
			}
		}
	}
	return nil
}

// handleFailureはbackend送信に失敗したrecordのdurable処理完了を待ちます。
// handler未設定時も失敗を成功扱いせず、呼び出し元へ通知します。
func (fm *ClickHouseFlushManager) handleFailure(ctx context.Context, res loggerfailure.SendResult) error {
	if fm.FailureHandler == nil {
		return flushfailure.ErrNoFailureHandler
	}
	return fm.FailureHandler.HandleFailure(ctx, res)
}

// logDroppedFallbackFailureはfallback snapshotの失敗recordを再退避せず終了します。
// fallback自体の失敗は再帰させず、標準loggerへ件数だけ記録します。
func (fm *ClickHouseFlushManager) logDroppedFallbackFailure(
	_ context.Context,
	res loggerfailure.SendResult,
) error {
	log.Printf("ClickHouse fallback logger dropped %d failed record(s)", res.FailedCount())
	return nil
}

// insertInternalLogDirectlyは、On*の中から再入した書き込みで発生した内部ログ一件を、遅延や
// 失敗を通知せず、failure handlerにも渡さずに一回のINSERTで同じtableへ書き込みます。失敗は
// fallback loggerの送信と同じく標準loggerへ残します。この書き込みは内部ログを通知しないため、
// 再入はここで終わります。
func (fm *ClickHouseFlushManager) insertInternalLogDirectly(ctx context.Context, entry *logging.LogEntry) {
	if fm == nil || fm.client == nil {
		log.Printf("failed to write ClickHouse internal log directly: ClickHouse client is not initialized")
		return
	}
	if err := ctx.Err(); err != nil {
		log.Printf("failed to write ClickHouse internal log directly: %v", err)
		return
	}
	records := clickHouseLogRecordsFromEntryRecords(fm.convertLogEntriesToEntryRecords([]*logging.LogEntry{entry}))
	response, err := fm.client.InsertLogsWithOptions(ctx, records, pkgclickhouse.InsertOptions{
		InsertDeduplicationToken: newClickHouseInsertDeduplicationToken(),
	})
	if err == nil && response == nil {
		err = infraclickhouse.ErrMissingInsertResponse
	}
	if err != nil {
		log.Printf("failed to write ClickHouse internal log directly: %v", err)
	}
}

func (fm *ClickHouseFlushManager) Close(ctx context.Context) error {
	if fm == nil || fm.FailureHandler == nil {
		return nil
	}
	return fm.FailureHandler.Drain(ctx)
}

// insertEntryRecordsは一つの物理HTTP INSERTを実行し、同じtokenを送信requestと
// failure payloadの両方へ使用します。呼び出し側がhard limit内に計画した単位を崩しません。
func (fm *ClickHouseFlushManager) insertEntryRecords(
	ctx context.Context,
	entryRecords []clickHouseLogEntryRecord,
	insertDeduplicationToken string,
) (loggerfailure.SendResult, error) {
	if len(entryRecords) == 0 {
		return loggerfailure.SendResult{}, nil
	}
	if err := fm.validateClient(ctx); err != nil {
		return loggerfailure.SendResult{}, err
	}
	requestID, _ := requestctx.RequestID(ctx)
	result := loggerfailure.SendResult{
		Meta: fm.newSendMeta(ctx, requestID, len(entryRecords)),
	}
	if err := ctx.Err(); err != nil {
		// clientは呼び出していません。後の再送で受理済みのrecordを配送できるよう、
		// この物理requestとそのtokenを変更せずに保持します。
		failureDetectedAt := result.Meta.AttemptedAt
		result.Meta.AttemptedAt = time.Time{}
		result.TerminalError = err
		result.RetryableRecords = fm.buildFailedRecordsFromInsertError(
			failureDetectedAt, entryRecords,
			retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonRequestNotSent}, err,
			insertDeduplicationToken, infraclickhouse.DurableReplayModeBatch,
		)
		return result, err
	}

	records := clickHouseLogRecordsFromEntryRecords(entryRecords)
	requestCtx, insert := telemetry.StartRequest(ctx, fm.Tracer, fm.Meter, SpanClickHouseInsert, deliveryBackendClickHouse, len(entryRecords))

	start := time.Now()
	response, err := fm.client.InsertLogsWithOptions(requestCtx, records, pkgclickhouse.InsertOptions{
		InsertDeduplicationToken: insertDeduplicationToken,
	})
	elapsed := time.Since(start)
	fm.handleInsertDelayLog(ctx, elapsed)

	// cancellationが完了と競合しても、成功responseを確定した結果として扱います。
	// quarantineが必要なのは、結果を確認できない場合だけです。
	if err == nil && response != nil {
		insert.End(nil)
		return result, nil
	}
	if err == nil {
		// 応答がなければINSERTの成否を確認できません。spool再送と同じく結果不明として
		// 分類し、送信したrecordを自動再送せずquarantineへ隔離します。
		err = infraclickhouse.ErrMissingInsertResponse
	}

	insert.End(err)
	result.TerminalError = err
	dec := retryable.ClassifyDurableRequestOutcome(ctx, err, func(classifyCtx context.Context, attemptErr error) retryable.RetryDecision {
		return infraclickhouse.ClassifyDurableInsertFailure(classifyCtx, attemptErr, len(entryRecords))
	})
	var cancellationErr error
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		cancellationErr = ctx.Err()
	}

	var marshalErr *pkgclickhouse.LogRecordMarshalError
	if errors.As(err, &marshalErr) && marshalErr != nil &&
		marshalErr.Index >= 0 && marshalErr.Index < len(entryRecords) {
		failureDetectedAt := result.Meta.AttemptedAt
		// LogRecordMarshalErrorはHTTP requestを作る前のlocal failureです。
		// FailedAtに検出時刻を残しつつ、backend送信試行は記録しません。
		result.Meta.AttemptedAt = time.Time{}
		result.QuarantineRecords = append(result.QuarantineRecords, fm.buildFailedRecord(
			failureDetectedAt,
			entryRecords[marshalErr.Index],
			retryable.RetryDecision{},
			err,
			insertDeduplicationToken,
			infraclickhouse.DurableReplayModeNone,
			FailureCodeLogRecordEncoding,
		))

		if len(entryRecords) > 1 {
			for index, entryRecord := range entryRecords {
				if index == marshalErr.Index {
					continue
				}
				result.RetryableRecords = append(result.RetryableRecords, fm.buildFailedRecord(
					failureDetectedAt,
					entryRecord,
					dec,
					err,
					insertDeduplicationToken,
					infraclickhouse.DurableReplayModeRecordIsolation,
					loggerfailure.FailureCode(dec.Reason),
				))
			}
		}
		return result, nil
	}

	replayMode := clickHouseDurableReplayMode(dec)
	failedRecords := fm.buildFailedRecordsFromInsertError(
		result.Meta.AttemptedAt,
		entryRecords,
		dec,
		err,
		insertDeduplicationToken,
		replayMode,
	)
	if dec.Quarantine {
		result.QuarantineRecords = failedRecords
	} else if dec.Retry {
		result.RetryableRecords = failedRecords
	} else {
		result.DeadRecords = failedRecords
	}
	return result, cancellationErr
}

func (fm *ClickHouseFlushManager) buildPreSendRejectedResult(
	ctx context.Context,
	entryRecords []clickHouseLogEntryRecord,
	rejected []infraclickhouse.RejectedInsertRecord,
) loggerfailure.SendResult {
	requestID, _ := requestctx.RequestID(ctx)
	result := loggerfailure.SendResult{
		Meta: fm.newSendMeta(ctx, requestID, len(rejected)),
	}
	failureDetectedAt := result.Meta.AttemptedAt
	result.Meta.AttemptedAt = time.Time{}
	var terminalErrors []error
	for _, rejection := range rejected {
		terminalErrors = append(terminalErrors, rejection.Err)
		failureCode := FailureCodeLogRecordEncoding
		if errors.Is(rejection.Err, infraclickhouse.ErrLogRecordTooLarge) {
			failureCode = FailureCodeLogRecordTooLarge
		}
		result.QuarantineRecords = append(result.QuarantineRecords, fm.buildFailedRecord(
			failureDetectedAt,
			entryRecords[rejection.InputIndex],
			retryable.RetryDecision{},
			rejection.Err,
			newClickHouseInsertDeduplicationToken(),
			infraclickhouse.DurableReplayModeNone,
			failureCode,
		))
	}
	result.TerminalError = errors.Join(terminalErrors...)
	return result
}

func newClickHouseInsertDeduplicationToken() string {
	return uuid.NewString()
}

// ErrClientNotInitializedは、送信に使うClickHouse clientが設定されていないことを表します。
var ErrClientNotInitialized = errors.New("ClickHouse client is not initialized")

func (fm *ClickHouseFlushManager) validateClient(ctx context.Context) error {
	if fm.client != nil {
		return nil
	}

	err := ErrClientNotInitialized
	fm.notifyError(
		ctx,
		&clickhouseerrors.ClickHouseLoggerInternalError{Op: "clickhouse validate client", Err: err},
		fm.InternalErrorEntry("ClickHouse client is not initialized", "", err),
		fm.Tracer,
		fm.Meter,
	)
	return err
}

type clickHouseLogEntryRecord struct {
	recordID string
	record   pkgclickhouse.LogRecord
}

func (fm *ClickHouseFlushManager) convertLogEntriesToEntryRecords(entries []*logging.LogEntry) []clickHouseLogEntryRecord {
	if len(entries) == 0 {
		return nil
	}

	records := make([]clickHouseLogEntryRecord, 0, len(entries))
	for _, entry := range entries {
		records = append(records, clickHouseLogEntryRecord{
			recordID: uuid.NewString(),
			record:   fm.convertLogEntryToRecord(entry),
		})
	}
	return records
}

func clickHouseLogRecordsFromEntryRecords(records []clickHouseLogEntryRecord) []pkgclickhouse.LogRecord {
	if len(records) == 0 {
		return nil
	}

	out := make([]pkgclickhouse.LogRecord, 0, len(records))
	for _, record := range records {
		out = append(out, record.record)
	}
	return out
}

func (fm *ClickHouseFlushManager) convertLogEntryToRecord(entry *logging.LogEntry) pkgclickhouse.LogRecord {
	return convertClickHouseLogEntryToRecord(fm.LoggerBase, fm.resource, entry)
}

func entryTimestamp(entry *logging.LogEntry) time.Time {
	return logging.ResolveEntryOccurredAt(entry)
}

func stackTrace(entry *logging.LogEntry) []map[string]interface{} {
	if entry == nil || len(entry.FuncName) == 0 {
		return nil
	}
	return logging.BuildStackTraceArray(*entry)
}

func (fm *ClickHouseFlushManager) handleInsertDelayLog(ctx context.Context, elapsed time.Duration) {
	if fm == nil || fm.config == nil || !fm.config.InsertLatencyEnabled() {
		return
	}

	switch {
	case fm.config.PostErrorThresholdDuration() > 0 && elapsed > fm.config.PostErrorThresholdDuration():
		err := fmt.Errorf("clickhouse insert delay exceeded error threshold: %s", elapsed)
		fm.notifyError(
			ctx,
			&clickhouseerrors.ClickHouseLoggerInternalError{Op: "clickhouse insert delay", Err: err},
			fm.InternalErrorEntry(fmt.Sprintf("clickhouse insert delay exceeded error threshold: %s", elapsed), "", nil),
			fm.Tracer,
			fm.Meter,
		)
	case fm.config.PostWarnThresholdDuration() > 0 && elapsed > fm.config.PostWarnThresholdDuration():
		fm.notifyWarn(
			ctx,
			fm.InternalWarnEntry(fmt.Sprintf("clickhouse insert delay exceeded warn threshold: %s", elapsed), ""),
			fm.Tracer,
			fm.Meter,
		)
	}
}

func (fm *ClickHouseFlushManager) newSendMeta(ctx context.Context, requestID string, inputCount int) loggerfailure.Meta {
	now := time.Now().UTC()
	if fm.Clock != nil {
		now = fm.Clock.Now()
	}

	meta := loggerfailure.Meta{
		Backend:     deliveryBackendClickHouse,
		RequestID:   requestID,
		AttemptedAt: now,
		InputCount:  inputCount,
		TraceID:     string(observer.TraceIDFromContext(ctx)),
	}
	if fm.resource != nil {
		meta.AppName = fm.resource.ServiceName()
		meta.HostName = fm.resource.HostName()
		meta.Environment = fm.resource.Environment()
		meta.RuntimeType = fm.resource.RuntimeType()
		meta.ExecutionUnitID = fm.resource.RuntimeInstanceID()
		meta.Version = fm.resource.ServiceVersion()
	}
	return meta
}

func (fm *ClickHouseFlushManager) buildFailedRecordsFromInsertError(
	attemptedAt time.Time,
	entryRecords []clickHouseLogEntryRecord,
	dec retryable.RetryDecision,
	err error,
	insertDeduplicationToken string,
	replayMode infraclickhouse.DurableReplayMode,
) []loggerfailure.FailedRecord {
	records := make([]loggerfailure.FailedRecord, 0, len(entryRecords))
	for _, entryRecord := range entryRecords {
		records = append(records, fm.buildFailedRecord(
			attemptedAt,
			entryRecord,
			dec,
			err,
			insertDeduplicationToken,
			replayMode,
			loggerfailure.FailureCode(dec.Reason),
		))
	}
	return records
}

const (
	// FailureCodeLogRecordEncodingは、ClickHouseへ送るJSONEachRowへ符号化できないため
	// 送信前に隔離したrecordのFailureCodeです。
	FailureCodeLogRecordEncoding loggerfailure.FailureCode = "clickhouse_log_record_encoding"
	// FailureCodeLogRecordTooLargeは、一件のrowだけでINSERT requestの上限を超えるため
	// 送信前に隔離したrecordのFailureCodeです。
	FailureCodeLogRecordTooLarge loggerfailure.FailureCode = "clickhouse_log_record_too_large"
)

func clickHouseDurableReplayMode(dec retryable.RetryDecision) infraclickhouse.DurableReplayMode {
	if !dec.Retry {
		return infraclickhouse.DurableReplayModeNone
	}
	if dec.Reason == infraclickhouse.RetryReasonRecordIsolationRequired {
		return infraclickhouse.DurableReplayModeRecordIsolation
	}
	return infraclickhouse.DurableReplayModeBatch
}

func (fm *ClickHouseFlushManager) buildFailedRecord(
	attemptedAt time.Time,
	entryRecord clickHouseLogEntryRecord,
	dec retryable.RetryDecision,
	err error,
	insertDeduplicationToken string,
	replayMode infraclickhouse.DurableReplayMode,
	failureCode loggerfailure.FailureCode,
) loggerfailure.FailedRecord {
	doc := fm.convertLogEntryRecordToDocument(entryRecord)
	doc[pkgclickhouse.SettingInsertDeduplicationToken] = insertDeduplicationToken
	doc[infraclickhouse.DurablePayloadFieldRecordID] = entryRecord.recordID
	doc[infraclickhouse.DurablePayloadFieldReplayMode] = replayMode

	return loggerfailure.FailedRecord{
		// 保存するFailureReasonとは別に、process内で判別できる元のerrorを保持します。
		Cause:         err,
		FailedAt:      attemptedAt,
		Operation:     "clickhouse_insert",
		StatusCode:    dec.Code,
		FailureCode:   failureCode,
		FailureReason: err.Error(),
		Payload:       doc,
		// ClickHouseが再送可能な応答でRetry-Afterを返した場合は、spool再送もその時刻まで待たせます。
		RetryAfter: loggerfailure.RetryAfterSince(attemptedAt, fm.Clock, dec),
	}
}

func (fm *ClickHouseFlushManager) convertLogEntryRecordToDocument(entryRecord clickHouseLogEntryRecord) map[string]any {
	record := entryRecord.record
	doc := map[string]any{
		"timestamp":         record.Timestamp.Format(time.RFC3339Nano),
		"level":             record.Level,
		"message":           record.Message,
		"line":              record.Line,
		"request_id":        record.RequestID,
		"trace_id":          record.TraceID,
		"span_id":           record.SpanID,
		"app_name":          record.AppName,
		"host_name":         record.HostName,
		"environment":       record.Environment,
		"runtime_type":      record.RuntimeType,
		"execution_unit_id": record.ExecutionUnitID,
		"version":           record.Version,
		"source":            record.Source,
	}
	if record.Error != "" {
		doc["error"] = record.Error
	}
	if len(record.StackTrace) > 0 {
		doc["stack_trace"] = record.StackTrace
	}
	if len(record.Fields) > 0 {
		doc["fields"] = record.Fields
	}
	return doc
}
