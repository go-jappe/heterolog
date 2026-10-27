package loki

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	lokierrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type FlushManager interface {
	Flush(ctx context.Context, entries []*logging.LogEntry) error
	Push(ctx context.Context, entry *logging.LogEntry) error
	PushWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error)
	FlushWithNoFallback(ctx context.Context, entries []*logging.LogEntry)
	Close(ctx context.Context) error
}

type LokiFlushManager struct {
	*LoggerBase
	client               Client
	config               LokiFlushManagerRuntimeConfigProvider
	FailureHandler       *flushfailure.CompositeFailureHandler
	FailureReportHandler *flushfailure.ReportingCompositeFailureHandler
	resource             resource.ResourceProvider
	Tracer               pkgobserver.Tracer
	Meter                pkgobserver.Meter
}

// NewLokiFlushManagerはLoki送信用FlushManagerを生成します。
// loggerBaseはLoki loggerのconstructorでLogFormatterまで初期化済みでなければなりません。
func NewLokiFlushManager(
	loggerBase *LoggerBase,
	client Client,
	cfg LokiFlushManagerRuntimeConfigProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	resourceProvider resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *LokiFlushManager {
	return newLokiFlushManager(
		loggerBase,
		client,
		cfg,
		failureHandler,
		nil,
		resourceProvider,
		tracer,
		meter,
	)
}

func newLokiFlushManager(
	loggerBase *LoggerBase,
	client Client,
	cfg LokiFlushManagerRuntimeConfigProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	failureReportHandler *flushfailure.ReportingCompositeFailureHandler,
	resourceProvider resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *LokiFlushManager {
	return &LokiFlushManager{
		LoggerBase:           loggerBase,
		client:               client,
		config:               cfg,
		FailureHandler:       failureHandler,
		FailureReportHandler: failureReportHandler,
		resource:             resourceProvider,
		Tracer:               observer.EnsureTracer(tracer),
		Meter:                observer.EnsureMeter(meter),
	}
}

// Flushはbuffer snapshotを送信します。送信失敗をdurable処理できなかった場合は、
// failure handlerのerrorとbackendの送信失敗の原因を併せて返します。
func (fm *LokiFlushManager) Flush(ctx context.Context, entries []*logging.LogEntry) error {
	res, err := fm.pushLogEntries(ctx, entries)
	if res.HasFailedRecords() {
		return joinUnhandledSendFailure(err, res, fm.handleFailure(ctx, res))
	}
	return err
}

// Pushは1件を即時送信します。送信失敗をdurable処理できなかった場合は、
// failure handlerのerrorとbackendの送信失敗の原因を併せて返します。
func (fm *LokiFlushManager) Push(ctx context.Context, entry *logging.LogEntry) error {
	res, err := fm.pushLogEntries(ctx, []*logging.LogEntry{entry})
	if res.HasFailedRecords() {
		return joinUnhandledSendFailure(err, res, fm.handleFailure(ctx, res))
	}
	return err
}

func (fm *LokiFlushManager) PushWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error) {
	res, err := fm.pushLogEntries(ctx, []*logging.LogEntry{entry})
	if err != nil && !res.HasFailedRecords() {
		return pkglogger.NewTargetDeliveryReport(lokiDeliveryRejectedResult(err)), err
	}
	if !res.HasSendFailure() {
		return pkglogger.NewTargetDeliveryReport(lokiDeliverySucceededResult()), nil
	}

	report := pkglogger.NewTargetDeliveryReport(lokiTargetDeliveryResult(res))
	failureReport, failureErr := fm.handleFailureWithReport(ctx, res)
	report.Durables = append(report.Durables, failureReport.Durables...)
	report.Evacuations = append(report.Evacuations, failureReport.Evacuations...)
	return report, errors.Join(err, report.Targets[0].Err, report.DurableError(), report.EvacuationError(), failureErr)
}

// FlushWithNoFallback は fallback logger 専用の batch 送信経路です。
// 計画した各requestを一度だけ送信し、失敗entryはfailure handlerへ渡さず破棄します。
func (fm *LokiFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	if len(entries) == 0 {
		return
	}
	if fm == nil || fm.client == nil {
		log.Printf("failed to flush Loki fallback logger: Loki client is not initialized")
		return
	}
	if err := ctx.Err(); err != nil {
		log.Printf("failed to flush Loki fallback logger: %v", err)
		return
	}

	entryRecords := fm.convertLogEntriesToEntryRecords(entries)
	plan, err := infraloki.PlanPushRequests(
		pushRecordsFromEntryRecords(entryRecords),
		fm.pushLimits(),
	)
	if err != nil {
		log.Printf("failed to plan Loki fallback logger push: %v", err)
		return
	}
	if len(plan.Rejected) > 0 {
		first := plan.Rejected[0]
		log.Printf(
			"Loki fallback push plan dropped %d log entries; first rejection at input index %d: %v",
			len(plan.Rejected),
			first.Record.InputIndex,
			first.Err,
		)
	}
	for _, request := range plan.Requests {
		if err := ctx.Err(); err != nil {
			log.Printf("failed to flush Loki fallback logger: %v", err)
			return
		}
		if err := fm.client.Push(ctx, request.Streams); err != nil {
			log.Printf("failed to flush Loki fallback logger: %v", err)
		}
	}
}

func (fm *LokiFlushManager) Close(ctx context.Context) error {
	if fm == nil || fm.FailureHandler == nil {
		return nil
	}
	return fm.FailureHandler.Drain(ctx)
}

func (fm *LokiFlushManager) pushLogEntries(ctx context.Context, entries []*logging.LogEntry) (loggerfailure.SendResult, error) {
	if len(entries) == 0 {
		return loggerfailure.SendResult{}, nil
	}
	if err := fm.validateClient(ctx); err != nil {
		return loggerfailure.SendResult{}, err
	}
	requestID, _ := requestctx.RequestID(ctx)
	result := loggerfailure.SendResult{
		Meta: fm.newSendMeta(ctx, requestID, len(entries)),
	}

	entryRecords := fm.convertLogEntriesToEntryRecords(entries)
	plan, err := infraloki.PlanPushRequests(
		pushRecordsFromEntryRecords(entryRecords),
		fm.pushLimits(),
	)
	if err != nil {
		return result, fmt.Errorf("plan Loki push requests: %w", err)
	}
	for _, rejected := range plan.Rejected {
		result.QuarantineRecords = append(
			result.QuarantineRecords,
			fm.buildLocallyRejectedRecord(entryRecords[rejected.Record.InputIndex], rejected.Err),
		)
	}

	ctx, span := fm.Tracer.StartSpan(ctx, SpanLokiPush)
	defer span.End()

	state := lokiPushState{result: &result, span: span}
	for _, request := range plan.Requests {
		fm.pushRequest(ctx, &state, entryRecords, request, nil)
	}

	// 一部requestの成功は確定結果として保持できるため、TerminalErrorは全network requestが
	// 失敗した場合だけ設定します。recordごとの失敗理由は常にFailures側へ残ります。
	if len(state.requestErrors) > 0 && state.succeededPushes == 0 {
		result.TerminalError = errors.Join(state.requestErrors...)
	}
	return result, state.cancellationErr
}

// lokiPushStateは、一回のflushで送信する物理requestの結果を集約します。
type lokiPushState struct {
	result          *loggerfailure.SendResult
	span            pkgobserver.Span
	requestErrors   []error
	cancellationErr error
	succeededPushes int
}

// pushRequestは計画済みの一requestを送信し、失敗したentryを分類してresultへ加えます。
// HTTP 413ではどのentryも受理されていないため、複数entryのrequestは半分ずつに分けて
// 再送し、一件でも拒否されたentryだけをquarantineへ隔離します。HTTP 400は一部の
// streamを受理した可能性があるため、分割して再送せず分類器の判定どおりquarantineへ
// 隔離します。splitFromは、このrequestを分割する原因となった413応答です。
func (fm *LokiFlushManager) pushRequest(
	ctx context.Context,
	state *lokiPushState,
	entryRecords []lokiLogEntryRecord,
	request infraloki.PlannedPushRequest,
	splitFrom error,
) {
	result := state.result
	requestEntryRecords := entryRecordsForPushRequest(entryRecords, request)
	if err := ctx.Err(); err != nil {
		// このrequestは一度もclientへ渡していません。再送のために保持し、先行する成功または
		// 結果不明のrequestは、それぞれの結果を保ちます。分割したrequestは、分割前の
		// request全体を拒否した確定済みの413を保持します。
		decision := retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonRequestNotSent}
		cause := err
		if splitFrom != nil {
			decision.Code = http.StatusRequestEntityTooLarge
			cause = errors.Join(splitFrom, err)
		}
		state.cancellationErr = err
		state.requestErrors = append(state.requestErrors, cause)
		result.RetryableRecords = append(result.RetryableRecords, fm.buildFailedRecordsFromPushError(
			result.Meta.AttemptedAt, requestEntryRecords, decision, cause,
		)...)
		return
	}

	start := time.Now()
	err := fm.client.Push(ctx, request.Streams)
	elapsed := time.Since(start)
	fm.handlePushDelayLog(ctx, elapsed)

	// 確定したresponseは、その返却とcancellationが競合しても確定結果として扱います。
	// 確認応答を受けた成功を再送に変えません。
	if err == nil {
		state.succeededPushes++
		return
	}

	state.span.RecordError(err)
	if infraloki.IsRequestEntityTooLarge(err) {
		if first, second, ok := infraloki.SplitPushRequest(request); ok {
			fm.pushRequest(ctx, state, entryRecords, first, err)
			fm.pushRequest(ctx, state, entryRecords, second, err)
			return
		}
	}

	state.requestErrors = append(state.requestErrors, err)
	dec := retryable.ClassifyDurableRequestOutcome(ctx, err, infraloki.ClassifyDurableRequestFailure)
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		state.cancellationErr = ctx.Err()
	}
	failedRecords := fm.buildFailedRecordsFromPushError(
		result.Meta.AttemptedAt,
		requestEntryRecords,
		dec,
		err,
	)
	if dec.Quarantine {
		result.QuarantineRecords = append(result.QuarantineRecords, failedRecords...)
	} else if dec.Retry {
		result.RetryableRecords = append(result.RetryableRecords, failedRecords...)
	} else {
		result.DeadRecords = append(result.DeadRecords, failedRecords...)
	}
}

// ErrClientNotInitializedは、送信に使うLoki clientが設定されていないことを表します。
var ErrClientNotInitialized = errors.New("Loki client is not initialized")

func (fm *LokiFlushManager) validateClient(ctx context.Context) error {
	if fm.client != nil {
		return nil
	}

	err := ErrClientNotInitialized
	fm.notifyError(
		ctx,
		&lokierrors.LokiLoggerInternalError{Op: "loki validate client", Err: err},
		fm.InternalErrorEntry("Loki client is not initialized", "", err),
		fm.Tracer,
		fm.Meter,
	)
	return err
}

type lokiLogEntryRecord struct {
	logEntry  *logging.LogEntry
	labels    map[string]string
	pushEntry pkgloki.Entry
}

func (fm *LokiFlushManager) convertLogEntriesToEntryRecords(entries []*logging.LogEntry) []lokiLogEntryRecord {
	if len(entries) == 0 {
		return nil
	}

	records := make([]lokiLogEntryRecord, 0, len(entries))
	for _, entry := range entries {
		records = append(records, fm.convertLogEntryToEntryRecord(entry))
	}
	return records
}

func (fm *LokiFlushManager) convertLogEntryToEntryRecord(entry *logging.LogEntry) lokiLogEntryRecord {
	formattedEntry := entry
	if entry != nil {
		entryCopy := *entry
		entryCopy.OccurredAt = logging.ResolveEntryOccurredAt(&entryCopy)
		formattedEntry = &entryCopy
	}
	return lokiLogEntryRecord{
		logEntry: formattedEntry,
		labels:   fm.labelsForLevel(formattedEntry.Level),
		pushEntry: pkgloki.Entry{
			Timestamp: entryTimestamp(formattedEntry),
			Line:      fm.LogFormatter.Format(formattedEntry),
		},
	}
}

func pushRecordsFromEntryRecords(records []lokiLogEntryRecord) []infraloki.PushRecord {
	pushRecords := make([]infraloki.PushRecord, 0, len(records))
	for inputIndex, record := range records {
		pushRecords = append(pushRecords, infraloki.PushRecord{
			InputIndex: inputIndex,
			Labels:     record.labels,
			Entry:      record.pushEntry,
		})
	}
	return pushRecords
}

func entryRecordsForPushRequest(
	records []lokiLogEntryRecord,
	request infraloki.PlannedPushRequest,
) []lokiLogEntryRecord {
	requestRecords := make([]lokiLogEntryRecord, 0, len(request.Records))
	for _, record := range request.Records {
		requestRecords = append(requestRecords, records[record.InputIndex])
	}
	return requestRecords
}

func (fm *LokiFlushManager) labelsForLevel(level pkglogger.LogLevel) map[string]string {
	appName := ""
	envName := ""
	if fm.LoggerBase != nil && fm.LoggerBase.config != nil && fm.LoggerBase.config.Runtime() != nil {
		appName = fm.LoggerBase.config.Runtime().AppName()
		envName = fm.LoggerBase.config.Runtime().Target()
	}
	if fm.resource != nil {
		if appName == "" {
			appName = fm.resource.ServiceName()
		}
		if fm.resource.Environment() != "" {
			envName = fm.resource.Environment()
		}
	}
	if appName == "" {
		appName = "unknown"
	}
	if envName == "" {
		envName = "unknown"
	}

	labels := map[string]string{
		"appName":  appName,
		"loglevel": strings.ToLower(level.String()),
		"env":      envName,
	}
	if fm.config != nil {
		for key, value := range fm.config.StaticLabels() {
			// appName、loglevel、envはloggerが生成するrouting identityであり、
			// 設定された追加labelから上書きさせません。
			if _, reserved := labels[key]; reserved {
				continue
			}
			labels[key] = value
		}
	}
	return labels
}

func entryTimestamp(entry *logging.LogEntry) time.Time {
	return logging.ResolveEntryOccurredAt(entry)
}

func (fm *LokiFlushManager) pushLimits() infraloki.PushLimits {
	if fm == nil || fm.LoggerBase == nil || fm.LoggerBase.config == nil {
		return infraloki.PushLimits{}
	}
	return infraloki.PushLimits{
		MaxLineBytes:        fm.LoggerBase.config.MaxLineBytes(),
		MaxPushRequestBytes: fm.LoggerBase.config.MaxPushRequestBytes(),
	}
}

func (fm *LokiFlushManager) handlePushDelayLog(ctx context.Context, elapsed time.Duration) {
	if fm == nil || fm.config == nil || !fm.config.PushLatencyEnabled() {
		return
	}

	switch {
	case fm.config.PostErrorThresholdDuration() > 0 && elapsed > fm.config.PostErrorThresholdDuration():
		err := fmt.Errorf("loki push delay exceeded error threshold: %s", elapsed)
		fm.notifyError(
			ctx,
			&lokierrors.LokiLoggerInternalError{Op: "loki push delay", Err: err},
			fm.InternalErrorEntry(fmt.Sprintf("loki push delay exceeded error threshold: %s", elapsed), "", nil),
			fm.Tracer,
			fm.Meter,
		)
	case fm.config.PostWarnThresholdDuration() > 0 && elapsed > fm.config.PostWarnThresholdDuration():
		fm.notifyWarn(
			ctx,
			fm.InternalWarnEntry(fmt.Sprintf("loki push delay exceeded warn threshold: %s", elapsed), ""),
			fm.Tracer,
			fm.Meter,
		)
	}
}

// handleFailureは1件即時送信の失敗後、durable処理の完了を同期的に待ちます。
func (fm *LokiFlushManager) handleFailure(
	ctx context.Context,
	res loggerfailure.SendResult,
) error {
	if fm.FailureHandler == nil {
		return flushfailure.ErrNoFailureHandler
	}
	return fm.FailureHandler.HandleFailure(ctx, res)
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

// handleFailureWithReportはDelivery用にdurable処理の完了と結果を同期的に返します。
func (fm *LokiFlushManager) handleFailureWithReport(
	ctx context.Context,
	res loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	if fm.FailureReportHandler == nil {
		return pkglogger.DeliveryReport{}, flushfailure.ErrNoFailureHandler
	}
	return fm.FailureReportHandler.HandleFailureWithReport(ctx, res)
}

func (fm *LokiFlushManager) newSendMeta(ctx context.Context, requestID string, inputCount int) loggerfailure.Meta {
	now := time.Now().UTC()
	if fm.Clock != nil {
		now = fm.Clock.Now()
	}

	meta := loggerfailure.Meta{
		Backend:     deliveryBackendLoki,
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

func (fm *LokiFlushManager) buildFailedRecordsFromPushError(
	attemptedAt time.Time,
	entryRecords []lokiLogEntryRecord,
	dec retryable.RetryDecision,
	err error,
) []loggerfailure.FailedRecord {
	// Lokiが再送可能な応答でRetry-Afterを返した場合は、spool再送もその時刻まで待たせます。
	retryAfter := loggerfailure.RetryAfterSince(attemptedAt, fm.Clock, dec)
	records := make([]loggerfailure.FailedRecord, 0, len(entryRecords))
	for _, entryRecord := range entryRecords {
		doc := fm.convertLogEntryRecordToDocument(entryRecord)
		records = append(records, loggerfailure.FailedRecord{
			// 一部のrequestが成功してTerminalErrorを設定しない場合も、退避処理の失敗時に
			// このrequestの元の送信errorを返せるよう、recordごとに原因を保持します。
			Cause:          err,
			DeliveryStatus: loggerfailure.RequestFailureDeliveryStatus(dec),
			FailedAt:       attemptedAt,
			Operation:      "loki_push",
			StatusCode:     dec.Code,
			FailureCode:    loggerfailure.FailureCode(dec.Reason),
			FailureReason:  err.Error(),
			Payload:        doc,
			RetryAfter:     retryAfter,
		})
	}
	return records
}

const (
	// FailureCodePushRecordTooLargeは、一件のentryだけでLokiのpush request上限を超えるため
	// 送信前に隔離したrecordのFailureCodeです。
	FailureCodePushRecordTooLarge loggerfailure.FailureCode = "loki_push_record_too_large"
	// FailureCodeLogLineTooLargeは、log lineがLokiのline上限を超えるため送信前に隔離した
	// recordのFailureCodeです。
	FailureCodeLogLineTooLarge loggerfailure.FailureCode = "loki_log_line_too_large"
)

func (fm *LokiFlushManager) buildLocallyRejectedRecord(
	record lokiLogEntryRecord,
	err error,
) loggerfailure.FailedRecord {
	failureCode := FailureCodePushRecordTooLarge
	if errors.Is(err, infraloki.ErrLogLineTooLarge) {
		failureCode = FailureCodeLogLineTooLarge
	}
	return loggerfailure.FailedRecord{
		Cause:          err,
		DeliveryStatus: pkglogger.DeliveryRejected,
		Operation:      "loki_push_validation",
		FailureCode:    failureCode,
		FailureReason:  err.Error(),
		Payload:        fm.convertLogEntryRecordToDocument(record),
	}
}

func (fm *LokiFlushManager) convertLogEntryRecordToDocument(record lokiLogEntryRecord) map[string]any {
	doc := map[string]any{
		"labels":    record.labels,
		"timestamp": record.pushEntry.Timestamp.Format(time.RFC3339Nano),
		"line":      record.pushEntry.Line,
	}

	entry := record.logEntry
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
