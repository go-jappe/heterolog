package opensearch

import (
	"context"
	stderrors "errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/duration"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/opaqueid"
	"github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/google/uuid"
)

// FlushManager はログエントリのフラッシュを管理するインターフェース
type FlushManager interface {
	Flush(ctx context.Context, entries []*logging.LogEntry) error // bulkでindex
	Index(ctx context.Context, entry *logging.LogEntry) error     // 1件index
	IndexWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error)
	FlushWithNoFallback(ctx context.Context, entries []*logging.LogEntry)
	Close(ctx context.Context) error
	Config() OpenSearchFlushManagerRuntimeConfigProvider
	LogEntryToDocumentConverter() LogEntryToDocumentConverter
	Resource() resource.ResourceProvider
}

// OpenSearchFlushManager はOpenSearchへのログエントリのフラッシュを管理する構造体
type OpenSearchFlushManager struct {
	*LoggerBase
	osClient opensearch.Client
	// configはbulk送信と1件送信で共有する送信先・遅延通知などの設定です。
	config OpenSearchFlushManagerRuntimeConfigProvider
	// bulkRetryConfigは構築時に検証済みのbulk項目の再送設定です。1件送信専用ではnilです。
	bulkRetryConfig      OpenSearchBulkRetryConfigProvider
	converter            LogEntryToDocumentConverter
	FailureHandler       *flushfailure.CompositeFailureHandler
	FailureReportHandler *flushfailure.ReportingCompositeFailureHandler
	resource             resource.ResourceProvider
	Tracer               pkgobserver.Tracer
	Meter                pkgobserver.Meter
}

// NewOpenSearchFlushManager は新しいOpenSearchFlushManagerを作成します。
func NewOpenSearchFlushManager(
	loggerBase *LoggerBase,
	osClient opensearch.Client,
	cfg OpenSearchBulkFlushManagerConfigProvider,
	converter LogEntryToDocumentConverter,
	failureHandler *flushfailure.CompositeFailureHandler,
	resource resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*OpenSearchFlushManager, error) {
	if err := validateOpenSearchBulkFlushManagerConfig(cfg); err != nil {
		return nil, fmt.Errorf("opensearch bulk flush manager config is invalid: %w", err)
	}
	manager := newOpenSearchFlushManager(
		loggerBase,
		osClient,
		cfg,
		converter,
		failureHandler,
		nil,
		resource,
		tracer,
		meter,
	)
	manager.bulkRetryConfig = cfg
	return manager, nil
}

const (
	// FailureCodeIndexRequestErrorは、一件のIndex requestの失敗を分類できなかったrecordの
	// FailureCodeです。
	FailureCodeIndexRequestError loggerfailure.FailureCode = "index_request_error"
	// FailureCodeRequestTooLargeは、一件分のBulk requestがapplication側のbyte上限を超えるため
	// 送信前に隔離したrecordのFailureCodeです。
	FailureCodeRequestTooLarge loggerfailure.FailureCode = "opensearch_request_too_large"
	// FailureCodeResponseInvalidは、Bulk応答から各documentの結果を確認できなかったrecordの
	// FailureCodeです。
	FailureCodeResponseInvalid loggerfailure.FailureCode = "opensearch_response_invalid"
)

var (
	// ErrNegativeMaxItemRetriesは、Bulk itemを再送する回数が負であることを表します。
	ErrNegativeMaxItemRetries = stderrors.New("max item retries must not be negative")
	// ErrNegativeRetryWaitTimeは、Bulk itemを再送するまでの待機時間が負であることを表します。
	ErrNegativeRetryWaitTime = stderrors.New("retry wait time must not be negative")
	// ErrClientNotInitializedは、送信に使うOpenSearch clientが設定されていないことを表します。
	ErrClientNotInitialized = stderrors.New("OpenSearch client is not initialized")
)

func validateOpenSearchBulkFlushManagerConfig(
	cfg OpenSearchBulkFlushManagerConfigProvider,
) error {
	if cfg == nil {
		return fmt.Errorf("OpenSearch bulk flush manager config is not initialized")
	}
	if maxItemRetries := cfg.MaxItemRetries(); maxItemRetries < 0 {
		return fmt.Errorf("%w: %d", ErrNegativeMaxItemRetries, maxItemRetries)
	}
	if retryWaitTime := cfg.RetryWaitTime(); retryWaitTime < 0 {
		return fmt.Errorf("%w: %s", ErrNegativeRetryWaitTime, retryWaitTime)
	}
	return nil
}

// newDeliveryOpenSearchFlushManager はreport対応failure handlerを、
// 通常の同期・非同期処理とDeliveryReport取得の両方へ配線します。
func newDeliveryOpenSearchFlushManager(
	loggerBase *LoggerBase,
	osClient opensearch.Client,
	cfg OpenSearchFlushManagerRuntimeConfigProvider,
	converter LogEntryToDocumentConverter,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
	resource resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *OpenSearchFlushManager {
	var baseHandler *flushfailure.CompositeFailureHandler
	if failureHandler != nil {
		baseHandler = failureHandler.CompositeFailureHandler
	}
	return newOpenSearchFlushManager(
		loggerBase,
		osClient,
		cfg,
		converter,
		baseHandler,
		failureHandler,
		resource,
		tracer,
		meter,
	)
}

func newOpenSearchFlushManager(
	loggerBase *LoggerBase,
	osClient opensearch.Client,
	cfg OpenSearchFlushManagerRuntimeConfigProvider,
	converter LogEntryToDocumentConverter,
	failureHandler *flushfailure.CompositeFailureHandler,
	failureReportHandler *flushfailure.ReportingCompositeFailureHandler,
	resource resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *OpenSearchFlushManager {
	return &OpenSearchFlushManager{
		LoggerBase:           loggerBase,
		osClient:             osClient,
		config:               cfg,
		converter:            converter,
		resource:             resource,
		Tracer:               tracer,
		Meter:                meter,
		FailureHandler:       failureHandler,
		FailureReportHandler: failureReportHandler,
	}
}

func (om *OpenSearchFlushManager) Config() OpenSearchFlushManagerRuntimeConfigProvider {
	return om.config
}

func (om *OpenSearchFlushManager) LogEntryToDocumentConverter() LogEntryToDocumentConverter {
	return om.converter
}

func (om *OpenSearchFlushManager) Resource() resource.ResourceProvider {
	return om.resource
}

// wrapErrorByLogger はエラーをロガーエラーでラップするヘルパー関数
func wrapErrorByLogger(err error, operation string) error {
	return &errors.OpenSearchLoggerInternalError{Op: operation, Err: err}
}

func (l *OpenSearchFlushManager) handleIndexDelayLog(
	ctx context.Context,
	level pkglogger.LogLevel,
	elapsed time.Duration,
) {
	opaqueID := ""
	if id, ok := opaqueid.OpaqueID(ctx); ok {
		opaqueID = id
	}

	var err error
	if level == pkglogger.LogLevelError {
		err = l.wrapErrorByLogger(nil, OperationIndexDelay)
	}

	switch level {
	case pkglogger.LogLevelError:
		message := fmt.Sprintf(ErrorMsgIndexDelayed, elapsed, l.config.PostErrorThresholdDuration(), opaqueID)
		l.notifyError(ctx, err, l.InternalErrorEntry(message, opaqueID, err), l.Tracer, l.Meter)
	case pkglogger.LogLevelWarn:
		message := fmt.Sprintf(MsgIndexDelayed, elapsed, l.config.PostWarnThresholdDuration(), opaqueID)
		l.notifyWarn(ctx, l.InternalWarnEntry(message, opaqueID), l.Tracer, l.Meter)
	case pkglogger.LogLevelInfo:
		message := fmt.Sprintf(MsgIndexNoDelayed, elapsed, opaqueID)
		l.notifyInfo(ctx, l.InternalInfoEntry(message, opaqueID), l.Tracer, l.Meter)
	case pkglogger.LogLevelDebug:
		message := fmt.Sprintf(MsgIndexNoDelayed, elapsed, opaqueID)
		l.notifyDebug(ctx, l.InternalDebugEntry(message, opaqueID), l.Tracer, l.Meter, l.config.IsDebug())
	default:
		message := fmt.Sprintf(MsgIndexNoDelayed, elapsed, opaqueID)
		l.notifyDebug(ctx, l.InternalDebugEntry(message, opaqueID), l.Tracer, l.Meter, l.config.IsDebug())
	}
}

// Flush はログエントリをOpenSearchに送信します。送信失敗をdurable処理できなかった場合は、
// failure handlerのerrorとbackendの送信失敗の原因を併せて返します。
func (fm *OpenSearchFlushManager) Flush(ctx context.Context, entries []*logging.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// OpenSearchクライアントが初期化されていない場合はエラー
	if err := fm.validateClient(ctx); err != nil {
		return err
	}

	// 一件だけの場合はIndex APIを使用します。一件ログの共通byte上限は
	// application層で検証し、MaxBulkRequestBytesはこのIndex経路に適用しません。
	if len(entries) == 1 {
		res, err := fm.indexLogEntry(ctx, entries[0])
		if res.HasFailedRecords() {
			return joinUnhandledSendFailure(err, res, fm.handleFailure(ctx, res))
		}
		return err
	}

	// ログエントリをOpenSearchのドキュメント形式に変換(converterの形式に依存)
	docs := fm.LogEntryToDocumentConverter().convertLogEntriesToDocuments(entries)

	// ドキュメントをOpenSearchに送信
	bulkSendResult, errBulkResult := fm.sendDocumentsToOpenSearch(ctx, docs)
	//登録失敗があれば保存
	if bulkSendResult.HasFailedRecords() {
		return joinUnhandledSendFailure(errBulkResult, bulkSendResult, fm.handleFailure(ctx, bulkSendResult))
	}

	return errBulkResult
}

// Indexはログエントリを1件即時送信します。送信失敗をdurable処理できなかった場合は、
// failure handlerのerrorとbackendの送信失敗の原因を併せて返します。
func (fm *OpenSearchFlushManager) Index(ctx context.Context, entry *logging.LogEntry) error {
	res, err := fm.indexLogEntry(ctx, entry)

	// 登録失敗データがあった場合の処理
	if res.HasFailedRecords() {
		return joinUnhandledSendFailure(err, res, fm.handleFailure(ctx, res))
	}

	return err
}

func (fm *OpenSearchFlushManager) IndexWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error) {
	res, err := fm.indexLogEntry(ctx, entry)
	if err != nil && !res.HasFailedRecords() {
		return pkglogger.NewTargetDeliveryReport(openSearchDeliveryRejectedResult(err)), err
	}

	if !res.HasSendFailure() {
		return pkglogger.NewTargetDeliveryReport(openSearchDeliverySucceededResult()), nil
	}

	target := openSearchTargetDeliveryResult(res)
	report := pkglogger.NewTargetDeliveryReport(target)
	failureReport, failureErr := fm.handleFailureWithReport(ctx, res)
	report.Durables = append(report.Durables, failureReport.Durables...)
	report.Evacuations = append(report.Evacuations, failureReport.Evacuations...)
	return report, stderrors.Join(err, target.Err, report.Error(), failureErr)
}

func (fm *OpenSearchFlushManager) indexLogEntry(ctx context.Context, entry *logging.LogEntry) (loggerfailure.SendResult, error) {
	// OpenSearchクライアントが初期化されていない場合はエラー
	if err := fm.validateClient(ctx); err != nil {
		return loggerfailure.SendResult{}, err
	}
	doc := fm.LogEntryToDocumentConverter().convertLogEntryToDocument(entry)

	// ドキュメント1件をOpenSearchに送信
	res, err := fm.sendDocumentToOpenSearch(ctx, doc)
	telemetry.RecordSendResult(ctx, fm.Meter, res)
	if err != nil {
		return res, fmt.Errorf("failed to send single log after retries: %w", err)
	}

	return res, nil
}

// handleFailureは1件即時送信の失敗後、durable処理の完了を同期的に待ちます。
func (fm *OpenSearchFlushManager) handleFailure(
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
	return stderrors.Join(err, sendFailureCause(err, res), failureErr)
}

// sendFailureCauseはSendResultが保持するbackendの送信失敗の原因を一つ返します。
// TerminalErrorを優先し、ない場合は元のerrorを保持する最初のrecordのCauseを使います。
// errが既に同じ原因を含む場合は、重複させないようnilを返します。
func sendFailureCause(err error, res loggerfailure.SendResult) error {
	cause := res.TerminalError
	if cause == nil {
		cause = firstFailedRecordCause(res)
	}
	if cause == nil || stderrors.Is(err, cause) {
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
func (fm *OpenSearchFlushManager) handleFailureWithReport(
	ctx context.Context,
	res loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	if fm.FailureReportHandler == nil {
		return pkglogger.DeliveryReport{}, flushfailure.ErrNoFailureHandler
	}

	return fm.FailureReportHandler.HandleFailureWithReport(ctx, res)
}

// FlushWithNoFallback は fallback logger 専用の batch 送信経路です。
// 1件でもBulk APIを使用し、item retry、failure handler、失敗batchの再投入は行いません。
// client wrapperに設定されたtransport retryだけはclient側の契約として維持されます。
func (fm *OpenSearchFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	if len(entries) == 0 {
		return
	}
	// validateClientはOnErrorを呼び得るため、no-fallback経路では直接検証します。
	if fm == nil || fm.osClient == nil {
		log.Printf("failed to flush OpenSearch fallback logger: OpenSearch client is not initialized")
		return
	}
	if err := ctx.Err(); err != nil {
		log.Printf("failed to flush OpenSearch fallback logger: %v", err)
		return
	}

	documents := fm.LogEntryToDocumentConverter().convertLogEntriesToDocuments(entries)
	requests := fm.convertDocumentsToCreateRequests(documents)
	records := make([]infraopensearch.BulkRecord, 0, len(requests))
	for inputIndex, request := range requests {
		records = append(records, infraopensearch.BulkRecord{
			InputIndex: inputIndex,
			Document:   request,
		})
	}
	plan, err := infraopensearch.PlanBulkRequests(records, fm.maxBulkRequestBytes())
	if err != nil {
		log.Printf("failed to plan OpenSearch fallback bulk request: %v", err)
		return
	}
	if len(plan.Rejected) > 0 {
		first := plan.Rejected[0]
		log.Printf(
			"OpenSearch fallback bulk plan dropped %d log entries; first rejection at input index %d: %v",
			len(plan.Rejected),
			first.Record.InputIndex,
			first.Err,
		)
	}

	for _, request := range plan.Requests {
		if err := ctx.Err(); err != nil {
			log.Printf("failed to flush OpenSearch fallback logger: %v", err)
			return
		}
		requestDocuments := request.Documents()
		response, err := fm.osClient.IndexDocuments(ctx, requestDocuments)
		if err != nil {
			log.Printf("failed to flush OpenSearch fallback logger: %v", err)
			continue
		}
		classification, err := infraopensearch.ClassifyFailedBulkDocs(response, requestDocuments)
		if err != nil {
			log.Printf("failed to classify OpenSearch fallback bulk response: %v", err)
			continue
		}
		if failed := classification.RetryableCount + classification.DeadFailedCount + classification.QuarantineFailedCount; failed > 0 {
			log.Printf("OpenSearch fallback bulk request dropped %d failed log entries", failed)
		}
	}
}

// indexInternalLogDirectlyは、On*の中から再入した送信で発生した内部ログ一件を、遅延や失敗を
// 通知せず、failure handlerにも渡さずにIndexDocumentで同じindexへ書き込みます。失敗は
// fallback loggerの送信と同じく標準loggerへ残します。この書き込みは内部ログを通知しないため、
// 再入はここで終わります。
func (fm *OpenSearchFlushManager) indexInternalLogDirectly(ctx context.Context, entry *logging.LogEntry) {
	if fm == nil || fm.osClient == nil {
		log.Printf("failed to write OpenSearch internal log directly: %v", ErrClientNotInitialized)
		return
	}
	if err := ctx.Err(); err != nil {
		log.Printf("failed to write OpenSearch internal log directly: %v", err)
		return
	}
	request := fm.convertDocumentToCreateRequest(fm.LogEntryToDocumentConverter().convertLogEntryToDocument(entry))
	response, err := fm.osClient.IndexDocument(ctx, request)
	if err == nil && response == nil {
		err = errOpenSearchNilIndexResponse
	}
	if err == nil {
		if validationErr := response.Validate(request); validationErr != nil {
			err = &opensearch.ResponseDecodeError{Operation: "index", Err: validationErr}
		}
	}
	if err != nil {
		log.Printf("failed to write OpenSearch internal log directly: %v", err)
	}
}

func (fm *OpenSearchFlushManager) validateClient(ctx context.Context) error {
	if fm.osClient == nil {
		err := ErrClientNotInitialized
		fm.notifyError(
			ctx,
			wrapErrorByLogger(err, "opensearch error"),
			fm.InternalErrorEntry("out of control bulk response error", "", err),
			fm.Tracer,
			fm.Meter,
		)
		return err
	}
	return nil
}

func (fm *OpenSearchFlushManager) convertDocumentToCreateRequest(doc map[string]any) opensearch.DocumentWriteRequest {
	return opensearch.DocumentWriteRequest{
		Index: fm.Config().Index(),
		ID:    uuid.NewString(),
		Doc:   doc,
		Op:    opensearch.WriteOpCreate,
	}
}

// documentを1件送信します
func (fm *OpenSearchFlushManager) sendDocumentToOpenSearch(
	ctx context.Context,
	doc map[string]any,
) (loggerfailure.SendResult, error) {
	var res loggerfailure.SendResult

	requestDocument := fm.convertDocumentToCreateRequest(doc)

	requestID, _ := requestctx.RequestID(ctx)
	ctx = opaqueid.WithOpaqueID(ctx, requestID)

	res.Meta = fm.newSendMeta(ctx, requestID, 1)
	attemptedAt := res.Meta.AttemptedAt
	if err := ctx.Err(); err != nil {
		res.Meta.AttemptedAt = time.Time{}
		res.TerminalError = err
		res.RetryableRecords = fm.unstartedDocumentRecords(attemptedAt, []opensearch.DocumentWriteRequest{requestDocument}, err)
		return res, err
	}

	requestCtx, request := telemetry.StartRequest(ctx, fm.Tracer, fm.Meter, SpanOpenSearchIndex, deliveryBackendOpenSearch, 1)
	indexRes, err := duration.MeasureAndLogDuration[*opensearch.IndexResponse](
		func() (*opensearch.IndexResponse, error) {
			return fm.osClient.IndexDocument(requestCtx, requestDocument)
		},
		func(level pkglogger.LogLevel, elapsed time.Duration, opaqueID any) {
			if level <= pkglogger.LogLevelInfo && !fm.config.IndexLatencyEnabled() {
				return
			}
			fm.handleIndexDelayLog(ctx, level, elapsed)
		},
		// このlabelはerror文言としてdurable／quarantine recordのFailureReasonへ残ります。
		// 単件Indexは通常・Deliveryの送信だけが使い、fallback loggerは使わないため、
		// 運用者が経路を誤認しないよう実際の操作を表す語にします。
		"index document to OpenSearch",
		nil,
		fm.config.PostErrorThresholdDuration(),
		fm.config.PostWarnThresholdDuration(),
	)

	// 単件createもBulk itemと同じbackend規則で409を成功扱いにする。
	// 固定IDの同じrequestを再送して409になった場合、以前の試行で目的の状態へ到達済みです。
	if infraopensearch.IsIdempotentCreateConflict(requestDocument, err) {
		request.End(nil)
		return res, nil
	}
	// custom clientでも、HTTP clientと同じrequestとresponseの対応関係が成り立って
	// 初めて、このrecordを受理済みにできます。
	if err == nil && indexRes != nil {
		if validationErr := indexRes.Validate(requestDocument); validationErr != nil {
			err = &opensearch.ResponseDecodeError{Operation: "index", Err: validationErr}
		}
	}
	if err == nil && indexRes == nil {
		request.End(errOpenSearchNilIndexResponse)
	} else {
		request.End(err)
	}

	if err != nil {
		dec := classifyOpenSearchRequestFailure(ctx, err)
		res.TerminalError = err
		failureCode := loggerfailure.FailureCode(dec.Reason)
		if failureCode == "" {
			failureCode = FailureCodeIndexRequestError
		}

		failedDoc := fm.newFailedDocumentRecord(
			attemptedAt,
			requestDocument,
			"sendDocumentToOpenSearch",
			dec.Code,
			failureCode,
			err.Error(),
		)
		// 再送可能な応答のRetry-Afterは、spool再送の開始時刻にも引き継ぐ。
		failedDoc.RetryAfter = loggerfailure.RetryAfterSince(attemptedAt, fm.Clock, dec)
		failedDoc.Cause = err

		var hsErr *opensearch.HTTPStatusError
		if stderrors.As(err, &hsErr) {
			failedDoc.StatusCode = hsErr.Code
			if oe := hsErr.OpenSearchError(); oe != nil {
				failedDoc.FailureCode = loggerfailure.FailureCode(oe.Type)
				failedDoc.FailureReason = oe.Reason
			} else {
				failedDoc.FailureReason = err.Error()
			}
		} else {
			failedDoc.FailureReason = err.Error()
		}

		switch {
		case dec.Quarantine:
			res.QuarantineRecords = append(res.QuarantineRecords, failedDoc)
		case dec.Retry:
			res.RetryableRecords = append(res.RetryableRecords, failedDoc)
		case infraopensearch.IsRequestEntityTooLarge(err),
			infraopensearch.IsRecordPayloadErrorType(opensearch.ErrorType(failedDoc.FailureCode)):
			res.QuarantineRecords = append(res.QuarantineRecords, failedDoc)
		default:
			res.DeadRecords = append(res.DeadRecords, failedDoc)
		}
		return res, ctx.Err()
	}

	if indexRes == nil {
		res.TerminalError = errOpenSearchNilIndexResponse
		nilResponse := fm.newFailedDocumentRecord(
			attemptedAt,
			requestDocument,
			"sendDocumentToOpenSearch",
			0,
			"nil_response",
			errOpenSearchNilIndexResponse.Error(),
		)
		nilResponse.Cause = errOpenSearchNilIndexResponse
		res.QuarantineRecords = append(res.QuarantineRecords, nilResponse)
		return res, nil
	}

	return res, nil
}

// errOpenSearchNilIndexResponseは、clientがerrorなしでnilのIndexResponseを返し、
// 書込み結果を確認できなかったことを表します。
var errOpenSearchNilIndexResponse = stderrors.New("IndexDocument returned nil response without error")

// bulkItemRejectionErrorは、OpenSearchがBulk応答のitemで一件のdocumentを拒否したことを
// 表します。FailedRecord.Causeへ保持し、配送結果や退避処理の失敗時に返すerrorから
// errors.Asで拒否の種類を判別できるようにします。
type bulkItemRejectionError struct {
	// Statusは、itemに返されたHTTP statusです。
	Status int
	// Typeは、itemに返されたOpenSearchのerror typeです。
	Type opensearch.ErrorType
	// Reasonは、itemに返された拒否理由です。
	Reason string
}

func (e *bulkItemRejectionError) Error() string {
	return fmt.Sprintf("OpenSearch bulk item rejected with status %d (%s): %s", e.Status, e.Type, e.Reason)
}

// Closeは呼び出し元の終了期限に従い、Drain開始前に受理済みのfailure handlingが
// 完了するまで待ちます。
func (fm *OpenSearchFlushManager) Close(ctx context.Context) error {
	if fm == nil || fm.FailureHandler == nil {
		return nil
	}
	return fm.FailureHandler.Drain(ctx)
}

// sendDocumentsToOpenSearch はドキュメントをOpenSearchにバルク送信します。
// transport/HTTP レベルの再送は retryable client 側の責務とし、ここではBulkResponseが
// 返った後のitem単位再送と、HTTP 413時のrequest分割だけを行います。
func (fm *OpenSearchFlushManager) sendDocumentsToOpenSearch(
	ctx context.Context,
	docs []map[string]any,
) (loggerfailure.SendResult, error) {
	ctx, requestID := attachOpaqueRequestID(ctx)

	// retryRequests は、この flush で実際に OpenSearch へ送信する DocumentWriteRequest。
	//
	// spool 再送では LogEntry から DocumentWriteRequest を作り直してはいけない。
	// create op の場合、ID を再生成すると同一 document の再送ではなくなるため、
	// 失敗時はこの request 情報を durable payload として保持する。
	retryRequests := fm.convertDocumentsToCreateRequests(docs)

	// MaxItemRetriesはBulkResponse内でretryableと判定されたitemを初回に加えて再送する回数で、
	// 送信の試行回数はそれに初回を足した数です。RetryWaitTimeはitem再送の間隔であり、transport/HTTP levelの
	// retry待機はretryable clientが別途担当します。
	if fm.bulkRetryConfig == nil {
		return loggerfailure.SendResult{}, fmt.Errorf("OpenSearch bulk flush manager config is not initialized")
	}
	maxAttempts := fm.bulkRetryConfig.MaxItemRetries() + 1
	retryWaitTime := fm.bulkRetryConfig.RetryWaitTime()

	meta := fm.newSendMeta(ctx, requestID, len(retryRequests))
	result := loggerfailure.SendResult{Meta: meta}
	records := make([]infraopensearch.BulkRecord, 0, len(retryRequests))
	for inputIndex, request := range retryRequests {
		records = append(records, infraopensearch.BulkRecord{
			InputIndex: inputIndex,
			Document:   request,
		})
	}
	plan, err := infraopensearch.PlanBulkRequests(records, fm.maxBulkRequestBytes())
	if err != nil {
		return result, fmt.Errorf("plan OpenSearch bulk requests: %w", err)
	}
	for _, rejected := range plan.Rejected {
		result.QuarantineRecords = append(
			result.QuarantineRecords,
			fm.buildLocallyRejectedBulkRecord(rejected),
		)
	}

	// 設定byte上限内に分けたrequestを順に送信します。OpenSearchまたは
	// proxyの実上限が設定より小さい場合に備え、各request内の413二分再送は維持します。
	var sendErrors []error
	for _, request := range plan.Requests {
		requestResult, err := fm.sendDocumentRequestsToOpenSearch(
			ctx,
			meta,
			request.Documents(),
			maxAttempts,
			retryWaitTime,
		)
		mergeSplitBulkSendResult(&result, requestResult)
		if err != nil {
			sendErrors = append(sendErrors, err)
		}
	}
	// 計画したすべての入力は、この時点で確定した成功か失敗recordのどちらかを持ちます。
	// 後のrequestのerrorによって、先に確認応答を受けた成功を消してはいけません。
	if result.FailedCount() < result.Meta.InputCount {
		result.TerminalError = nil
	}
	telemetry.RecordSendResult(ctx, fm.Meter, result)
	return result, stderrors.Join(sendErrors...)
}

func (fm *OpenSearchFlushManager) maxBulkRequestBytes() int {
	if fm == nil || fm.LoggerBase == nil || fm.LoggerBase.config == nil {
		return 0
	}
	return fm.LoggerBase.config.MaxBulkRequestBytes()
}

func (fm *OpenSearchFlushManager) buildLocallyRejectedBulkRecord(
	rejected infraopensearch.RejectedBulkRecord,
) loggerfailure.FailedRecord {
	failureCode := FailureCodeRequestTooLarge
	record := fm.newFailedDocumentRecord(
		time.Time{},
		rejected.Record.Document,
		"opensearch_bulk_validation",
		0,
		failureCode,
		rejected.Err.Error(),
	)
	record.Cause = rejected.Err
	record.DeliveryStatus = pkglogger.DeliveryRejected
	return record
}

// sendDocumentRequestsToOpenSearchは固定ID付きrequestを送信します。HTTP 413で複数件が
// 拒否された場合は二分して再送し、単一documentでも413になる場合だけquarantineへ確定します。
func (fm *OpenSearchFlushManager) sendDocumentRequestsToOpenSearch(
	ctx context.Context,
	meta loggerfailure.Meta,
	retryRequests []opensearch.DocumentWriteRequest,
	maxAttempts int,
	retryWaitTime time.Duration,
) (loggerfailure.SendResult, error) {
	result := loggerfailure.SendResult{Meta: meta}
	if len(retryRequests) == 0 {
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		result.TerminalError = err
		result.RetryableRecords = fm.unstartedDocumentRecords(meta.AttemptedAt, retryRequests, err)
		return result, err
	}

	var lastRetryFailures []infraopensearch.BulkFailedRequest

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			if err := waitForBulkItemRetry(ctx, retryWaitTime); err != nil {
				// これらのrecordは、直前のresponseで明示的に拒否されています。
				// 次の試行が止められた場合も、判明しているstatusとIDを保持します。
				result.RetryableRecords = fm.buildFailedRecordsFromBulkFailures(
					result.Meta.AttemptedAt, "sendDocumentsToOpenSearch", lastRetryFailures,
				)
				return result, err
			}
		}

		bulkResp, err := fm.indexBulkDocuments(ctx, retryRequests)
		if err != nil {
			// HTTP 413やdocument内容の不正でrequest全体が拒否された場合は、どのdocumentも
			// 処理されていません。複数documentなら半分ずつ再送して原因を隔離し、
			// 分割できない一件だけをquarantineへ確定します。
			if infraopensearch.IsRecordScopedRequestRejection(err) {
				if len(retryRequests) == 1 {
					failedRecords, _ := fm.buildFailedRecordsFromBulkSendError(
						ctx,
						result.Meta.AttemptedAt,
						retryRequests,
						err,
					)
					result.QuarantineRecords = append(result.QuarantineRecords, failedRecords...)
					return result, nil
				}

				middle := len(retryRequests) / 2
				left, leftErr := fm.sendDocumentRequestsToOpenSearch(
					ctx,
					meta,
					retryRequests[:middle],
					maxAttempts,
					retryWaitTime,
				)
				right, rightErr := fm.sendDocumentRequestsToOpenSearch(
					ctx,
					meta,
					retryRequests[middle:],
					maxAttempts,
					retryWaitTime,
				)
				// 送信を始めていない分割requestにも、元のrequestが拒否されたという確定した事実が
				// 残っています。小さくしたrequestの再送を許しつつ、その証拠を保持します。
				// cancellationによって結果不明にはなりません。
				rejectedStatus := http.StatusRequestEntityTooLarge
				var rejectedStatusErr *opensearch.HTTPStatusError
				if stderrors.As(err, &rejectedStatusErr) {
					rejectedStatus = rejectedStatusErr.StatusCode()
				}
				for _, part := range []*loggerfailure.SendResult{&left, &right} {
					for i := range part.RetryableRecords {
						record := &part.RetryableRecords[i]
						if record.FailureCode == loggerfailure.FailureCode(retryable.RetryReasonRequestNotSent) {
							record.Cause = stderrors.Join(err, record.Cause)
							record.StatusCode = rejectedStatus
							record.DeliveryStatus = pkglogger.DeliveryFailed
							record.FailureReason = fmt.Sprintf(
								"bulk request rejected with HTTP %d; split request not sent: %s",
								rejectedStatus,
								record.FailureReason,
							)
						}
					}
				}
				mergeSplitBulkSendResult(&result, left)
				mergeSplitBulkSendResult(&result, right)
				return result, stderrors.Join(leftErr, rightErr)
			}

			// BulkResponse が取れない失敗は retryable client の責務。
			// flush manager では再送せず、退避先を決められるように失敗 record 化して返す。
			result.TerminalError = err
			failedRecords, decision := fm.buildFailedRecordsFromBulkSendError(
				ctx,
				result.Meta.AttemptedAt,
				retryRequests,
				err,
			)
			if decision.Quarantine {
				result.QuarantineRecords = append(result.QuarantineRecords, failedRecords...)
			} else if decision.Retry {
				result.RetryableRecords = failedRecords
			} else {
				result.DeadRecords = append(result.DeadRecords, failedRecords...)
			}
			return result, ctx.Err()
		}

		// BulkResponse が返った場合だけ、item 単位の成功/永久失敗/retryable 失敗を分類する。
		classification, err := infraopensearch.ClassifyFailedBulkDocs(bulkResp, retryRequests)
		if err != nil {
			result.TerminalError = fmt.Errorf("out of control bulk response error: %w", err)
			for _, request := range retryRequests {
				record := fm.newFailedDocumentRecord(
					result.Meta.AttemptedAt, request, "sendDocumentsToOpenSearch", 0,
					FailureCodeResponseInvalid, result.TerminalError.Error(),
				)
				record.Cause = result.TerminalError
				result.QuarantineRecords = append(result.QuarantineRecords, record)
			}
			return result, nil
		}

		// backend terminal failureはrecord自体の修復対象ではないためdeadへ残す。
		result.DeadRecords = append(
			result.DeadRecords,
			fm.buildFailedRecordsFromBulkFailures(
				result.Meta.AttemptedAt,
				"sendDocumentsToOpenSearch",
				classification.DeadFailures,
			)...,
		)
		// record固有のpayload不正が確定したitemだけをquarantineへ残す。
		result.QuarantineRecords = append(
			result.QuarantineRecords,
			fm.buildFailedRecordsFromBulkFailures(
				result.Meta.AttemptedAt,
				"sendDocumentsToOpenSearch",
				classification.QuarantineFailures,
			)...,
		)
		if len(classification.RetryableFailures) == 0 {
			result.RetryableRecords = nil
			return result, nil
		}

		// 次の attempt では retryable item だけを再送する。
		lastRetryFailures = classification.RetryableFailures
		retryRequests = bulkFailedRequestsToRequests(classification.RetryableFailures)
	}

	if len(lastRetryFailures) > 0 {
		// item 単位 retry を使い切った残りは retryable として退避対象にする。
		result.RetryableRecords = fm.buildFailedRecordsFromBulkFailures(
			result.Meta.AttemptedAt,
			"sendDocumentsToOpenSearch",
			lastRetryFailures,
		)
		return result, nil
	}

	return result, nil
}

// waitForBulkItemRetryはretryable itemの次回送信まで待ちます。呼び出し元のdeadlineや
// cancelを待機より優先し、非正値の待機時間ではtimerを作成しません。
func waitForBulkItemRetry(ctx context.Context, retryWaitTime time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if retryWaitTime <= 0 {
		return nil
	}

	timer := time.NewTimer(retryWaitTime)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// mergeSplitBulkSendResultは、413分割後の各request結果を元のflush結果へ統合します。
func mergeSplitBulkSendResult(
	destination *loggerfailure.SendResult,
	source loggerfailure.SendResult,
) {
	destination.TerminalError = stderrors.Join(destination.TerminalError, source.TerminalError)
	destination.RetryableRecords = append(destination.RetryableRecords, source.RetryableRecords...)
	destination.DeadRecords = append(destination.DeadRecords, source.DeadRecords...)
	destination.QuarantineRecords = append(destination.QuarantineRecords, source.QuarantineRecords...)
}

// buildFailedRecordsFromBulkSendError は、BulkResponse を得られなかった送信失敗を
// failure handler が扱える loggerfailure.FailedRecord に変換する。
func (fm *OpenSearchFlushManager) buildFailedRecordsFromBulkSendError(
	ctx context.Context,
	failedAt time.Time,
	reqs []opensearch.DocumentWriteRequest,
	err error,
) ([]loggerfailure.FailedRecord, retryable.RetryDecision) {
	dec := classifyOpenSearchRequestFailure(ctx, err)
	status, failureCode, reason := bulkSendErrorDetails(err, dec)
	// 再送可能な応答のRetry-Afterは、spool再送の開始時刻にも引き継ぐ。
	retryAfter := loggerfailure.RetryAfterSince(failedAt, fm.Clock, dec)

	records := make([]loggerfailure.FailedRecord, 0, len(reqs))
	for _, req := range reqs {
		record := fm.newFailedDocumentRecord(
			failedAt, req, "sendDocumentsToOpenSearch", status, failureCode, reason,
		)
		// 他のrequestが成功してTerminalErrorを設定しない場合も、退避処理の失敗時に
		// このrequestの元の送信errorを返せるよう、recordごとに原因を保持します。
		record.Cause = err
		record.DeliveryStatus = loggerfailure.RequestFailureDeliveryStatus(dec)
		record.RetryAfter = retryAfter
		records = append(records, record)
	}

	return records, dec
}

// classifyOpenSearchRequestFailureは、OpenSearchへのrequestの失敗を再試行の判断へ
// 分類します。client呼び出し中に呼び出し元がcancelすると、受理されたかどうかは不確かです。
// clientだけのtimeoutは既存のretry方針を保ち、backendの明示的なresponseは、
// 呼び出し元がその後停止していてもresponse自身の分類を保ちます。
func classifyOpenSearchRequestFailure(ctx context.Context, err error) retryable.RetryDecision {
	return retryable.ClassifyDurableRequestOutcome(ctx, err, infraopensearch.ClassifyDurableRequestFailure)
}

func (fm *OpenSearchFlushManager) unstartedDocumentRecords(
	failedAt time.Time,
	requests []opensearch.DocumentWriteRequest,
	cause error,
) []loggerfailure.FailedRecord {
	records := make([]loggerfailure.FailedRecord, 0, len(requests))
	for _, request := range requests {
		record := fm.newFailedDocumentRecord(
			failedAt, request, "sendDocumentsToOpenSearch", 0,
			loggerfailure.FailureCode(retryable.RetryReasonRequestNotSent), cause.Error(),
		)
		record.Cause = cause
		records = append(records, record)
	}
	return records
}

// bulkSendErrorDetails はtransport/HTTP levelのerrorから保存用のstatus、failure code、
// reasonを取り出します。OpenSearchのHTTP error本文が読める場合は、OpenSearch側の
// error typeをfailure codeとして優先します。
func bulkSendErrorDetails(
	err error,
	dec retryable.RetryDecision,
) (int, loggerfailure.FailureCode, string) {
	status := dec.Code
	failureCode := loggerfailure.FailureCode(dec.Reason)
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	if failureCode == "" {
		failureCode = "bulk_request_error"
	}

	var hsErr *opensearch.HTTPStatusError
	if stderrors.As(err, &hsErr) {
		status = hsErr.Code
		if oe := hsErr.OpenSearchError(); oe != nil {
			if oe.Type != "" {
				failureCode = loggerfailure.FailureCode(oe.Type)
			}
			if oe.Reason != "" {
				reason = oe.Reason
			}
		}
	}

	return status, failureCode, reason
}

func (fm *OpenSearchFlushManager) convertDocumentsToCreateRequests(
	docs []map[string]any,
) []opensearch.DocumentWriteRequest {
	if len(docs) == 0 {
		return nil
	}

	reqs := make([]opensearch.DocumentWriteRequest, 0, len(docs))
	for _, d := range docs {
		reqs = append(reqs, fm.convertDocumentToCreateRequest(d))
	}
	return reqs
}

func (fm *OpenSearchFlushManager) newSendMeta(
	ctx context.Context,
	requestID string,
	inputCount int,
) loggerfailure.Meta {
	now := fm.Clock.Now()

	return loggerfailure.Meta{
		Backend:         deliveryBackendOpenSearch,
		RequestID:       requestID,
		AppName:         fm.Resource().ServiceName(),
		HostName:        fm.Resource().HostName(),
		Environment:     fm.Resource().Environment(),
		RuntimeType:     fm.Resource().RuntimeType(),
		ExecutionUnitID: fm.Resource().RuntimeInstanceID(),
		Version:         fm.Resource().ServiceVersion(),
		TraceID:         string(observer.TraceIDFromContext(ctx)),
		AttemptedAt:     now,
		InputCount:      inputCount,
	}
}

func bulkFailedRequestsToRequests(
	items []infraopensearch.BulkFailedRequest,
) []opensearch.DocumentWriteRequest {
	if len(items) == 0 {
		return nil
	}

	reqs := make([]opensearch.DocumentWriteRequest, 0, len(items))
	for _, item := range items {
		reqs = append(reqs, item.Request)
	}
	return reqs
}

func (fm *OpenSearchFlushManager) newFailedDocumentRecord(
	failedAt time.Time,
	req opensearch.DocumentWriteRequest,
	operation string,
	status int,
	failureCode loggerfailure.FailureCode,
	reason string,
) loggerfailure.FailedRecord {
	return loggerfailure.FailedRecord{
		DeliveryStatus: loggerfailure.RequestFailureDeliveryStatus(retryable.RetryDecision{Code: status, Reason: retryable.RetryReason(failureCode)}),
		FailedAt:       failedAt,
		Operation:      operation,
		StatusCode:     status,
		FailureCode:    failureCode,
		FailureReason:  reason,
		Payload: map[string]any{
			"index": req.Index,
			"id":    req.ID,
			"doc":   req.Doc,
			"op":    string(req.Op),
		},
	}
}

func (fm *OpenSearchFlushManager) buildFailedRecordsFromBulkFailures(
	failedAt time.Time,
	operation string,
	items []infraopensearch.BulkFailedRequest,
) []loggerfailure.FailedRecord {
	if len(items) == 0 {
		return nil
	}

	records := make([]loggerfailure.FailedRecord, 0, len(items))
	for _, item := range items {
		record := fm.newFailedDocumentRecord(
			failedAt,
			item.Request,
			operation,
			item.Status,
			loggerfailure.FailureCode(item.ErrorType),
			item.ErrorReason,
		)
		// 他のdocumentが受理されてTerminalErrorを設定しない場合も、退避処理の失敗時に
		// itemの拒否理由を返せるよう、recordごとに原因を保持します。
		record.Cause = &bulkItemRejectionError{Status: item.Status, Type: item.ErrorType, Reason: item.ErrorReason}
		records = append(records, record)
	}
	return records
}

func attachOpaqueRequestID(ctx context.Context) (context.Context, string) {
	requestID, _ := requestctx.RequestID(ctx)
	ctx = opaqueid.WithOpaqueID(ctx, requestID)
	return ctx, requestID
}

func (fm *OpenSearchFlushManager) indexBulkDocuments(
	ctx context.Context,
	requestDocument []opensearch.DocumentWriteRequest,
) (*opensearch.BulkResponse, error) {
	requestCtx, request := telemetry.StartRequest(ctx, fm.Tracer, fm.Meter, SpanOpenSearchBulk, deliveryBackendOpenSearch, len(requestDocument))
	resp, err := duration.MeasureAndIndexDocumentsDuration(
		func() (*opensearch.BulkResponse, error) {
			return fm.osClient.IndexDocuments(requestCtx, requestDocument)
		},
		func(level pkglogger.LogLevel, elapsed time.Duration, opaqueID any) {
			if level <= pkglogger.LogLevelInfo && !fm.config.IndexLatencyEnabled() {
				return
			}
			fm.handleIndexDelayLog(ctx, level, elapsed)
		},
		"bulk send to OpenSearch",
		nil,
		fm.config.PostErrorThresholdDuration(),
		fm.config.PostWarnThresholdDuration(),
	)
	request.End(err)
	return resp, err
}
