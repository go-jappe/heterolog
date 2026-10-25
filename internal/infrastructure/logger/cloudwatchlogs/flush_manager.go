package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	cloudwatchlogserrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs/errors"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// FlushManager は LogEntry を CloudWatch Logs へ送信し、失敗時の durable fallback へ橋渡しします。
//
// FlushWithNoFallback は fallback logger 専用で、失敗をさらに failure handler へ戻さないために分けています。
type FlushManager interface {
	// Flushはbulk loggerが切り離した一回限りのbuffer snapshotを受け取ります。実log streamの
	// 解決とAWS制限内のphysical PutLogEvents requestへの分割は、この境界より下で行います。
	// errorはbackend送信またはfailure処理を完了できなかったことを表しますが、呼び出し元は
	// snapshotを通常bufferへ戻しません。
	Flush(ctx context.Context, entries []*logging.LogEntry) error

	// Put は immediate logger 用に 1 件の LogEntry を送信します。
	// 送信後に失敗 record が残った場合は、同期 failure handler の完了を待ちます。
	Put(ctx context.Context, entry *logging.LogEntry) error

	// PutWithReport はdelivery logger用に1件を送信し、
	// targetと失敗後のdurableまたはevacuationの結果を返します。
	PutWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error)

	// FlushWithNoFallback は fallback logger 専用に一回のbatch snapshotを送信します。
	// fallback 失敗をさらに failure handler へ戻さないため、この経路は再退避を行いません。
	FlushWithNoFallback(ctx context.Context, entries []*logging.LogEntry)

	// Closeは呼び出し元の終了期限に従い、すでに受理したfailure handlingが
	// 完了するまで待ちます。clientなど、呼び出し元が所有するresourceは閉じません。
	Close(ctx context.Context) error
}

// CloudWatchLogsFlushManagerは、一回のbuffer snapshotに対するrequest計画、physical
// PutLogEventsの逐次送信、失敗record化とdurable処理を調整します。AWS固有のstream分類と
// request分割はlogbackendのplannerへ委譲し、送信結果はbackend共通のSendResultへ変換します。
type CloudWatchLogsFlushManager struct {
	*LoggerBase
	client               pkgcloudwatchlogs.Client
	streamNameResolver   *infracloudwatchlogs.LogStreamNameResolver
	config               CloudWatchLogsFlushManagerRuntimeConfigProvider
	FailureHandler       *flushfailure.CompositeFailureHandler
	FailureReportHandler *flushfailure.ReportingCompositeFailureHandler
	resource             resource.ResourceProvider
	Tracer               observer.Tracer
	Meter                observer.Meter
}

// NewCloudWatchLogsFlushManagerはCloudWatch Logs送信用FlushManagerを生成します。
// loggerBaseはCloudWatch Logs loggerのconstructorでLogFormatterまで初期化済みでなければなりません。
// failureHandlerが未設定の場合も正常な送信はできますが、送信失敗を処理する経路がないため
// FlushまたはPutはErrNoFailureHandlerを返します。
func NewCloudWatchLogsFlushManager(
	loggerBase *LoggerBase,
	client pkgcloudwatchlogs.Client,
	streamNameResolver *infracloudwatchlogs.LogStreamNameResolver,
	cfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	resourceProvider resource.ResourceProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) *CloudWatchLogsFlushManager {
	return newCloudWatchLogsFlushManager(
		loggerBase,
		client,
		streamNameResolver,
		cfg,
		failureHandler,
		nil,
		resourceProvider,
		tracer,
		meter,
	)
}

func newCloudWatchLogsFlushManager(
	loggerBase *LoggerBase,
	client pkgcloudwatchlogs.Client,
	streamNameResolver *infracloudwatchlogs.LogStreamNameResolver,
	cfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	failureReportHandler *flushfailure.ReportingCompositeFailureHandler,
	resourceProvider resource.ResourceProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) *CloudWatchLogsFlushManager {
	return &CloudWatchLogsFlushManager{
		LoggerBase:           loggerBase,
		client:               client,
		streamNameResolver:   streamNameResolver,
		config:               cfg,
		FailureHandler:       failureHandler,
		FailureReportHandler: failureReportHandler,
		resource:             resourceProvider,
		Tracer:               observer.EnsureTracer(tracer),
		Meter:                observer.EnsureMeter(meter),
	}
}

// Flushは一回限りのbuffer snapshotをCloudWatch Logsのphysical requestへ計画し、
// 決定的な順序で送信します。拒否・送信失敗entryはfailure handlerへ渡し、独立した後続
// requestも処理してerrorを集約します。durable処理を完了できなかった場合は、failure handlerの
// errorに加えてbackendの送信失敗の原因も返します。context終了後は未送信分を再送対象として
// 保存し、新たなHTTP送信を開始しません。呼び出し元はsnapshotを再投入しません。
func (fm *CloudWatchLogsFlushManager) Flush(
	ctx context.Context,
	entries []*logging.LogEntry,
) error {
	if len(entries) == 0 {
		return nil
	}
	if err := fm.validateClient(ctx); err != nil {
		return err
	}
	eventRecords := convertLogEntriesToEventRecords(entries, fm.LogFormatter)
	validRecords, invalidRecords := splitValidCloudWatchLogsEventRecords(eventRecords)
	requests, plannerInvalidRecords, err := adaptCloudWatchLogsRecordsToRequestPlan(
		fm.streamNameResolver,
		validRecords,
	)
	if err != nil {
		return err
	}
	invalidRecords = append(invalidRecords, plannerInvalidRecords...)

	var flushErr error
	requestID, _ := requestctx.RequestID(ctx)
	for _, request := range requests {
		requestRecords, err := cloudWatchLogsRecordsForPlannedRequest(eventRecords, request)
		if err != nil {
			return errors.Join(flushErr, err)
		}
		requestResult, err := fm.putCloudWatchLogsRequest(
			ctx,
			fm.newSendMeta(ctx, requestID, len(requestRecords)),
			request.LogStreamName,
			requestRecords,
		)
		if !requestResult.HasFailedRecords() {
			flushErr = errors.Join(flushErr, err)
			continue
		}
		flushErr = errors.Join(
			flushErr,
			joinUnhandledSendFailure(err, requestResult, fm.handleFailure(ctx, requestResult)),
		)
	}

	if len(invalidRecords) > 0 {
		invalidResult := loggerfailure.SendResult{
			Meta: fm.newSendMeta(ctx, requestID, len(invalidRecords)),
		}
		invalidResult.QuarantineRecords = fm.buildFailedRecordsFromInvalidEvents(
			invalidResult.Meta.AttemptedAt,
			invalidRecords,
		)
		flushErr = errors.Join(
			flushErr,
			joinUnhandledSendFailure(nil, invalidResult, fm.handleFailure(ctx, invalidResult)),
		)
	}

	return flushErr
}

// Put は immediate logger 用の送信経路です。
// 失敗 record がある場合は同期 failure handler の完了を待ちます。durable処理を完了できなかった
// 場合は、failure handlerのerrorに加えてbackendの送信失敗の原因も返します。
func (fm *CloudWatchLogsFlushManager) Put(ctx context.Context, entry *logging.LogEntry) error {
	return fm.Flush(ctx, []*logging.LogEntry{entry})
}

// PutWithReport はdelivery logger用にCloudWatch Logs本体とdurable退避の結果を返します。
func (fm *CloudWatchLogsFlushManager) PutWithReport(ctx context.Context, entry *logging.LogEntry) (pkglogger.DeliveryReport, error) {
	res, err := fm.putEntryAsSingleRequest(ctx, entry)
	if err != nil && !res.HasFailedRecords() {
		return pkglogger.NewTargetDeliveryReport(cloudWatchLogsDeliveryRejectedResult(err)), err
	}
	if !res.HasSendFailure() {
		return pkglogger.NewTargetDeliveryReport(cloudWatchLogsDeliverySucceededResult()), nil
	}

	report := pkglogger.NewTargetDeliveryReport(cloudWatchLogsTargetDeliveryResult(res))
	failureReport, failureErr := fm.handleFailureWithReport(ctx, res)
	report.Durables = append(report.Durables, failureReport.Durables...)
	report.Evacuations = append(report.Evacuations, failureReport.Evacuations...)
	return report, errors.Join(err, report.Targets[0].Err, report.DurableError(), report.EvacuationError(), failureErr)
}

// FlushWithNoFallback は fallback logger 専用のbatch送信経路です。
// 各physical requestを一度だけ送信し、失敗entryはfailure handlerへ渡さず破棄します。
func (fm *CloudWatchLogsFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	if len(entries) == 0 {
		return
	}
	if fm == nil || fm.client == nil || fm.streamNameResolver == nil {
		log.Printf("failed to flush CloudWatch Logs fallback logger: client or stream name resolver is not initialized")
		return
	}
	if err := ctx.Err(); err != nil {
		log.Printf("failed to flush CloudWatch Logs fallback logger: %v", err)
		return
	}

	attemptedAt := time.Now().UTC()
	if fm.Clock != nil {
		attemptedAt = fm.Clock.Now()
	}
	allRecords := convertLogEntriesToEventRecords(entries, fm.LogFormatter)
	validRecords, invalidRecords := splitValidCloudWatchLogsEventRecords(allRecords)
	if len(invalidRecords) > 0 {
		first := invalidRecords[0]
		log.Printf(
			"CloudWatch Logs fallback conversion dropped %d log entries; first rejection at input index %d: %v",
			len(invalidRecords),
			first.record.inputIndex,
			first.err,
		)
	}
	requests, plannerInvalidRecords, err := adaptCloudWatchLogsRecordsToRequestPlan(
		fm.streamNameResolver,
		validRecords,
	)
	if err != nil {
		log.Printf("failed to plan CloudWatch Logs fallback requests: %v", err)
		return
	}
	if len(plannerInvalidRecords) > 0 {
		first := plannerInvalidRecords[0]
		log.Printf(
			"CloudWatch Logs fallback request plan dropped %d log entries; first rejection at input index %d: %v",
			len(plannerInvalidRecords),
			first.record.inputIndex,
			first.err,
		)
	}

	for _, request := range requests {
		if err := ctx.Err(); err != nil {
			log.Printf("failed to flush CloudWatch Logs fallback logger: %v", err)
			return
		}
		requestRecords, err := cloudWatchLogsRecordsForPlannedRequest(allRecords, request)
		if err != nil {
			log.Printf("failed to build CloudWatch Logs fallback request: %v", err)
			continue
		}
		output, putErr := fm.client.PutLogEvents(ctx, pkgcloudwatchlogs.PutLogEventsRequest{
			LogStreamName: request.LogStreamName,
			Events:        cloudWatchLogsEventsFromRecords(requestRecords),
			AttemptedAt:   attemptedAt,
		})
		if putErr != nil {
			log.Printf("failed to flush CloudWatch Logs fallback logger: %v", putErr)
			continue
		}
		if output == nil {
			log.Printf("failed to flush CloudWatch Logs fallback logger: %v", errNilCloudWatchLogsPutLogEventsOutput())
			continue
		}
		logRejectedCloudWatchLogsOutput("cloudwatch logs fallback logger", output, len(requestRecords))
	}
}

// Closeは呼び出し元の終了期限に従い、Drain開始前に受理済みのfailure handlingが
// 完了するまで待ちます。
func (fm *CloudWatchLogsFlushManager) Close(ctx context.Context) error {
	if fm == nil || fm.FailureHandler == nil {
		return nil
	}
	return fm.FailureHandler.Drain(ctx)
}

// putEntryAsSingleRequestはdelivery経路の一件を計画・送信し、最大一回のphysical
// PutLogEvents結果をbackend共通のloggerfailure.SendResultへ変換します。buffer snapshotの
// stream分類と複数request送信はFlushが担当します。AWSの受付可能期間はローカル判定せず、
// rejectedLogEventsInfoを結果の正本とします。
func (fm *CloudWatchLogsFlushManager) putEntryAsSingleRequest(
	ctx context.Context,
	entry *logging.LogEntry,
) (loggerfailure.SendResult, error) {
	if err := fm.validateClient(ctx); err != nil {
		return loggerfailure.SendResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return loggerfailure.SendResult{}, err
	}

	allEventRecords := convertLogEntriesToEventRecords(
		[]*logging.LogEntry{entry},
		fm.LogFormatter,
	)
	eventRecords, invalidEventRecords := splitValidCloudWatchLogsEventRecords(allEventRecords)
	requests, plannerInvalidRecords, err := adaptCloudWatchLogsRecordsToRequestPlan(
		fm.streamNameResolver,
		eventRecords,
	)
	if err != nil {
		return loggerfailure.SendResult{}, err
	}
	invalidEventRecords = append(invalidEventRecords, plannerInvalidRecords...)
	if len(requests) > 1 {
		return loggerfailure.SendResult{}, fmt.Errorf(
			"cloudwatch logs single-entry put requires at most one PutLogEvents request: planned requests=%d",
			len(requests),
		)
	}

	requestID, _ := requestctx.RequestID(ctx)
	result := loggerfailure.SendResult{
		Meta: fm.newSendMeta(ctx, requestID, 1),
	}
	if len(invalidEventRecords) > 0 {
		result.QuarantineRecords = append(
			result.QuarantineRecords,
			fm.buildFailedRecordsFromInvalidEvents(result.Meta.AttemptedAt, invalidEventRecords)...,
		)
	}
	if len(requests) == 0 {
		return result, nil
	}

	requestRecords, err := cloudWatchLogsRecordsForPlannedRequest(
		allEventRecords,
		requests[0],
	)
	if err != nil {
		return result, err
	}
	requestResult, err := fm.putCloudWatchLogsRequest(
		ctx,
		result.Meta,
		requests[0].LogStreamName,
		requestRecords,
	)
	result.RetryableRecords = append(result.RetryableRecords, requestResult.RetryableRecords...)
	result.DeadRecords = append(result.DeadRecords, requestResult.DeadRecords...)
	result.QuarantineRecords = append(result.QuarantineRecords, requestResult.QuarantineRecords...)
	result.TerminalError = requestResult.TerminalError
	return result, err
}

// putCloudWatchLogsRequestはrouting・sort・分割済みの一physical requestだけを送信し、
// backendのresponseをdurable処理用SendResultへ変換します。HTTP 200内の部分拒否では、
// rejectedLogEventsInfoが示したeventだけを失敗recordへ含めます。
func (fm *CloudWatchLogsFlushManager) putCloudWatchLogsRequest(
	ctx context.Context,
	meta loggerfailure.Meta,
	logStreamName string,
	eventRecords []cloudWatchLogsEventRecord,
) (loggerfailure.SendResult, error) {
	result := loggerfailure.SendResult{Meta: meta}
	if err := ctx.Err(); err != nil {
		// No backend attempt began. Preserve the accepted input for later replay.
		result.TerminalError = err
		result.RetryableRecords = fm.buildFailedRecordsFromPutError(
			meta.AttemptedAt, logStreamName, eventRecords,
			retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonRequestNotSent}, err,
		)
		return result, err
	}

	ctx, span := fm.Tracer.StartSpan(ctx, SpanCloudWatchLogsPut)
	defer span.End()

	start := time.Now()
	output, putErr := fm.client.PutLogEvents(ctx, pkgcloudwatchlogs.PutLogEventsRequest{
		LogStreamName: logStreamName,
		Events:        cloudWatchLogsEventsFromRecords(eventRecords),
		AttemptedAt:   meta.AttemptedAt,
	})
	fm.handlePutDelayLog(ctx, time.Since(start))

	// HTTP成功後にctxが終了していても、CloudWatch Logs側では受理済みの可能性が
	// あるため、putErr == nilならresponseを優先します。
	if putErr == nil {
		// clientがerrorなしでnilを返した場合は、送信結果を確認できない原因をsentinelで保持します。
		outputErr := errNilCloudWatchLogsPutLogEventsOutput()
		if output != nil {
			outputErr = output.Validate(len(eventRecords))
		}
		if outputErr != nil {
			span.RecordError(outputErr)
			result.TerminalError = outputErr
			result.QuarantineRecords = append(
				result.QuarantineRecords,
				fm.buildFailedRecordsFromPutError(
					meta.AttemptedAt,
					logStreamName,
					eventRecords,
					retryable.RetryDecision{
						Quarantine: true,
						Reason:     retryable.RetryReasonUnknownOutcome,
					},
					outputErr,
				)...,
			)
			return result, nil
		}
		fm.addRejectedLogEventFailures(
			&result,
			eventRecords,
			logStreamName,
			output,
		)
		return result, nil
	}
	span.RecordError(putErr)
	if infracloudwatchlogs.IsRequestEntityTooLarge(putErr) && len(eventRecords) > 1 {
		// 413ではどのeventも受理されていないため、timestamp順を保って半分ずつ再送し、
		// 一件でも拒否されたeventだけを分類器の判定どおりquarantineへ隔離します。
		return fm.putSplitCloudWatchLogsRequest(ctx, meta, logStreamName, eventRecords, putErr)
	}
	// Preserve explicit backend responses even if the caller stops afterwards.
	// An attempt stopped with the caller's context does not establish acceptance.
	decision := infracloudwatchlogs.ClassifyDurableRequestOutcome(ctx, putErr)
	sendErr := ctx.Err()
	if sendErr != nil && (errors.Is(putErr, context.Canceled) || errors.Is(putErr, context.DeadlineExceeded)) {
		sendErr = errors.Join(sendErr, putErr)
	}
	if sendErr != nil {
		result.TerminalError = sendErr
	}
	failedRecords := fm.buildFailedRecordsFromPutError(
		meta.AttemptedAt,
		logStreamName,
		eventRecords,
		decision,
		putErr,
	)
	if decision.Quarantine {
		result.QuarantineRecords = append(result.QuarantineRecords, failedRecords...)
	} else if decision.Retry {
		result.RetryableRecords = append(result.RetryableRecords, failedRecords...)
	} else {
		result.DeadRecords = append(result.DeadRecords, failedRecords...)
	}
	return result, sendErr
}

// putSplitCloudWatchLogsRequestは、HTTP 413で拒否された複数eventのrequestを前半と後半に
// 分けて再送し、結果をまとめます。分けたrequestを送る前に呼び出し元が停止した場合は、
// 確定した413を保持して未送信のeventを再送対象にします。一部のeventが受理された場合は
// TerminalErrorを設定しません。
func (fm *CloudWatchLogsFlushManager) putSplitCloudWatchLogsRequest(
	ctx context.Context,
	meta loggerfailure.Meta,
	logStreamName string,
	eventRecords []cloudWatchLogsEventRecord,
	rejection error,
) (loggerfailure.SendResult, error) {
	result := loggerfailure.SendResult{Meta: meta}
	middle := len(eventRecords) / 2
	var terminalErrs, sendErrs []error
	for _, half := range [][]cloudWatchLogsEventRecord{eventRecords[:middle], eventRecords[middle:]} {
		var halfResult loggerfailure.SendResult
		var halfErr error
		if ctxErr := ctx.Err(); ctxErr != nil {
			halfResult = loggerfailure.SendResult{
				TerminalError: ctxErr,
				RetryableRecords: fm.buildFailedRecordsFromPutError(
					meta.AttemptedAt, logStreamName, half,
					retryable.RetryDecision{
						Retry:  true,
						Reason: retryable.RetryReasonRequestNotSent,
						Code:   http.StatusRequestEntityTooLarge,
					},
					errors.Join(rejection, ctxErr),
				),
			}
			halfErr = ctxErr
		} else {
			halfResult, halfErr = fm.putCloudWatchLogsRequest(ctx, meta, logStreamName, half)
		}
		result.RetryableRecords = append(result.RetryableRecords, halfResult.RetryableRecords...)
		result.DeadRecords = append(result.DeadRecords, halfResult.DeadRecords...)
		result.QuarantineRecords = append(result.QuarantineRecords, halfResult.QuarantineRecords...)
		terminalErrs = append(terminalErrs, halfResult.TerminalError)
		sendErrs = append(sendErrs, halfErr)
	}
	result.TerminalError = errors.Join(terminalErrs...)
	// 分けたrequestの一部が受理された場合は、受理済みの結果を消さないようTerminalErrorを
	// 設定しません（Loki、OpenSearchと同じ規則です）。受理されなかったeventは、それぞれの
	// recordに原因を保持します。
	if result.FailedCount() < len(eventRecords) {
		result.TerminalError = nil
	}
	return result, errors.Join(sendErrs...)
}

var errCloudWatchLogsNilPutLogEventsOutput = errors.New("cloudwatch logs client returned nil PutLogEventsOutput without error")

func errNilCloudWatchLogsPutLogEventsOutput() error { return errCloudWatchLogsNilPutLogEventsOutput }

// ErrClientNotInitializedは、送信に使うCloudWatch Logs clientまたはlog stream名の解決器が
// 設定されていないことを表します。
var ErrClientNotInitialized = errors.New("CloudWatch Logs client or stream name resolver is not initialized")

// validateClientは未初期化のclientまたはstream resolverをhandlerへ通知し、送信を止めます。
func (fm *CloudWatchLogsFlushManager) validateClient(ctx context.Context) error {
	if fm.client != nil && fm.streamNameResolver != nil {
		return nil
	}

	err := ErrClientNotInitialized
	fm.notifyError(
		ctx,
		&cloudwatchlogserrors.CloudWatchLogsLoggerInternalError{Op: "cloudwatch logs validate dependencies", Err: err},
		fm.InternalErrorEntry("CloudWatch Logs client or stream name resolver is not initialized", "", err),
		fm.Tracer,
		fm.Meter,
	)
	return err
}

// handlePutDelayLog は PutLogEvents の遅延が設定閾値を超えた場合に内部 warn/error を発火します。
func (fm *CloudWatchLogsFlushManager) handlePutDelayLog(ctx context.Context, elapsed time.Duration) {
	if fm == nil || fm.config == nil || !fm.config.PutLatencyEnabled() {
		return
	}

	switch {
	case fm.config.PostErrorThresholdDuration() > 0 && elapsed > fm.config.PostErrorThresholdDuration():
		err := fmt.Errorf("cloudwatch logs put delay exceeded error threshold: %s", elapsed)
		fm.notifyError(
			ctx,
			&cloudwatchlogserrors.CloudWatchLogsLoggerInternalError{Op: "cloudwatch logs put delay", Err: err},
			fm.InternalErrorEntry(fmt.Sprintf("cloudwatch logs put delay exceeded error threshold: %s", elapsed), "", nil),
			fm.Tracer,
			fm.Meter,
		)
	case fm.config.PostWarnThresholdDuration() > 0 && elapsed > fm.config.PostWarnThresholdDuration():
		fm.notifyWarn(
			ctx,
			fm.InternalWarnEntry(fmt.Sprintf("cloudwatch logs put delay exceeded warn threshold: %s", elapsed), ""),
			fm.Tracer,
			fm.Meter,
		)
	}
}

// handleFailureはbuffer flushまたはimmediate送信で発生した失敗recordの
// durable処理完了を待ちます。
func (fm *CloudWatchLogsFlushManager) handleFailure(
	ctx context.Context,
	res loggerfailure.SendResult,
) error {
	if fm.FailureHandler == nil {
		fm.notifyFailureWithoutHandler(ctx, res)
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
// 同じphysical requestのrecordは同じ送信errorを共有するため、一つだけを返します。
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
func (fm *CloudWatchLogsFlushManager) handleFailureWithReport(
	ctx context.Context,
	res loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	if fm.FailureReportHandler == nil {
		fm.notifyFailureWithoutHandler(ctx, res)
		return pkglogger.DeliveryReport{}, flushfailure.ErrNoFailureHandler
	}
	return fm.FailureReportHandler.HandleFailureWithReport(ctx, res)
}

func (fm *CloudWatchLogsFlushManager) notifyFailureWithoutHandler(ctx context.Context, res loggerfailure.SendResult) {
	err := fmt.Errorf("cloudwatch logs failure handler is not configured: %d failed entrie(s)", res.FailedCount())
	fm.notifyError(
		ctx,
		&cloudwatchlogserrors.CloudWatchLogsLoggerInternalError{Op: "cloudwatch logs failure handler", Err: err},
		fm.InternalErrorEntry("failed to handle cloudwatch logs failure records. failureHandler is nil. error:", res.Meta.RequestID, err),
		fm.Tracer,
		fm.Meter,
	)
}

// newSendMeta はfailure recordに共通で付与するrequest/resource metadataを作ります。
func (fm *CloudWatchLogsFlushManager) newSendMeta(ctx context.Context, requestID string, inputCount int) loggerfailure.Meta {
	now := time.Now().UTC()
	if fm.Clock != nil {
		now = fm.Clock.Now()
	}

	meta := loggerfailure.Meta{
		Backend:     deliveryBackendCloudWatchLogs,
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
