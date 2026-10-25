package flushfailure

import (
	"context"
	"errors"
	"fmt"
	"sync"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// ReportingCompositeFailureHandler は、report対応childだけを束ねるDelivery用Compositeです。
// 埋め込んだCompositeFailureHandlerが同期処理とDrainを担います。Drainは通常の
// HandleFailureだけでなく、この型が受理したHandleFailureWithReportの完了も待ってから
// childをShutdownします。この型だけがFailureReportHandlerを実装します。
type ReportingCompositeFailureHandler struct {
	*CompositeFailureHandler
	reportHandlers []FailureHandlerWithReport
}

var (
	_ FailureHandler       = (*ReportingCompositeFailureHandler)(nil)
	_ FailureReportHandler = (*ReportingCompositeFailureHandler)(nil)
)

// NewReportingCompositeFailureHandler はreport対応childだけを受け取り、通常のfailure処理と
// report処理で同じchild instanceを共有するCompositeを返します。同時実行上限は正の値を
// 必須とし、0以下を暗黙のdefaultへ補正しません。
func NewReportingCompositeFailureHandler(
	handlers []FailureHandlerWithReport,
	maxConcurrent int,
) (*ReportingCompositeFailureHandler, error) {
	if err := pkgdurable.ValidateFailureHandlerMaxConcurrent(maxConcurrent); err != nil {
		return nil, err
	}
	return newReportingCompositeFailureHandler(handlers, maxConcurrent), nil
}

func newReportingCompositeFailureHandler(
	handlers []FailureHandlerWithReport,
	maxConcurrent int,
) *ReportingCompositeFailureHandler {
	reportHandlers := append([]FailureHandlerWithReport(nil), handlers...)
	baseHandlers := make([]FailureHandler, 0, len(reportHandlers))
	for _, handler := range reportHandlers {
		baseHandlers = append(baseHandlers, handler)
	}

	return &ReportingCompositeFailureHandler{
		CompositeFailureHandler: newCompositeFailureHandler(baseHandlers, maxConcurrent),
		reportHandlers:          reportHandlers,
	}
}

// HandleFailureWithReport はreport対応childを並列実行し、durable処理と退避処理の
// 結果だけを集約します。Targetsは本来の送信先を扱う呼び出し側が設定します。
func (c *ReportingCompositeFailureHandler) HandleFailureWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	var report pkglogger.DeliveryReport
	if c == nil || c.CompositeFailureHandler == nil {
		return report, ErrNoFailureHandler
	}
	finishHandling, err := c.beginHandling()
	if err != nil {
		return report, err
	}
	finishTransferred := false
	defer func() {
		if !finishTransferred {
			finishHandling()
		}
	}()

	handleCtx, cancel := c.handleContext(ctx)
	defer cancel()

	type childResult struct {
		report pkglogger.DeliveryReport
		err    error
	}
	resultCh := make(chan childResult, len(c.reportHandlers))
	var wg sync.WaitGroup
	started := 0
	errs := make([]error, 0, len(c.reportHandlers)+1)

	for index, handler := range c.reportHandlers {
		if handler == nil {
			continue
		}
		started++

		wg.Add(1)
		go func(index int, handler FailureHandlerWithReport) {
			defer wg.Done()
			outcome := childResult{}
			defer func() {
				if recovered := recover(); recovered != nil {
					outcome.err = &FailureHandlerError{
						Index: index, Handler: handler, Report: true,
						Operation: FailureHandlerOperationPanic, PanicValue: recovered,
					}
				}
				resultCh <- outcome
			}()

			if err := c.acquire(handleCtx); err != nil {
				outcome.err = &FailureHandlerError{
					Index: index, Handler: handler, Report: true, Operation: FailureHandlerOperationAcquire, Err: err,
				}
				return
			}
			defer c.release()

			childReport, err := handler.HandleFailureWithReport(handleCtx, result)
			outcome.report = childReport
			if err != nil {
				outcome.err = &FailureHandlerError{
					Index: index, Handler: handler, Report: true, Operation: FailureHandlerOperationHandle, Err: err,
				}
			}
		}(index, handler)
	}

	if started == 0 {
		return report, ErrNoFailureHandler
	}

	mergeChildResult := func(result childResult) {
		report.Durables = append(report.Durables, result.report.Durables...)
		report.Evacuations = append(report.Evacuations, result.report.Evacuations...)
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}

	done := waitAndFinish(&wg, finishHandling)
	finishTransferred = true
	select {
	case <-done:
		close(resultCh)
		for result := range resultCh {
			mergeChildResult(result)
		}
		return report, errors.Join(errs...)
	case <-handleCtx.Done():
	drainCompleted:
		for {
			select {
			case result := <-resultCh:
				mergeChildResult(result)
			default:
				break drainCompleted
			}
		}
		errs = append(errs, fmt.Errorf(
			"failure report handling timed out: %w",
			handleCtx.Err(),
		))
		return report, errors.Join(errs...)
	}
}
