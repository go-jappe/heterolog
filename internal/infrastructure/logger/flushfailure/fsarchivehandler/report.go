package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// HandleFailureWithReport はfailure archiveまたはoverload退避の完了を待ち、
// その結果だけを返します。本来の送信先の結果は呼び出し側が設定するため、
// Targetsは空のままです。
//
// この境界は保存完了の確認を目的とするため、保存後のarchive cleanupは実行しません。
func (f *FailureArchivePipeline) HandleFailureWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	if f == nil || f.handler == nil {
		err := errors.New("failure archive handler is nil")
		return pkglogger.DeliveryReport{}, err
	}
	if !f.tryAcceptFailureRequest() {
		err := ErrFailureArchiveClosing
		return pkglogger.DeliveryReport{
			Durables: []pkglogger.DurableResult{archiveDurableResult(
				result.Meta.Backend,
				pkglogger.DurableFailed,
				err,
			)},
		}, err
	}
	defer f.finishFailureRequest()
	return f.handler.handleFailureWithReport(ctx, result)
}

// handleFailureWithReportは外側のPipelineが受理済み処理として登録した要求の保存結果を返します。

func (f *FsArchiveHandler) handleFailureWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	if result.FailedCount() == 0 {
		return pkglogger.DeliveryReport{}, nil
	}

	ctx = contextOrBackground(ctx)
	ctx, span := f.Tracer.StartSpan(ctx, SpanFileWrite)
	defer span.End()

	write := f.rotateAndWriteFailureArchive(ctx, result)
	f.reportDeferredRotations(ctx, write.deferredRotations)
	f.reportPreviousFileCloseFailures(
		ctx,
		write.previousFileCloseFailures,
	)
	f.reportArchiveRotationPreparationFailures(ctx, write.preparationFailures)

	if write.rotationErr != nil {
		span.RecordError(write.rotationErr)
		wrapped := fmt.Errorf("failed to rotate failure archive: %w", write.rotationErr)
		f.delegateError(
			ctx,
			wrapped,
			f.internalCallerErrorEntry("failed to rotate failure archive", write.rotationErr),
		)
		return pkglogger.DeliveryReport{
			Durables: []pkglogger.DurableResult{archiveDurableResult(
				result.Meta.Backend,
				pkglogger.DurableFailed,
				wrapped,
			)},
		}, wrapped
	}

	if write.overloaded {
		return f.handleOverloadWithReport(ctx, result)
	}

	if write.archiveErr == nil {
		return pkglogger.DeliveryReport{
			Durables: []pkglogger.DurableResult{archiveDurableResult(
				result.Meta.Backend,
				pkglogger.DurableSucceeded,
				nil,
			)},
		}, nil
	}

	span.RecordError(write.archiveErr)
	wrapped := fmt.Errorf("%w: %w", ErrFailureArchiveWrite, write.archiveErr)
	f.delegateError(
		ctx,
		wrapped,
		f.internalCallerErrorEntry("failed to write failure archive", write.archiveErr),
	)
	return pkglogger.DeliveryReport{
		Durables: []pkglogger.DurableResult{archiveDurableResult(
			result.Meta.Backend,
			pkglogger.DurableFailed,
			wrapped,
		)},
	}, wrapped
}

func (f *FsArchiveHandler) handleOverloadWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	kind := f.evacuationKind()
	status := pkglogger.EvacuationSucceeded
	var (
		err                  error
		internalErrorMessage string
	)

	if f.OverloadHandler == nil {
		err = errNilOverloadHandler
		internalErrorMessage = "failed to handle failure archive overload"
	} else if err = f.OverloadHandler.Handle(ctx, result); errors.Is(err, pkglogger.ErrLogDropped) {
		// Delivery APIはdrop policyの適用をEvacuationDroppedの成功した処置として返し、
		// 呼び出し元はDeliveryReport.HasDropで判定します。同じdropを退避の失敗や
		// errorとして重ねて返しません。
		err = nil
	} else if err != nil {
		internalErrorMessage = "failed to evacuate overloaded failure archive"
	}

	if err != nil {
		status = pkglogger.EvacuationFailed
		f.delegateError(
			ctx,
			err,
			f.internalCallerErrorEntry(internalErrorMessage, err),
		)
	}

	return pkglogger.DeliveryReport{
		Evacuations: []pkglogger.EvacuationResult{{
			Kind:   kind,
			Status: status,
			Err:    err,
		}},
	}, err
}

func archiveDurableResult(
	backend string,
	status pkglogger.DurableStatus,
	err error,
) pkglogger.DurableResult {
	return pkglogger.DurableResult{
		Backend: backend,
		Kind:    pkglogger.DurableArchive,
		Status:  status,
		Err:     err,
	}
}

func (f *FsArchiveHandler) evacuationKind() pkglogger.EvacuationKind {
	if f == nil || f.Config == nil || f.Config.OverloadPolicyConfig() == nil {
		return pkglogger.EvacuationDropped
	}

	switch f.Config.OverloadPolicyConfig().OverloadPolicy() {
	case pkgdurable.OverloadPolicyDegrade:
		return pkglogger.EvacuationSpilled
	case pkgdurable.OverloadPolicyDrop:
		return pkglogger.EvacuationDropped
	default:
		return pkglogger.EvacuationDropped
	}
}
