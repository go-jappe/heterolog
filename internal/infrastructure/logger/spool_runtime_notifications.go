package logger

import (
	"context"
	"errors"

	fsstoreretryafter "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type failureSpoolRecoveryOptions struct {
	onError logging.LoggerErrorHandler
	onWarn  logging.LoggerHandlerFunc
	// senderは試験がbackendへの送信だけを置き換えるための差し替え口です。公開optionは
	// なく、通常はnilのまま設定のclientからbackend別のsenderを構築します。
	sender worker.Sender
}

type FailureSpoolRecoveryOption func(*failureSpoolRecoveryOptions)

// WithFailureSpoolRecoveryOnErrorは、writerを持たない復旧専用runtimeの内部エラー通知先です。
// 通常Loggerではspool writerのHandlerConfigで選択されたOnError/OnWarnを共有します。
// callbackは別のgoroutineで呼びます。通知中に終了を要求する場合はRequestCloseを使います。
func WithFailureSpoolRecoveryOnError(onError logging.LoggerErrorHandler) FailureSpoolRecoveryOption {
	return func(options *failureSpoolRecoveryOptions) { options.onError = onError }
}

// WithFailureSpoolRecoveryOnWarnは、復旧専用runtimeの隔離・保全成功の警告通知先です。
// callbackは別のgoroutineで呼びます。通知中に終了を要求する場合はRequestCloseを使います。
func WithFailureSpoolRecoveryOnWarn(onWarn logging.LoggerHandlerFunc) FailureSpoolRecoveryOption {
	return func(options *failureSpoolRecoveryOptions) { options.onWarn = onWarn }
}

// configureRunnerNotificationsは構築中だけ呼びます。通常結果はSupervisorのreportに残し、
// 内部エラーと隔離の警告だけを既存のOnError/OnWarnへ委譲します。
func (r *FailureSpoolRuntime) configureRunnerNotifications(
	runner *failureSpoolRunnerRuntime,
	onError logging.LoggerErrorHandler,
	onWarn logging.LoggerHandlerFunc,
) {
	if r == nil || runner == nil || runner.supervisor == nil || (onError == nil && onWarn == nil) {
		return
	}
	if r.notifications == nil {
		r.notifications = notification.NewDispatcher()
	}
	if onWarn != nil {
		// 保存先の保全成功はRunReportとは別に発生します。既存の通知先を共有し、
		// worker上では通知登録だけを行い、通常Closeが後段の通知完了も待ちます。
		runner.storeOnWarn = func(ctx context.Context, entry *logging.LogEntry, tracer pkgobserver.Tracer, meter pkgobserver.Meter) {
			if entry == nil {
				return
			}
			notification := *entry
			notification.Source = "failure_spool_runner"
			notification.Fields = make(map[string]any, len(entry.Fields)+2)
			for key, value := range entry.Fields {
				notification.Fields[key] = value
			}
			notification.Fields["logger_contract"] = runner.contractKind
			notification.Fields["backend"] = runner.backend
			r.notifications.Enqueue(func() {
				// Storeの保全通知は送信期限から切り離されています。強制終了だけを再接続します。
				r.lifecycleMu.Lock()
				forceContext := r.operationContext
				r.lifecycleMu.Unlock()
				notificationContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
				defer cancel()
				if forceContext != nil {
					stop := context.AfterFunc(forceContext, cancel)
					defer stop()
					if forceContext.Err() != nil {
						cancel()
					}
				}
				handler.CallWarnHandlerIfPresent(onWarn, notificationContext, &notification, tracer, meter)
			})
		}
	}
	runner.supervisor.Reporter = &failureSpoolRunLogReporter{
		notifications: r.notifications,
		onError:       onError,
		onWarn:        onWarn,
		contract:      runner.contractKind,
		backend:       runner.backend,
		clock:         runner.clock,
		tracer:        runner.supervisor.Tracer,
		meter:         runner.supervisor.Meter,
	}
}

type failureSpoolRunLogReporter struct {
	notifications *notification.Dispatcher
	onError       logging.LoggerErrorHandler
	onWarn        logging.LoggerHandlerFunc
	contract      pkglogger.LoggerContractKind
	backend       pkglogger.LogOutputDestination
	clock         clock.Clock
	tracer        pkgobserver.Tracer
	meter         pkgobserver.Meter
}

func (r *failureSpoolRunLogReporter) ReportSpoolRunnerRun(ctx context.Context, report logspool.RunReport, err error) {
	// StopOnError等に使う戻り値は変更せず、停止要求だけのエラーは通知から除きます。
	if notificationErr := failureSpoolNotificationError(ctx, err); notificationErr != nil && r.onError != nil {
		entry := r.entry(pkglogger.LogLevelError, "failure spool runner failed", report, notificationErr)
		r.notifications.Enqueue(func() {
			handler.CallErrorHandlerIfPresent(r.onError, ctx, notificationErr, entry, r.tracer, r.meter)
		})
	}
	// 壊れたspoolを隔離できた場合は処理を継続するため戻り値がnilでも、原因はreportに残ります。
	// 戻り値ですでに通知したエラーは重複通知しません。
	process := report.ProcessReport
	if process.Status == worker.ProcessStatusQuarantined && process.Err != nil && !errors.Is(err, process.Err) {
		if r.onWarn != nil {
			entry := r.entry(pkglogger.LogLevelWarn, "failure spool file quarantined", report, process.Err)
			r.notifications.Enqueue(func() {
				handler.CallWarnHandlerIfPresent(r.onWarn, ctx, entry, r.tracer, r.meter)
			})
		}
	}
	// backendが上限を超えるRetry-Afterを指定した場合や、保存済みの再送下限時刻が上限より
	// 先だった場合は、上限まで短縮して待機することを発生ごとに一回通知します。
	if clamp := process.RetryAfterClamp; clamp != nil && r.onWarn != nil {
		entry := r.entry(pkglogger.LogLevelWarn, "failure spool Retry-After wait exceeds the maximum", report, nil)
		entry.Fields["requested_retry_after"] = clamp.Requested.String()
		entry.Fields["max_retry_after"] = clamp.MaxRetryAfter.String()
		entry.Fields["not_before"] = clamp.NotBefore
		entry.Fields["stored_not_before"] = clamp.Stored
		r.notifications.Enqueue(func() {
			handler.CallWarnHandlerIfPresent(r.onWarn, ctx, entry, r.tracer, r.meter)
		})
	}
	// 送信先の待機期限を保存したmarkerの読み込み・保存・削除に失敗しても、再送は止めずに警告します。
	// 保存に失敗した期限はprocess内で守り、期限が残る間は保存を再試行します。続く失敗は
	// 一連の失敗として通知し直しません。保存できないまま再起動した場合は引き継がれない場合があります。
	if markerErr := process.RetryAfterMarkerErr; markerErr != nil && r.onWarn != nil {
		entry := r.entry(pkglogger.LogLevelWarn, "failure spool Retry-After marker failed", report, markerErr)
		var marker *fsstoreretryafter.MarkerError
		if errors.As(markerErr, &marker) {
			entry.Fields["retry_after_marker_operation"] = marker.Operation
			entry.Fields["retry_after_marker_path"] = marker.Path
		}
		r.notifications.Enqueue(func() {
			handler.CallWarnHandlerIfPresent(r.onWarn, ctx, entry, r.tracer, r.meter)
		})
	}
}

func (r *failureSpoolRunLogReporter) entry(level pkglogger.LogLevel, message string, report logspool.RunReport, err error) *logging.LogEntry {
	return &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(r.clock),
		Level:      level,
		Message:    message,
		Err:        err,
		Internal:   true,
		Source:     "failure_spool_runner",
		Fields: map[string]any{
			"logger_contract":            r.contract,
			"backend":                    r.backend,
			"processing_path":            report.ProcessReport.ProcessingPath,
			"processing_status":          report.ProcessReport.Status,
			"transition_retry_path":      report.ProcessingFileTransitionRetryReport.ProcessingPath,
			"transition_retry_operation": report.ProcessingFileTransitionRetryReport.Operation,
		},
	}
}

// runtimeへの中止要求だけは異常通知から除きます。送信timeoutや、まだ稼働中の
// runtimeに戻ったcontext errorは通知し、Joinされた別の障害も捨てません。
// Unwrapがnilや空の原因を返すerror（例: Errがnilのos.PathError）は辿れる原因を
// 持たないため、それ自体を障害として判定し、中止要求でなければ通知します。
func failureSpoolNotificationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var remaining []error
		causes := 0
		for _, cause := range joined.Unwrap() {
			if cause == nil {
				continue
			}
			causes++
			if cause = failureSpoolNotificationError(ctx, cause); cause != nil {
				remaining = append(remaining, cause)
			}
		}
		if causes > 0 {
			return errors.Join(remaining...)
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if cause := wrapped.Unwrap(); cause != nil {
			if failureSpoolNotificationError(ctx, cause) == nil {
				return nil
			}
			return err
		}
	}
	if errors.Is(ctx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
