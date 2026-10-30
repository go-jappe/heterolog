package failurestore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	fsstoreretryafter "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// internalHandlersは、spoolとfailure archiveの内部ログのうち、ERRORとWARNをerror handlerへ
// 渡す通知関数を返します。ログの通知先の設定は使いません。INFOとDEBUGは知らせません。
func internalHandlers(report delivery.ErrorHandler) flushfailure.InternalLogHandlerFactory {
	return func(pkgdurable.DurableType, loggerhandler.ConfigProvider) flushfailure.InternalHandlers {
		return flushfailure.InternalHandlers{
			OnError: func(_ context.Context, err error, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				report(entryError(entry, err))
			},
			OnWarn: warnHandler(report),
		}
	}
}

func warnHandler(report delivery.ErrorHandler) logging.LoggerHandlerFunc {
	return func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		report(entryError(entry, nil))
	}
}

// entryErrorは、内部ログを、発生元、本文、原因、fieldを並べたerrorにします。
func entryError(entry *logging.LogEntry, err error) error {
	if entry == nil {
		return fmt.Errorf("telemetry failure store: %w", err)
	}
	if err == nil {
		err = entry.Err
	}
	var b strings.Builder
	b.WriteString("telemetry failure store")
	if entry.Source != "" {
		b.WriteString(" " + entry.Source)
	}
	b.WriteString(": " + entry.Message)
	for _, key := range slices.Sorted(maps.Keys(entry.Fields)) {
		fmt.Fprintf(&b, " %s=%v", key, entry.Fields[key])
	}
	if err == nil {
		return errors.New(b.String())
	}
	return fmt.Errorf("%s: %w", b.String(), err)
}

// runReporterは、spoolから送り直すrunnerの各周期の結果のうち、知らせるべきものを
// error handlerへ渡します。ログの出力先のrunnerと同じく、runnerのerror、壊れたfileの
// quarantine、Retry-Afterの期限の短縮、期限を保存するfileの失敗を知らせます。
type runReporter struct {
	backend string
	report  delivery.ErrorHandler
}

func (r runReporter) ReportSpoolRunnerRun(ctx context.Context, run logspool.RunReport, err error) {
	if err := logspool.NotificationError(ctx, err); err != nil {
		r.report(fmt.Errorf("the %s failure spool runner failed: %w", r.backend, err))
	}
	process := run.ProcessReport
	if process.Status == worker.ProcessStatusQuarantined && process.Err != nil && !errors.Is(err, process.Err) {
		r.report(fmt.Errorf("the %s failure spool quarantined %s: %w", r.backend, process.ProcessingPath, process.Err))
	}
	if clamp := process.RetryAfterClamp; clamp != nil {
		r.report(fmt.Errorf("the %s failure spool shortened a Retry-After of %s to the maximum of %s",
			r.backend, clamp.Requested, clamp.MaxRetryAfter))
	}
	if markerErr := process.RetryAfterMarkerErr; markerErr != nil {
		var marker *fsstoreretryafter.MarkerError
		if errors.As(markerErr, &marker) {
			r.report(fmt.Errorf("the %s failure spool could not %s its Retry-After marker %s: %w",
				r.backend, marker.Operation, marker.Path, markerErr))
			return
		}
		r.report(fmt.Errorf("the %s failure spool Retry-After marker failed: %w", r.backend, markerErr))
	}
}
