package lifecycle

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
)

// dispatchedCompressedLogFileUploadFailureRunReporterはRunReporter呼出しだけをdispatcherへ渡します。
// report生成元のrunner/coordinatorにはcallback完了を待たせません。
type dispatchedCompressedLogFileUploadFailureRunReporter struct {
	next       RunReporter
	dispatcher *notification.Dispatcher
	// forceContextは通常の周期停止では中止せず、所有者の強制終了だけを通知へ伝えます。
	forceContext context.Context
}

func (r *dispatchedCompressedLogFileUploadFailureRunReporter) ReportRun(
	ctx context.Context,
	report RunReport,
	err error,
) {
	if r == nil || r.next == nil {
		return
	}
	r.dispatcher.Enqueue(func() {
		// 発行元の周期が終了しても、受理した通知は出力が終わるまで継続します。
		ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		if r.forceContext != nil {
			stop := context.AfterFunc(r.forceContext, cancel)
			defer stop()
			if r.forceContext.Err() != nil {
				cancel()
			}
		}
		r.next.ReportRun(ctx, report, err)
	})
}
