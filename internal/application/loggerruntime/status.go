package loggerruntime

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/configview"
	"github.com/go-jappe/heterolog/internal/application/status"
)

// Statusは、このloggerの様子の材料（内部のMeterのmetricsと、infrastructure層が示す
// spoolとdirectory）を返します。管理用APIのstatusの報告が使います。
func (r *Runtime) Status(ctx context.Context) status.MemberStatus {
	return status.MemberStatus{Kind: configview.KindLogger, Metrics: r.meter.Collect(), Inspection: inspect(ctx, r.Logger)}
}

// Statusは、このloggerの様子の材料を返します。Runtime.Statusのとおりです。
func (r *AsyncRuntime) Status(ctx context.Context) status.MemberStatus {
	return status.MemberStatus{Kind: configview.KindAsyncLogger, Metrics: r.meter.Collect(), Inspection: inspect(ctx, r.AsyncLogger)}
}

// Statusは、このloggerの様子の材料を返します。Runtime.Statusのとおりです。
func (r *DeliveryRuntime) Status(ctx context.Context) status.MemberStatus {
	return status.MemberStatus{Kind: configview.KindDeliveryLogger, Metrics: r.meter.Collect(), Inspection: inspect(ctx, r.DeliveryLogger)}
}

// inspectは、Builderが組み立てたloggerがspoolとdirectoryを示せれば、それを返します。
func inspect(ctx context.Context, logger any) status.Inspection {
	if inspector, ok := logger.(status.Inspector); ok {
		return inspector.InspectStatus(ctx)
	}
	return status.Inspection{}
}
