package lifecycle

import (
	"context"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
)

type Store interface {
	PurgeDoneIfNeed(ctx context.Context) (report.PurgeReport, error)

	PurgeDeadIfNeed(ctx context.Context) (report.PurgeReport, error)

	PurgeQuarantineIfNeed(ctx context.Context) (report.PurgeReport, error)
}
