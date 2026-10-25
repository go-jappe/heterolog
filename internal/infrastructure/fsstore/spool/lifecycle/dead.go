package lifecycle

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func (f *FileOperator) PurgeDeadIfNeed(ctx context.Context) (report.PurgeReport, error) {
	return f.purgeStateIfNeed(
		ctx,
		pkgspool.StateDead,
	)
}
