package telemetry

import (
	"context"
	"strings"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RecordInternalLogは、levelの内部ログを1件数えます。
func RecordInternalLog(ctx context.Context, meter pkgobserver.Meter, level pkglogger.LogLevel) {
	if ctx == nil {
		ctx = context.Background()
	}
	observer.EnsureMeter(meter).Counter(InternalLogs).Add(ctx, 1,
		pkgobserver.Attr{Key: AttrLevel, Value: LevelName(level)},
	)
}

// LevelNameは、metricsとspanの属性に使うlevelの名前（小文字）を返します。
func LevelName(level pkglogger.LogLevel) string {
	return strings.ToLower(level.String())
}
