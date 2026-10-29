package file

import (
	"context"
	"errors"

	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// recordWriteは、通常の書き込み1件の結果を数えます。Close後の拒否は書き込んでいないため
// 数えず、overloadの方針dropで捨てたentryはFileDroppedEntriesで数えます。内部ログの通知の
// 書き込みは数えません。
func recordWrite(ctx context.Context, meter pkgobserver.Meter, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	outcome := telemetry.OutcomeSuccess
	switch {
	case err == nil:
	case errors.Is(err, pkglogger.ErrLoggerClosed), errors.Is(err, pkglogger.ErrLogDropped):
		return
	default:
		outcome = telemetry.OutcomeFailure
	}
	telemetry.RecordRecords(ctx, meter, BackendServices, outcome, 1)
}
