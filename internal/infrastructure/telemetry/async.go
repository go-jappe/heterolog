package telemetry

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RecordAsyncRejectedは、非同期APIがreasonの理由でentryを1件捨てたことを数えます。
func RecordAsyncRejected(ctx context.Context, meter pkgobserver.Meter, reason string) {
	if ctx == nil {
		ctx = context.Background()
	}
	observer.EnsureMeter(meter).Counter(AsyncRejectedEntries).Add(ctx, 1,
		pkgobserver.Attr{Key: AttrReason, Value: reason},
	)
}

// RecordAsyncBackendFailureは、非同期APIが渡したentryをbackendがreasonの理由で処理できな
// かったことを数えます。
func RecordAsyncBackendFailure(ctx context.Context, meter pkgobserver.Meter, backend, reason string) {
	if ctx == nil {
		ctx = context.Background()
	}
	observer.EnsureMeter(meter).Counter(AsyncBackendFailures).Add(ctx, 1,
		pkgobserver.Attr{Key: AttrBackend, Value: backend},
		pkgobserver.Attr{Key: AttrReason, Value: reason},
	)
}
