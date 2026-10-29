package multi

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

// namedHandlerStubは、metricsのbackend属性の名前を返すchildです。
type namedHandlerStub struct {
	*syncHandlerStub
	name string
}

func (h namedHandlerStub) BackendName() string { return h.name }

// TestAsyncBestEffortMultiLoggerCountsUnreturnedFailuresは、非同期APIが呼出し元へ返さない
// childのerror・timeout・panicを出力先の名前ごとに数え、受け付けなかった入力を理由ごとに
// 数えることを確認します。名前を返さないchildはunknownとして数えます。
func TestAsyncBestEffortMultiLoggerCountsUnreturnedFailures(t *testing.T) {
	previousOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(previousOutput)

	synctest.Test(t, func(t *testing.T) {
		meter := observertest.NewMeter()
		failing := namedHandlerStub{name: "loki", syncHandlerStub: &syncHandlerStub{
			handleFn: func(context.Context, *infralogging.LogEntry) error { return errors.New("rejected") },
		}}
		slow := namedHandlerStub{name: "file", syncHandlerStub: &syncHandlerStub{
			handleFn: func(ctx context.Context, _ *infralogging.LogEntry) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}}
		panicking := namedHandlerStub{name: "slack", syncHandlerStub: &syncHandlerStub{
			handleFn: func(context.Context, *infralogging.LogEntry) error { panic("async child panic") },
		}}
		unnamed := &syncHandlerStub{
			handleFn: func(context.Context, *infralogging.LogEntry) error { return errors.New("unnamed failure") },
		}
		succeeding := namedHandlerStub{name: "stdout", syncHandlerStub: &syncHandlerStub{}}
		m, err := NewAsyncBestEffortMultiLoggerWithMeter(
			asyncBestEffortConfigStub{timeout: 20 * time.Millisecond, maxParallel: 5, maxLogEntryBytes: 64},
			meter,
			failing, slow, panicking, unnamed, succeeding,
		)
		require.NoError(t, err)

		ctx := context.Background()
		m.Info(ctx, "accepted")
		m.Info(ctx, strings.Repeat("x", 100))
		require.NoError(t, m.Close())
		m.Info(ctx, "after close")

		const failures = "heterolog_async_backend_failures_total"
		const rejected = "heterolog_async_rejected_entries_total"
		require.Equal(t, map[string]int64{
			observertest.Key(failures, "backend", "loki", "reason", "error"):    1,
			observertest.Key(failures, "backend", "file", "reason", "timeout"):  1,
			observertest.Key(failures, "backend", "slack", "reason", "panic"):   1,
			observertest.Key(failures, "backend", "unknown", "reason", "error"): 1,
			observertest.Key(rejected, "reason", "invalid"):                     1,
			observertest.Key(rejected, "reason", "closed"):                      1,
		}, meter.Counters())
		require.Equal(t, int32(1), succeeding.handleCalls.Load())
	})
}
