package multi

import (
	"context"
	"sync"
	"testing"
	"time"

	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestAsyncAndDeliveryMultiLoggersIsolateEntriesForEachHandler(t *testing.T) {
	for _, mode := range []string{"async", "delivery"} {
		t.Run(mode, func(t *testing.T) {
			original := testLogEntry(pkglogger.LogLevelInfo, "original")
			original.File = []string{"original.go"}
			original.Line = []int{42}
			original.FuncName = []string{"originalFunction"}
			original.Fields = map[string]any{"component": "original"}
			var seenMu sync.Mutex
			var seen []*infralogging.LogEntry
			record := func(entry *infralogging.LogEntry) {
				seenMu.Lock()
				seen = append(seen, entry)
				seenMu.Unlock()
			}

			switch mode {
			case "async":
				newHandler := func() *syncHandlerStub {
					return &syncHandlerStub{handleFn: func(_ context.Context, entry *infralogging.LogEntry) error {
						record(entry)
						return nil
					}}
				}
				logger, err := NewAsyncBestEffortMultiLogger(
					asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 2},
					newHandler(), newHandler(),
				)
				require.NoError(t, err)
				logger.runAll(context.Background(), original)
				require.NoError(t, logger.Close())
			case "delivery":
				newHandler := func() *deliveryHandlerStub {
					return &deliveryHandlerStub{handleFn: func(_ context.Context, entry *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
						record(entry)
						return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
							Backend: "test", Status: pkglogger.DeliverySucceeded,
						}), nil
					}}
				}
				logger := newTestDeliveryMultiLogger(newHandler(), newHandler())
				_, err := logger.runAll(context.Background(), original)
				require.NoError(t, err)
				require.NoError(t, logger.Close())
			}

			// handlerのentryを確認・変更する前に、Closeでhandlerの終了を待ちます。
			// これにより、test自体にraceを持ち込まずにaliasingも検出できます。
			require.Len(t, seen, 2)
			first, second := seen[0], seen[1]
			require.Equal(t, original, first)
			require.Equal(t, original, second)

			first.Message = "first handler only"
			first.File[0] = "first.go"
			first.Line[0] = 99
			first.FuncName[0] = "firstFunction"
			first.Fields["component"] = "first"
			first.Fields["added"] = true
			for _, unchanged := range []*infralogging.LogEntry{original, second} {
				require.Equal(t, "original", unchanged.Message)
				require.Equal(t, []string{"original.go"}, unchanged.File)
				require.Equal(t, []int{42}, unchanged.Line)
				require.Equal(t, []string{"originalFunction"}, unchanged.FuncName)
				require.Equal(t, map[string]any{"component": "original"}, unchanged.Fields)
			}
		})
	}
}
