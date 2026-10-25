package opensearch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

// slowIndexOpenSearchClientは、一件のIndexDocumentをdelayだけ遅らせます。synctestの
// bubble内で使い、遅延は仮想時間で進みます。
type slowIndexOpenSearchClient struct {
	*MockOpenSearchClient
	delay time.Duration
}

func (c *slowIndexOpenSearchClient) IndexDocument(
	ctx context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	time.Sleep(c.delay)
	return c.MockOpenSearchClient.IndexDocument(ctx, doc)
}

// directInternalDocuments は、内部ログの直接書き込みで送られたdocumentを返します。
// handlerは送信しないため、送られた内部ログのdocumentはすべて直接書き込みです。
func (c *slowIndexOpenSearchClient) directInternalDocuments() []map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var documents []map[string]any
	for _, request := range append(append([]pkgopensearch.DocumentWriteRequest(nil), c.IndexDocumentArgs...), c.IndexDocumentsArgs...) {
		if internal, _ := request.Doc["internal"].(bool); internal {
			documents = append(documents, request.Doc)
		}
	}
	return documents
}

// withNestedRequestIDは、handlerからの書き込みに識別用のrequest idを付けます。
func withNestedRequestID(ctx context.Context, depth int32) context.Context {
	return requestctx.WithRequestID(ctx, fmt.Sprintf("nested-%d", depth))
}

// requireOneDirectInternalDocumentは、再入した最も深い書き込みの遅延だけが、通知されずに
// 同じindexへ直接書き込まれたことを確認します。
func requireOneDirectInternalDocument(t *testing.T, client *slowIndexOpenSearchClient, mode reentrytest.Mode) {
	t.Helper()
	documents := client.directInternalDocuments()
	require.Len(t, documents, 1)
	require.Equal(t, pkglogger.LogLevelError.String(), documents[0]["level"])
	require.Equal(t, fmt.Sprintf("nested-%d", reentrytest.HandlerNestingDepth), documents[0]["request_id"])
}

func TestOpenSearchLoggerNoBulkHandlerWriteDoesNotRedispatchIndexDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := NewMockOpenSearchConfigProvider()
				cfg.noBulkFlushModeEnabled = true
				cfg.postWarnThresholdDuration = 100 * time.Millisecond
				cfg.postErrorThresholdDuration = 200 * time.Millisecond
				// error閾値（200ms）を超える一件送信で、毎回Index遅延のerrorを通知させます。
				client := &slowIndexOpenSearchClient{MockOpenSearchClient: NewMockOpenSearchClient(), delay: 300 * time.Millisecond}
				logger, err := NewOpenSearchLogger(
					context.Background(), cfg, client, cfg, NewMockResourceProvider(), nil,
					observer.NopTracer{}, observer.NopMeter{}, nil,
				)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })
				unexpected := func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					t.Errorf("unexpected %v internal log notification: %s", entry.Level, entry.Message)
				}
				logger.OnWarn, logger.OnInfo, logger.OnDebug = unexpected, unexpected, unexpected
				probe := reentrytest.NewProbe(t)
				var mu sync.Mutex
				var nestedErrs []error
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						err := logger.HandleLogEntry(
							withNestedRequestID(ctx, depth),
							&logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "write from OnError"},
						)
						mu.Lock()
						nestedErrs = append(nestedErrs, err)
						mu.Unlock()
					})
				}

				require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write",
				}))

				probe.RequireCounts(t, mode, 1)
				mu.Lock()
				for _, nestedErr := range nestedErrs {
					require.NoError(t, nestedErr)
				}
				mu.Unlock()
				requireOneDirectInternalDocument(t, client, mode)
			})
		})
	}
}

func TestDeliveryOpenSearchLoggerHandlerWriteDoesNotRedispatchIndexDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &slowIndexOpenSearchClient{MockOpenSearchClient: NewMockOpenSearchClient(), delay: 300 * time.Millisecond}
				loggerCfg := NewDeliveryOpenSearchLoggerConfigAdapter(pkglogger.DeliveryOpenSearchLoggerBaseConfig{
					RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: "delivery-reentry-test", Target: "opensearch"},
					LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
				})
				flushManagerCfg := NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryOpenSearchFlushManagerConfig{
					Index:                      "delivery-index",
					PostWarnThresholdDuration:  100 * time.Millisecond,
					PostErrorThresholdDuration: 200 * time.Millisecond,
					Resource:                   pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
				})
				logger := NewDeliveryOpenSearchLogger(
					context.Background(), loggerCfg, client, flushManagerCfg, NewMockResourceProvider(), nil,
					observer.NopTracer{}, observer.NopMeter{},
					&LoggerBase{Clock: pkgclock.NewUTCClock()},
				)
				t.Cleanup(func() { require.NoError(t, logger.Close()) })
				probe := reentrytest.NewProbe(t)
				var mu sync.Mutex
				var nestedStatuses []pkglogger.DeliveryStatus
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						report, err := logger.HandleLogEntry(
							withNestedRequestID(ctx, depth),
							&logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "write from OnError"},
						)
						if err != nil {
							t.Errorf("write from OnError error = %v", err)
						}
						mu.Lock()
						nestedStatuses = append(nestedStatuses, report.Targets[0].Status)
						mu.Unlock()
					})
				}

				report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write",
				})
				require.NoError(t, err)
				require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)

				probe.RequireCounts(t, mode, 1)
				mu.Lock()
				for _, status := range nestedStatuses {
					require.Equal(t, pkglogger.DeliverySucceeded, status)
				}
				mu.Unlock()
				requireOneDirectInternalDocument(t, client, mode)
			})
		})
	}
}

// indexedDocumentCountは、一件のIndexとBulkで送られたdocumentの数を返します。
func (c *slowIndexOpenSearchClient) indexedDocumentCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.IndexDocumentArgs) + len(c.IndexDocumentsArgs)
}

// TestOpenSearchLoggerReentrantDirectWriteAppliesLogLevelは、再入した書き込みの内部ログを直接
// 書き込む場合もloggerのlog levelを適用することを確認します。log levelがERRORのloggerで、Index
// 遅延のwarningを通知するOnWarnからERRORを書き込むと、その書き込みの遅延のwarningは通知されず、
// log level未満のため直接もindexへ書き込まれません。
func TestOpenSearchLoggerReentrantDirectWriteAppliesLogLevel(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := NewMockOpenSearchConfigProvider()
				cfg.noBulkFlushModeEnabled = true
				cfg.logLevel = pkglogger.LogLevelError
				// warn閾値（100ms）だけを超える一件送信で、毎回Index遅延のwarningを通知させます。
				cfg.postWarnThresholdDuration = 100 * time.Millisecond
				cfg.postErrorThresholdDuration = time.Hour
				client := &slowIndexOpenSearchClient{MockOpenSearchClient: NewMockOpenSearchClient(), delay: 300 * time.Millisecond}
				logger, err := NewOpenSearchLogger(
					context.Background(), cfg, client, cfg, NewMockResourceProvider(), nil,
					observer.NopTracer{}, observer.NopMeter{}, nil,
				)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })
				unexpected := func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					t.Errorf("unexpected %v internal log notification: %s", entry.Level, entry.Message)
				}
				logger.OnInfo, logger.OnDebug = unexpected, unexpected
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					t.Errorf("unexpected error internal log notification: %v", err)
				}
				probe := reentrytest.NewProbe(t)
				logger.OnWarn = func(ctx context.Context, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						if err := logger.HandleLogEntry(
							withNestedRequestID(ctx, depth),
							&logging.LogEntry{Level: pkglogger.LogLevelError, Message: "write from OnWarn"},
						); err != nil {
							t.Errorf("write from OnWarn error = %v", err)
						}
					})
				}

				require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelError, Message: "original write",
				}))

				probe.RequireCounts(t, mode, 1)
				require.Empty(t, client.directInternalDocuments(), "a warning below the log level must not be written directly")
				require.Equal(t, 2, client.indexedDocumentCount(), "the original and the nested write are indexed")
			})
		})
	}
}

// TestOpenSearchLoggerBulkFlushNotifiesAfterReleasingFlushLockは、件数の閾値でflushした書き込みの
// 内部ログ（clientが未初期化）をflushMuの解放後に通知し、通知の中から同じloggerへの書き込みが
// 閾値でflushしても停止しないことを確認します。flushMuを保持したまま通知すると、通知の中の
// 書き込みがflushMuを待ち続けるため、完了をhang検出の上限付きで待ちます。
func TestOpenSearchLoggerBulkFlushNotifiesAfterReleasingFlushLock(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			reentrytest.DiscardStandardLog(t)
			cfg := NewMockOpenSearchConfigProvider()
			cfg.maxBulkDocument = 1
			cfg.flushInterval = time.Hour
			logger, err := NewOpenSearchLogger(
				context.Background(), cfg, nil, cfg, NewMockResourceProvider(), nil,
				observer.NopTracer{}, observer.NopMeter{}, nil,
			)
			require.NoError(t, err)
			unexpected := func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				t.Errorf("unexpected %v internal log notification: %s", entry.Level, entry.Message)
			}
			logger.OnWarn, logger.OnInfo, logger.OnDebug = unexpected, unexpected, unexpected
			probe := reentrytest.NewProbe(t)
			var heldDuringNotification atomic.Bool
			logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				if logger.flushMu.TryLock() {
					logger.flushMu.Unlock()
				} else {
					heldDuringNotification.Store(true)
				}
				_, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					// 件数の閾値に達するため、この書き込みもその場でflushします。
					_ = logger.HandleLogEntry(ctx, &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "write from OnError"})
				})
			}

			err, finished := reentrytest.CompleteWithin(t, "a write whose flush notifies OnError", func() error {
				return logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write",
				})
			})
			if !finished {
				t.Fatalf("the flush deadlocked (OnError observed flushMu held: %t)", heldDuringNotification.Load())
			}
			t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })
			require.ErrorIs(t, err, ErrClientNotInitialized)
			require.False(t, heldDuringNotification.Load(), "OnError must run after the flush released flushMu")
			probe.RequireCounts(t, mode, 1)
		})
	}
}
