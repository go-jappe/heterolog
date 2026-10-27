package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// 実際のfactoryとHTTP clientを通して、application層のinterfaceを動かします。
// test自身が作成したspanを観測しても、logが送信されたことは証明できません。
func TestLoggingApplicationPayloadAndTraceReachBackend(t *testing.T) {
	backend := newApplicationLogBackend(t)
	cfg := applicationHTTPLoggerConfig(backend.server.URL)
	cfg.OpenSearch.NoBulkFlushModeEnabled = true
	cfg.OpenSearch.MaxBulkDocument = 1
	logger := newApplicationHTTPLogger(t, cfg)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	fields := []pkglogger.Field{
		pkglogger.WithRequestID("request-42"),
		pkglogger.MustFieldKey("attempt").Int(2),
	}
	require.NoError(t, logger.Info(ctx, "application information", fields...))
	require.NoError(t, logger.ErrorWithStack(ctx, "application failure", errors.New("database unavailable"), fields...))

	docs := backend.documents()
	require.Len(t, docs, 2, "both application calls must reach the backend")
	for _, doc := range docs {
		require.Equal(t, sc.TraceID().String(), doc["trace_id"])
		require.Equal(t, sc.SpanID().String(), doc["span_id"])
		require.Equal(t, "request-42", doc["request_id"])
		require.JSONEq(t, `{"attempt":2}`, doc["fields"].(string))
	}
	require.Equal(t, "application information", docs[0]["message"])
	require.Equal(t, pkglogger.LogLevelInfo.String(), docs[0]["level"])
	require.Equal(t, "application failure", docs[1]["message"])
	require.Equal(t, pkglogger.LogLevelError.String(), docs[1]["level"])
	require.Equal(t, "database unavailable", docs[1]["error"])
	require.NotEmpty(t, docs[1]["stacktrace"])
	require.NotEmpty(t, docs[1]["stack_trace"])
}

func TestLoggingCloseFlushesAcceptedMessagesAndRejectsFurtherWrites(t *testing.T) {
	backend := newApplicationLogBackend(t)
	logger := newApplicationHTTPLogger(t, applicationHTTPLoggerConfig(backend.server.URL))
	ctx := context.Background()
	require.NoError(t, logger.Info(ctx, "accepted first"))
	require.NoError(t, logger.Warn(ctx, "accepted second"))
	require.Empty(t, backend.documents(), "below-threshold messages must still be buffered")

	require.NoError(t, logger.Close())
	docs := backend.documents()
	require.Len(t, docs, 2, "Close must send every accepted record")
	require.Equal(t, "accepted first", docs[0]["message"])
	require.Equal(t, "accepted second", docs[1]["message"])
	require.NoError(t, logger.Close())
	require.ErrorIs(t, logger.Info(ctx, "after close"), pkglogger.ErrLoggerClosed)
	require.Len(t, backend.documents(), 2, "Close and rejected writes must not resend records")
}

func TestLoggingDirectSendReturnsBackendFailure(t *testing.T) {
	backend := newApplicationLogBackend(t)
	backend.reject.Store(true)
	cfg := applicationHTTPLoggerConfig(backend.server.URL)
	cfg.OpenSearch.NoBulkFlushModeEnabled = true
	cfg.OpenSearch.MaxBulkDocument = 1
	logger := newApplicationHTTPLogger(t, cfg)
	err := logger.Info(context.Background(), "rejected by backend")
	require.Error(t, err, "a direct-send logger cannot report success when OpenSearch rejects the record")
	// 退避先がない構成でも、durable処理のerrorだけでなくOpenSearchの拒否そのものを判別できます。
	var statusErr *pkgopensearch.HTTPStatusError
	require.ErrorAs(t, err, &statusErr, "the returned error must keep the backend rejection")
	require.Equal(t, http.StatusForbidden, statusErr.Code)
	docs := backend.documents()
	require.Len(t, docs, 1)
	require.Equal(t, "rejected by backend", docs[0]["message"])
}

func applicationHTTPLoggerConfig(endpoint string) pkglogger.LoggerConfig {
	cfg := durableOpenSearchLoggerConfig(endpoint, durableTestDirs{}, []durable.DurableType{durable.DurableTypeNone})
	cfg.OpenSearch.MaxBulkDocument = 100 // timerとsizeのtriggerでは、壊れたCloseを隠せません。
	return cfg
}

func newApplicationHTTPLogger(t *testing.T, cfg pkglogger.LoggerConfig) applogging.CloseableLogger {
	t.Helper()
	logger, err := infralogger.NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	closeable, ok := logger.(applogging.CloseableLogger)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, closeable.Close()) })
	return closeable
}

type applicationLogBackend struct {
	server *httptest.Server
	reject atomic.Bool
	mu     sync.Mutex
	docs   []map[string]any
}

func newApplicationLogBackend(t *testing.T) *applicationLogBackend {
	t.Helper()
	backend := &applicationLogBackend{}
	backend.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			http.Error(w, "read body", 400)
			return
		}
		var docs []map[string]any
		var items []map[string]any
		if r.URL.Path == "/_bulk" {
			if r.Method != http.MethodPost {
				t.Errorf("bulk method = %s", r.Method)
			}
			lines := bytes.Split(bytes.TrimSpace(body), []byte{'\n'})
			if len(lines)%2 != 0 {
				t.Error("bulk request has unmatched metadata/document")
				http.Error(w, "invalid bulk", 400)
				return
			}
			for i := 0; i < len(lines); i += 2 {
				var meta map[string]map[string]string
				var doc map[string]any
				if err := json.Unmarshal(lines[i], &meta); err != nil {
					t.Error(err)
					http.Error(w, "invalid metadata", 400)
					return
				}
				if err := json.Unmarshal(lines[i+1], &doc); err != nil {
					t.Error(err)
					http.Error(w, "invalid document", 400)
					return
				}
				fields, ok := meta["create"]
				if !ok || fields["_index"] != "logs-integration" || fields["_id"] == "" {
					t.Errorf("invalid create metadata: %v", meta)
				}
				docs = append(docs, doc)
				items = append(items, map[string]any{"create": map[string]any{"_index": fields["_index"], "_id": fields["_id"], "status": 201, "result": "created", "_version": 1}})
			}
		} else {
			if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/logs-integration/_create/") {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			var doc map[string]any
			if err := json.Unmarshal(body, &doc); err != nil {
				t.Error(err)
				http.Error(w, "invalid document", 400)
				return
			}
			docs = append(docs, doc)
		}
		backend.mu.Lock()
		backend.docs = append(backend.docs, docs...)
		backend.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if backend.reject.Load() {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"type":"security_exception","reason":"test rejection"},"status":403}`)
		} else if items != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": false, "items": items})
		} else {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"_index": "logs-integration", "_id": r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], "_version": 1, "result": "created", "status": 201})
		}
	}))
	t.Cleanup(backend.server.Close)
	return backend
}

func (b *applicationLogBackend) documents() []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]map[string]any(nil), b.docs...)
}
