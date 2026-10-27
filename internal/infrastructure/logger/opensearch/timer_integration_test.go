package opensearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/stretchr/testify/require"
)

func TestTimerBasedFlush(t *testing.T) {
	received := make(chan map[string]any, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/test-index/_create/") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var doc map[string]any
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			t.Error(err)
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		received <- doc
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"_index": "test-index", "_id": r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:],
			"_version": 1, "result": "created", "status": 201,
		})
	}))
	defer server.Close()
	tracer, meter := observer.NopTracer{}, observer.NopMeter{}
	client, err := infraopensearch.NewClient(infraopensearch.NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
		Endpoint: server.URL, Timeout: time.Second, AuthConfig: pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone},
	}), nil, tracer, meter)
	require.NoError(t, err)
	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = 10 * time.Millisecond
	cfg.maxBulkDocument = 100
	logger, err := NewOpenSearchLogger(context.Background(), cfg, client, cfg, NewMockResourceProvider(), nil, tracer, meter, &LoggerBase{Clock: clock.NewUTCClock(), Tracer: tracer, Meter: meter})
	require.NoError(t, err)
	defer func() { require.NoError(t, logger.Close(context.Background())) }()
	require.NoError(t, logger.testInfo(context.Background(), "sent by the timer"))
	// Closeの前に確認します。shutdown時のflushで、壊れたtimer経路を隠してはいけません。
	select {
	case doc := <-received:
		require.Equal(t, "sent by the timer", doc["message"])
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("the interval elapsed without delivering the buffered record")
	}
	require.NoError(t, logger.Close(context.Background()))
	select {
	case doc := <-received:
		t.Fatalf("already delivered record was sent again during Close: %v", doc)
	default:
	}
}
