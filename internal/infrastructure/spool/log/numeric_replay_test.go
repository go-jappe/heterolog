package logspool

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	chbackend "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	chsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/clickhouse"
	ossender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func numericReplayFields() map[string]any {
	return map[string]any{
		"event_id":     int64(9007199254740993),
		"max_unsigned": uint64(18446744073709551615),
		"min_signed":   int64(-9223372036854775808),
		"nested":       map[string]any{"values": []any{json.Number("0.12345678901234567890123456789"), true, nil, "unchanged"}},
	}
}

// Decode observed wire data without rounding in the test oracle itself.
func decodeNumericReplayRows(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var rows []map[string]any
	for {
		var row map[string]any
		err := decoder.Decode(&row)
		if err == io.EOF {
			return rows
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}
}

func requireExactReplayFields(t *testing.T, fields any) {
	t.Helper()
	require.Equal(t, map[string]any{
		"event_id":     json.Number("9007199254740993"),
		"max_unsigned": json.Number("18446744073709551615"),
		"min_signed":   json.Number("-9223372036854775808"),
		"nested":       map[string]any{"values": []any{json.Number("0.12345678901234567890123456789"), true, nil, "unchanged"}},
	}, fields)
}

func TestClickHouseSpoolReplayPreservesNumericPayloadAndTokenOnWire(t *testing.T) {
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	row := clickhouse.LogRecord{Timestamp: now, Level: "INFO", Message: "preserve exact values", Fields: numericReplayFields()}
	var mu sync.Mutex
	var bodies [][]byte
	var tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		bodies = append(bodies, body)
		tokens = append(tokens, r.URL.Query().Get("insert_deduplication_token"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := clickhouse.New(clickhouse.Config{Endpoint: server.URL, Table: "logs"})
	require.NoError(t, err)
	_, err = client.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{row}, clickhouse.InsertOptions{InsertDeduplicationToken: "stable-token"})
	require.NoError(t, err)
	payload, err := json.Marshal(chbackend.DurablePayload{LogRecord: row, InsertDeduplicationToken: "stable-token", RecordID: "record-1", ReplayMode: chbackend.DurableReplayModeBatch})
	require.NoError(t, err)
	sender, err := chsender.NewClickHouseSpoolSender(fixedSpoolClock{now: now}, client, 1024*1024)
	require.NoError(t, err)
	for range 2 {
		report, err := sender.Send(context.Background(), []spool.SpoolRecord{{OccurredAt: now, RequestID: "request-1", Payload: payload}})
		require.NoError(t, err)
		require.NoError(t, report.Validate(1))
		require.Equal(t, spool.RecordOutcomeStatusSuccess, report[0].Status)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"stable-token", "stable-token", "stable-token"}, tokens)
	require.Len(t, bodies, 3)
	initial := decodeNumericReplayRows(t, bodies[0])
	require.Len(t, initial, 1)
	requireExactReplayFields(t, initial[0]["fields"])
	for _, body := range bodies[1:] {
		require.Equal(t, initial, decodeNumericReplayRows(t, body))
	}
}

func TestOpenSearchSpoolReplayPreservesNumericPayloadAndIdentityOnWire(t *testing.T) {
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"errors":false,"items":[{"index":{"_index":"logs","_id":"stable-id","status":201,"result":"created"}}]}`)
	}))
	defer server.Close()
	client, err := opensearch.New(opensearch.Config{Endpoint: server.URL, AuthConfig: opensearch.AuthConfig{Type: opensearch.AuthTypeNone}})
	require.NoError(t, err)
	document := opensearch.DocumentWriteRequest{Index: "logs", ID: "stable-id", Op: opensearch.WriteOpIndex, Doc: map[string]any{"message": "preserve exact values", "attributes": map[string]any{"fields": numericReplayFields()}}}
	_, err = client.IndexDocuments(context.Background(), []opensearch.DocumentWriteRequest{document})
	require.NoError(t, err)
	payload, err := json.Marshal(document)
	require.NoError(t, err)
	sender, err := ossender.NewOpensearchSpoolSender(fixedSpoolClock{now: now}, client, 1024*1024)
	require.NoError(t, err)
	for range 2 {
		report, err := sender.Send(context.Background(), []spool.SpoolRecord{{OccurredAt: now, RequestID: "request-1", Payload: payload}})
		require.NoError(t, err)
		require.NoError(t, report.Validate(1))
		require.Equal(t, spool.RecordOutcomeStatusSuccess, report[0].Status)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 3)
	initial := decodeNumericReplayRows(t, bodies[0])
	require.Len(t, initial, 2)
	require.Equal(t, map[string]any{"index": map[string]any{"_index": "logs", "_id": "stable-id"}}, initial[0])
	requireExactReplayFields(t, initial[1]["attributes"].(map[string]any)["fields"])
	for _, body := range bodies[1:] {
		require.Equal(t, initial, decodeNumericReplayRows(t, body))
	}
}
