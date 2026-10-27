package clickhouse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientInsertLogsPreservesEveryRowInRequest(t *testing.T) {
	for _, compress := range []bool{false, true} {
		name := "plain"
		if compress {
			name = "gzip"
		}
		t.Run(name, func(t *testing.T) {
			firstTime := time.Date(2026, 6, 25, 12, 34, 56, 123_000_000, time.UTC)
			records := []LogRecord{
				{
					Timestamp: firstTime,
					Level:     "INFO",
					Message:   "first: 日本語",
					RequestID: "request-1",
					TraceID:   "trace-1",
					Fields:    map[string]any{"item": "book", "quantity": float64(2)},
				},
				{
					Timestamp: firstTime.Add(time.Second),
					Level:     "WARN",
					Message:   "second: line one\nline two",
					RequestID: "request-2",
					SpanID:    "span-2",
					Fields:    map[string]any{"item": "pen", "available": false},
				},
				{
					Timestamp: firstTime.Add(2 * time.Second),
					Level:     "ERROR",
					Message:   `third: "quoted" \\ tail`,
					RequestID: "request-3",
					Error:     "failure-3",
					Fields:    map[string]any{"item": "paper", "quantity": float64(5)},
				},
			}
			var gotRecords []LogRecord
			requestCount := 0
			client, err := New(Config{
				Endpoint:        "http://clickhouse.example.test:8123",
				Table:           "app_logs",
				CompressRequest: compress,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					requestCount++
					var body io.Reader = request.Body
					if compress {
						require.Equal(t, "gzip", request.Header.Get("Content-Encoding"))
						reader, err := gzip.NewReader(request.Body)
						require.NoError(t, err)
						defer reader.Close()
						body = reader
					} else {
						require.Empty(t, request.Header.Get("Content-Encoding"))
					}
					// serialization helperだけを試すのではなく、message内でescapeされた改行も
					// 含めて、送信されたJSONEachRowの各行をdecodeします。
					scanner := bufio.NewScanner(body)
					for scanner.Scan() {
						var record LogRecord
						require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
						gotRecords = append(gotRecords, record)
					}
					require.NoError(t, scanner.Err())
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader("")),
					}, nil
				})},
			})
			require.NoError(t, err)

			_, err = client.InsertLogs(context.Background(), records)
			require.NoError(t, err)
			require.Equal(t, 1, requestCount)
			require.Equal(t, records, gotRecords, "HTTP payload must retain each row's content and input order")
		})
	}
}
