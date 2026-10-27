package cloudwatchlogs

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPutLogEventsMeasuresJSONDecodedMessageAtBatchBoundary(t *testing.T) {
	// AWSは、JSON decode後に受け取るUTF-8のmessageに、eventごとの26 byteを加えて数えます。
	// JSONのescape自体は、このmessageの上限枠を消費しません。
	const messageLimit = 1048576 - 26
	for _, compressed := range []bool{false, true} {
		for _, tc := range []struct{ name, inputPrefix, receivedPrefix string }{
			{"ASCII", "ab", "ab"},
			{"multibyte", "日本😀", "日本😀"},
			{"invalid UTF8", "\xff\xc3", "\ufffd\ufffd"},
			{"JSON escapes", "\n\"<", "\n\"<"},
		} {
			t.Run(fmt.Sprintf("%s/gzip=%v", tc.name, compressed), func(t *testing.T) {
				padding := strings.Repeat("a", messageLimit-len(tc.receivedPrefix))
				message := tc.inputPrefix + padding
				want := tc.receivedPrefix + padding
				calls := 0
				client := newMessageSizeClient(t, compressed, func(r *http.Request) {
					calls++
					payload := decodeMessageSizeRequest(t, r, compressed)
					if len(payload.LogEvents) != 1 {
						t.Fatalf("received %d events, want 1", len(payload.LogEvents))
					}
					got := payload.LogEvents[0]
					if got.Message != want || got.Timestamp != 10000 {
						t.Fatalf("received message bytes=%d timestamp=%d, want exact normalized message (%d bytes) timestamp=10000", len(got.Message), got.Timestamp, len(want))
					}
				})
				request := PutLogEventsRequest{LogStreamName: "stream", AttemptedAt: time.Unix(20, 0), Events: []Event{{Timestamp: time.Unix(10, 0), Message: message}}}
				if _, err := client.PutLogEvents(context.Background(), request); err != nil {
					t.Fatalf("exact boundary: %v", err)
				}
				request.Events[0].Message += "x"
				if _, err := client.PutLogEvents(context.Background(), request); err == nil {
					t.Fatal("one decoded byte over the batch limit was accepted")
				}
				if calls != 1 {
					t.Fatalf("HTTP calls=%d, want only the exact-boundary request", calls)
				}
			})
		}
	}
}

func TestSplitPutLogEventsBatchesFitsJSONDecodedMessages(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%v", compressed), func(t *testing.T) {
			events := []Event{
				{Timestamp: time.Unix(10, 0), Message: strings.Repeat("\xff", 120000)},
				{Timestamp: time.Unix(11, 0), Message: strings.Repeat("a", 700000)},
			}
			want := []string{strings.Repeat("\ufffd", 120000), events[1].Message}
			batches, err := SplitPutLogEventsBatches(events)
			if err != nil {
				t.Fatal(err)
			}
			if len(batches) != 2 {
				t.Fatalf("batches=%d, want 2 for 1,060,052 decoded bytes", len(batches))
			}
			received := 0
			client := newMessageSizeClient(t, compressed, func(r *http.Request) {
				payload := decodeMessageSizeRequest(t, r, compressed)
				total := 0
				for _, event := range payload.LogEvents {
					total += len(event.Message) + 26
					if received >= len(want) || event.Message != want[received] || event.Timestamp != events[received].Timestamp.UnixMilli() {
						t.Fatalf("received event %d lost content or order (bytes=%d timestamp=%d)", received, len(event.Message), event.Timestamp)
					}
					received++
				}
				if total > 1048576 {
					t.Fatalf("wire message budget=%d, exceeds 1 MiB", total)
				}
			})
			for _, batch := range batches {
				if _, err := client.PutLogEvents(context.Background(), PutLogEventsRequest{LogStreamName: "stream", AttemptedAt: time.Unix(20, 0), Events: batch}); err != nil {
					t.Fatal(err)
				}
			}
			if received != len(want) {
				t.Fatalf("received=%d, want %d", received, len(want))
			}
		})
	}
}

type messageSizePayload struct {
	LogEvents []struct {
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	} `json:"logEvents"`
}

func decodeMessageSizeRequest(t *testing.T, r *http.Request, compressed bool) messageSizePayload {
	t.Helper()
	var body io.Reader = r.Body
	wantEncoding := ""
	if compressed {
		wantEncoding = "gzip"
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		defer gz.Close()
		body = gz
	}
	if got := r.Header.Get("Content-Encoding"); got != wantEncoding {
		t.Fatalf("Content-Encoding=%q, want %q", got, wantEncoding)
	}
	var payload messageSizePayload
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func newMessageSizeClient(t *testing.T, compressed bool, receive func(*http.Request)) *HTTPClient {
	t.Helper()
	client, err := New(Config{
		Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "logs",
		AccessKeyID: "test", SecretAccessKey: "test", CompressRequest: compressed,
		HTTPClient: &http.Client{Transport: uncertainResponseTransport(func(r *http.Request) (*http.Response, error) {
			receive(r)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
