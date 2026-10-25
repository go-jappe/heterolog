package loki

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestPushPreservesAllStreamsAndEntriesOnWire(t *testing.T) {
	for _, compress := range []bool{false, true} {
		name := "plain"
		if compress {
			name = "gzip"
		}
		t.Run(name, func(t *testing.T) {
			first := time.Unix(1_700_000_000, 123)
			streams := []Stream{
				{Labels: map[string]string{"app": "alpha", "level": "info"}, Entries: []Entry{
					{Timestamp: first, Line: "first: 日本語"},
					{Timestamp: first.Add(time.Second), Line: "second: line one\nline two"},
				}},
				{Labels: map[string]string{"app": "beta", "region": "east"}, Entries: []Entry{
					{Timestamp: first.Add(2 * time.Second), Line: `third: "quoted"`},
				}},
				{Labels: map[string]string{"app": "gamma", "level": "warn"}, Entries: []Entry{
					{Timestamp: first.Add(3 * time.Second), Line: "fourth"},
					{Timestamp: first.Add(4 * time.Second), Line: "fifth"},
				}},
			}
			type receivedResult struct {
				payload receivedPush
				err     error
			}
			results := make(chan receivedResult, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				var body io.Reader = request.Body
				if compress {
					if got := request.Header.Get("Content-Encoding"); got != "gzip" {
						t.Errorf("Content-Encoding = %q, want gzip", got)
					}
					reader, err := gzip.NewReader(request.Body)
					if err != nil {
						results <- receivedResult{err: err}
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					defer reader.Close()
					body = reader
				} else if got := request.Header.Get("Content-Encoding"); got != "" {
					t.Errorf("Content-Encoding = %q, want uncompressed JSON", got)
				}
				var result receivedResult
				result.err = json.NewDecoder(body).Decode(&result.payload)
				results <- result
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client, err := New(Config{Endpoint: server.URL, CompressRequest: compress})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Push(context.Background(), streams); err != nil {
				t.Fatal(err)
			}
			got := <-results
			if got.err != nil {
				t.Fatal(got.err)
			}
			if len(got.payload.Streams) != len(streams) {
				t.Fatalf("transmitted streams = %d, want %d", len(got.payload.Streams), len(streams))
			}
			wantByApp := make(map[string]Stream, len(streams))
			for _, stream := range streams {
				wantByApp[stream.Labels["app"]] = stream
			}
			for i, actual := range got.payload.Streams {
				app := actual.Stream["app"]
				want, exists := wantByApp[app]
				if !exists {
					t.Fatalf("stream %d has unexpected or repeated app label %q", i, app)
				}
				delete(wantByApp, app)
				if !reflect.DeepEqual(actual.Stream, want.Labels) {
					t.Errorf("stream %d labels = %v, want %v", i, actual.Stream, want.Labels)
				}
				if len(actual.Values) != len(want.Entries) {
					t.Fatalf("stream %d entries = %d, want %d", i, len(actual.Values), len(want.Entries))
				}
				for j, entry := range want.Entries {
					assertValue(t, actual.Values[j], entry.Timestamp, entry.Line)
				}
			}
		})
	}
}
