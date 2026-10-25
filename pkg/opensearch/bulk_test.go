package opensearch

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBulkRequestSizeMatchesIndexDocumentsNDJSONBody(t *testing.T) {
	t.Parallel()

	documents := []DocumentWriteRequest{
		{
			Index: "logs-2026",
			ID:    "doc-1",
			Doc: map[string]any{
				"level":   "info",
				"message": "一件目",
			},
			Op: WriteOpIndex,
		},
		{
			Index: "logs-2026",
			ID:    "doc-2",
			Doc: map[string]any{
				"level":   "warn",
				"message": "second",
			},
			Op: WriteOpCreate,
		},
	}
	wantBody := "" +
		`{"index":{"_id":"doc-1","_index":"logs-2026"}}` + "\n" +
		`{"level":"info","message":"一件目"}` + "\n" +
		`{"create":{"_id":"doc-2","_index":"logs-2026"}}` + "\n" +
		`{"level":"warn","message":"second"}` + "\n"

	for _, compress := range []bool{false, true} {
		body := sentBulkRequestBody(t, compress, documents)
		if got := string(body); got != wantBody {
			t.Fatalf("compress=%t IndexDocuments() sent %q, want %q", compress, got, wantBody)
		}

		size, err := BulkRequestSize(documents)
		if err != nil {
			t.Fatalf("BulkRequestSize() error = %v", err)
		}
		// BulkRequestSizeは圧縮の有無にかかわらず、gzip圧縮前の送信bodyを数えます。
		if size != len(body) {
			t.Fatalf("compress=%t BulkRequestSize() = %d, sent body length = %d", compress, size, len(body))
		}
	}
}

func TestBulkRequestSizeIncludesBothTrailingNewlinesForSingleDocument(t *testing.T) {
	t.Parallel()

	document := DocumentWriteRequest{
		Index: "logs",
		ID:    "doc-1",
		Doc:   map[string]any{"message": "hello"},
		Op:    WriteOpIndex,
	}
	body := sentBulkRequestBody(t, false, []DocumentWriteRequest{document})
	if got := bytes.Count(body, []byte{'\n'}); got != 2 {
		t.Fatalf("sent bulk body newline count = %d, want 2; body = %q", got, body)
	}
	if body[len(body)-1] != '\n' {
		t.Fatalf("sent bulk body = %q, want final newline", body)
	}

	size, err := BulkRequestSize([]DocumentWriteRequest{document})
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	if size != len(body) {
		t.Fatalf("BulkRequestSize() = %d, sent body length = %d", size, len(body))
	}
}

// sentBulkRequestBodyは、IndexDocumentsがHTTP serverへ実際に送信したbodyを返します。
// 圧縮送信ではContent-Encodingを確認し、BulkRequestSizeと比べられるよう展開します。
func sentBulkRequestBody(t *testing.T, compress bool, documents []DocumentWriteRequest) []byte {
	t.Helper()

	type sentRequest struct {
		body            []byte
		contentEncoding string
		readErr         error
	}
	requests := make(chan sentRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		requests <- sentRequest{body: body, contentEncoding: r.Header.Get("Content-Encoding"), readErr: err}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"took":1,"errors":false,"items":[]}`)
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint:        server.URL,
		Timeout:         5 * time.Second,
		CompressRequest: compress,
		AuthConfig:      AuthConfig{Type: AuthTypeNone},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := client.IndexDocuments(context.Background(), documents); err != nil {
		t.Fatalf("IndexDocuments() error = %v", err)
	}

	var sent sentRequest
	select {
	case sent = <-requests:
	default:
		t.Fatal("IndexDocuments() returned without sending a bulk request")
	}
	if sent.readErr != nil {
		t.Fatalf("read bulk request body: %v", sent.readErr)
	}
	if !compress {
		if sent.contentEncoding != "" {
			t.Fatalf("Content-Encoding = %q, want uncompressed request", sent.contentEncoding)
		}
		return sent.body
	}
	if sent.contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", sent.contentEncoding)
	}
	assertGzipHeader(t, sent.body)
	return readGzipBody(t, sent.body)
}
