package opensearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/opaqueid"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNewUsesDefaultTimeoutOnlyWhenTimeoutIsZero(t *testing.T) {
	client, err := New(Config{
		Endpoint:   "http://localhost:9200",
		AuthConfig: AuthConfig{Type: AuthTypeNone},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	realClient := client.(*realClient)
	if got, want := realClient.httpClient.Timeout, defaultTimeout; got != want {
		t.Fatalf("httpClient.Timeout = %s, want %s", got, want)
	}

	_, err = New(Config{
		Endpoint:   "http://localhost:9200",
		Timeout:    -time.Second,
		AuthConfig: AuthConfig{Type: AuthTypeNone},
	})
	if !errors.Is(err, ErrNegativeTimeout) {
		t.Fatalf("New() error = %v, want %v", err, ErrNegativeTimeout)
	}
}

func TestIndexDocument(t *testing.T) {
	// コンテキスト
	ctx := context.Background()

	// テストサーバを使ってOpenSearchのモックを作成
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"message"`)) {
			t.Errorf("unexpected body: %s", body)
		}
		// X-Opaque-Idヘッダーがないことを確認
		if r.Header.Get("X-Opaque-Id") != "" {
			t.Errorf("unexpected X-Opaque-Id header: %s", r.Header.Get("X-Opaque-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		w.Write([]byte(`{"_index":"test-test-index","_id":"testopenseach1234","_version":1,"result":"created"}`))
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint: ts.URL,
		Timeout:  3 * time.Second,
		AuthConfig: AuthConfig{
			Type:   AuthTypeNone,
			Config: nil,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	doc := DocumentWriteRequest{
		Index: "test-index",
		ID:    "testopenseach1234",
		Op:    WriteOpIndex,
		Doc:   map[string]any{"message": "test"},
	}

	resp, err := client.IndexDocument(ctx, doc)
	if err != nil {
		t.Fatalf("indexing failed: %v", err)
	}

	// レスポンスの検証
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Result != "created" {
		t.Errorf("expected result 'created', got '%s'", resp.Result)
	}
	if resp.Index != "test-test-index" {
		t.Errorf("expected index 'test-test-index', got '%s'", resp.Index)
	}
	if resp.ID != doc.ID {
		t.Errorf("expected ID %q, got %q", doc.ID, resp.ID)
	}
	if resp.Version != 1 {
		t.Errorf("expected version 1, got %d", resp.Version)
	}
}

func TestHTTPStatusErrorsPreserveRetryAfter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint: ts.URL,
		AuthConfig: AuthConfig{
			Type: AuthTypeNone,
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	assertRetryAfter := func(t *testing.T, err error) {
		t.Helper()
		var statusErr *HTTPStatusError
		if !errors.As(err, &statusErr) {
			t.Fatalf("error = %T, want HTTPStatusError", err)
		}
		if got, want := statusErr.RetryAfter(), 2*time.Second; got != want {
			t.Fatalf("RetryAfter() = %s, want %s", got, want)
		}
	}

	_, err = client.IndexDocument(context.Background(), DocumentWriteRequest{
		Index: "logs",
		ID:    "single",
		Op:    WriteOpIndex,
		Doc:   map[string]any{"message": "single"},
	})
	assertRetryAfter(t, err)

	_, err = client.IndexDocuments(context.Background(), []DocumentWriteRequest{{
		Index: "logs",
		ID:    "bulk",
		Op:    WriteOpIndex,
		Doc:   map[string]any{"message": "bulk"},
	}})
	assertRetryAfter(t, err)
}

func TestSuccessfulHTTPResponseDecodeErrorsPreserveOperation(t *testing.T) {
	tests := []struct {
		name          string
		wantOperation string
		send          func(context.Context, Client) error
	}{
		{
			name:          "index response",
			wantOperation: "index",
			send: func(ctx context.Context, client Client) error {
				_, err := client.IndexDocument(ctx, DocumentWriteRequest{
					Index: "logs",
					ID:    "document-1",
					Doc:   map[string]any{"message": "test"},
					Op:    WriteOpIndex,
				})
				return err
			},
		},
		{
			name:          "bulk response",
			wantOperation: "bulk",
			send: func(ctx context.Context, client Client) error {
				_, err := client.IndexDocuments(ctx, []DocumentWriteRequest{{
					Index: "logs",
					ID:    "document-1",
					Doc:   map[string]any{"message": "test"},
					Op:    WriteOpIndex,
				}})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("{"))
			}))
			defer ts.Close()

			client, err := New(Config{
				Endpoint: ts.URL,
				AuthConfig: AuthConfig{
					Type: AuthTypeNone,
				},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			err = tt.send(context.Background(), client)
			var decodeErr *ResponseDecodeError
			if !errors.As(err, &decodeErr) {
				t.Fatalf("error = %T (%v), want ResponseDecodeError", err, err)
			}
			if decodeErr.Operation != tt.wantOperation {
				t.Fatalf("Operation = %q, want %q", decodeErr.Operation, tt.wantOperation)
			}
			if decodeErr.Err == nil {
				t.Fatal("Err = nil, want concrete JSON decode error")
			}
		})
	}
}

// time.Durationで表せないRetry-Afterは、HTTP-dateと同じく最大値へ飽和させ、呼び出し側の
// 待機上限を適用させます。0にすると、長い待機を求めたserverへ即時再送してしまいます。
func TestParseRetryAfterSaturatesDurationOverflow(t *testing.T) {
	const maxDelay = time.Duration(1<<63 - 1)
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{value: "9223372036", want: 9223372036 * time.Second},
		{value: "9223372037", want: maxDelay},
		{value: "99999999999999999999", want: maxDelay},
		{value: "Fri, 31 Dec 9999 23:59:59 GMT", want: maxDelay},
		{value: "12a", want: 0},
		{value: "-1", want: 0},
	} {
		resp := &http.Response{Header: http.Header{"Retry-After": []string{tt.value}}}
		if got := parseRetryAfter(resp); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tt.value, got, tt.want)
		}
	}
}

func TestWithOpaqueID(t *testing.T) {
	// コンテキスト
	ctx := opaqueid.WithOpaqueID(context.Background(), "test-request-id")
	// テストサーバを使ってOpenSearchのモックを作成
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"message"`)) {
			t.Errorf("unexpected body: %s", body)
		}
		// X-Opaque-Idヘッダーが期待通りの値であることを確認
		if r.Header.Get("X-Opaque-Id") != "test-request-id" {
			t.Errorf("expected X-Opaque-Id header 'test-request-id', got '%s'", r.Header.Get("X-Opaque-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		w.Write([]byte(`{"_index":"test-test-index","_id":"testopenseach1234","_version":1,"result":"created"}`))
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint: ts.URL,
		Timeout:  3 * time.Second,
		AuthConfig: AuthConfig{
			Type:   AuthTypeNone,
			Config: nil,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	doc := DocumentWriteRequest{
		Index: "test-index",
		ID:    "testopenseach1234",
		Op:    WriteOpIndex,
		Doc:   map[string]any{"message": "test"},
	}

	resp, err := client.IndexDocument(ctx, doc)
	if err != nil {
		t.Fatalf("indexing failed: %v", err)
	}

	// レスポンスの検証
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Result != "created" {
		t.Errorf("expected result 'created', got '%s'", resp.Result)
	}
}

func TestWithOpaqueID_BulkRequest(t *testing.T) {
	// コンテキスト
	ctx := opaqueid.WithOpaqueID(context.Background(), "bulk-request-id")

	// テストサーバを使ってOpenSearchのモックを作成
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"message"`)) {
			t.Errorf("unexpected body: %s", body)
		}
		// X-Opaque-Idヘッダーが期待通りの値であることを確認
		if r.Header.Get("X-Opaque-Id") != "bulk-request-id" {
			t.Errorf("expected X-Opaque-Id header 'bulk-request-id', got '%s'", r.Header.Get("X-Opaque-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{
			"took": 30,
			"errors": false,
			"items": [
				{
					"index": {
						"_index": "test-test-index",
						"_id": "bulk1",
						"_version": 1,
						"result": "created",
						"status": 201
					}
				},
				{
					"index": {
						"_index": "test-test-index",
						"_id": "bulk2",
						"_version": 1,
						"result": "created",
						"status": 201
					}
				}
			]
		}`))
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint: ts.URL,
		Timeout:  3 * time.Second,
		AuthConfig: AuthConfig{
			Type:   AuthTypeNone,
			Config: nil,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	docs := []DocumentWriteRequest{
		{
			Index: "test-index",
			ID:    "testopenseach1234",
			Op:    WriteOpIndex,
			Doc:   map[string]any{"message": "test2"},
		},
		{
			Index: "test-index",
			ID:    "testopenseach1235",
			Op:    WriteOpIndex,
			Doc:   map[string]any{"message": "test4"},
		},
	}

	resp, err := client.IndexDocuments(ctx, docs)
	if err != nil {
		t.Fatalf("bulk indexing failed: %v", err)
	}

	// レスポンスの検証
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Errors {
		t.Error("expected no errors, got errors")
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
}

func TestIndexDocuments(t *testing.T) {
	// コンテキスト
	ctx := context.Background()

	// テストサーバを使ってOpenSearchのモックを作成
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"message"`)) {
			t.Errorf("unexpected body: %s", body)
		}
		// X-Opaque-Idヘッダーがないことを確認
		if r.Header.Get("X-Opaque-Id") != "" {
			t.Errorf("unexpected X-Opaque-Id header: %s", r.Header.Get("X-Opaque-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{
			"took": 30,
			"errors": false,
			"items": [
				{
					"index": {
						"_index": "test-test-index",
						"_id": "bulk1",
						"_version": 1,
						"result": "created",
						"status": 201
					}
				},
				{
					"index": {
						"_index": "test-test-index",
						"_id": "bulk2",
						"_version": 1,
						"result": "created",
						"status": 201
					}
				}
			]
		}`))
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint: ts.URL,
		Timeout:  3 * time.Second,
		AuthConfig: AuthConfig{
			Type:   AuthTypeNone,
			Config: nil,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	docs := []DocumentWriteRequest{
		{
			Index: "test-index",
			ID:    "testopenseach1236",
			Op:    WriteOpIndex,
			Doc:   map[string]any{"message": "test2"},
		},
		{
			Index: "test-index",
			ID:    "testopenseach1237",
			Op:    WriteOpIndex,
			Doc:   map[string]any{"message": "test4"},
		},
	}
	resp, err := client.IndexDocuments(ctx, docs)
	if err != nil {
		t.Fatalf("bulk indexing failed: %v", err)
	}

	// レスポンスの検証
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Errors {
		t.Error("expected no errors, got errors")
	}
	if resp.Took != 30 {
		t.Errorf("expected took 30, got %d", resp.Took)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
	if resp.Items[0]["index"].Result != "created" {
		t.Errorf("expected first item result 'created', got '%s'", resp.Items[0]["index"].Result)
	}
	if resp.Items[1]["index"].Result != "created" {
		t.Errorf("expected second item result 'created', got '%s'", resp.Items[1]["index"].Result)
	}
}

func TestIndexDocumentCompressesRequestWhenEnabled(t *testing.T) {
	ctx := context.Background()
	indexName := "app-log-test-index"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := fmt.Sprintf("/%s/_doc/compressed-single", indexName)
		if r.URL.Path != wantPath {
			t.Errorf("expected request path %q, got %q", wantPath, r.URL.Path)
		}
		if got := r.Header.Get("Content-Encoding"); got != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %q", got)
		}

		compressedBody, _ := io.ReadAll(r.Body)
		assertGzipHeader(t, compressedBody)
		body := readGzipBody(t, compressedBody)
		if !bytes.Contains(body, []byte(`"message":"compressed single"`)) {
			t.Errorf("unexpected decompressed body: %s", body)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		w.Write([]byte(`{"_index":"app-log-test-index","_id":"compressed-single","_version":1,"result":"created","status":201}`))
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint:        ts.URL,
		Timeout:         3 * time.Second,
		CompressRequest: true,
		AuthConfig: AuthConfig{
			Type: AuthTypeNone,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	resp, err := client.IndexDocument(ctx, DocumentWriteRequest{
		Index: indexName,
		ID:    "compressed-single",
		Op:    WriteOpIndex,
		Doc:   map[string]any{"message": "compressed single"},
	})
	if err != nil {
		t.Fatalf("indexing failed: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
}

func TestIndexDocumentsCompressesBulkRequestWhenEnabled(t *testing.T) {
	ctx := context.Background()
	indexName := "app-log-test-index"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Encoding"); got != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-ndjson" {
			t.Errorf("expected Content-Type application/x-ndjson, got %q", got)
		}

		compressedBody, _ := io.ReadAll(r.Body)
		assertGzipHeader(t, compressedBody)
		body := readGzipBody(t, compressedBody)
		if !bytes.Contains(body, []byte(`"message":"compressed bulk 1"`)) {
			t.Errorf("unexpected decompressed body: %s", body)
		}
		if !bytes.Contains(body, []byte(`"_index":"app-log-test-index"`)) {
			t.Errorf("expected bulk body to target app-log-test-index, got %s", body)
		}
		if !bytes.HasSuffix(body, []byte("\n")) {
			t.Errorf("expected bulk body to end with newline, got %q", body)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{
			"took": 10,
			"errors": false,
			"items": [
				{"index": {"_index": "app-log-test-index", "_id": "bulk-compressed-1", "_version": 1, "result": "created", "status": 201}},
				{"index": {"_index": "app-log-test-index", "_id": "bulk-compressed-2", "_version": 1, "result": "created", "status": 201}}
			]
		}`))
	}))
	defer ts.Close()

	client, err := New(Config{
		Endpoint:        ts.URL,
		Timeout:         3 * time.Second,
		CompressRequest: true,
		AuthConfig: AuthConfig{
			Type: AuthTypeNone,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	resp, err := client.IndexDocuments(ctx, []DocumentWriteRequest{
		{
			Index: indexName,
			ID:    "bulk-compressed-1",
			Op:    WriteOpIndex,
			Doc:   map[string]any{"message": "compressed bulk 1"},
		},
		{
			Index: indexName,
			ID:    "bulk-compressed-2",
			Op:    WriteOpIndex,
			Doc:   map[string]any{"message": "compressed bulk 2"},
		},
	})
	if err != nil {
		t.Fatalf("bulk indexing failed: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Errors {
		t.Error("expected no errors, got errors")
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
}

func TestIndexDocumentToLocalOpenSearch(t *testing.T) {
	// コンテキスト
	ctx := context.Background()

	client, err := New(openSearchIntegrationConfig(t, false))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	indexName := openSearchIntegrationIndex(t)

	docs := DocumentWriteRequest{
		Index: indexName,
		ID:    integrationDocID("single"),
		Op:    WriteOpIndex,
		Doc: map[string]any{
			"message": "integration test",
			"ts":      time.Now().Format(time.RFC3339),
		},
	}

	resp, err := client.IndexDocument(ctx, docs)
	if err != nil {
		t.Fatalf("failed to index to local OpenSearch: %v", err)
	}

	// 実際のOpenSearchに接続するテストなので、レスポンスの詳細な検証は省略
	// 基本的な検証のみ行う
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Result != "created" && resp.Result != "updated" {
		t.Errorf("expected result 'created' or 'updated', got '%s'", resp.Result)
	}
	// aliasやdata streamへの書き込みでは、_indexは解決後のconcrete/backing indexです。
	// 要求したindex名との一致ではなく、応答が書き込んだdocumentを示すことを確認します。
	if err := resp.Validate(docs); err != nil {
		t.Errorf("index response does not identify the written document: %v", err)
	}
}

// 実OpenSearchテストは OPENSEARCH_INTEGRATION_TEST=true で明示的に有効化する。
func TestIndexDocumentsToLocalOpenSearch(t *testing.T) {
	// コンテキスト
	ctx := context.Background()

	client, err := New(openSearchIntegrationConfig(t, false))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	indexName := openSearchIntegrationIndex(t)

	clock := clock.NewUTCClock()
	firstID := integrationDocID("bulk-1")
	secondID := integrationDocID("bulk-2")

	docs := []DocumentWriteRequest{
		{

			Index: indexName,
			ID:    firstID,
			Op:    WriteOpIndex,
			Doc: map[string]any{
				"message": "integration test bulk 1",
				"ts":      clock.Now(),
			}},
		{

			Index: indexName,
			ID:    secondID,
			Op:    WriteOpIndex,
			Doc: map[string]any{
				"message": "integration test bulk 2",
				"ts":      time.Now().Format(time.RFC3339),
			}},
	}

	resp, err := client.IndexDocuments(ctx, docs)
	if err != nil {
		t.Fatalf("failed to bulk index to local OpenSearch: %v", err)
	}

	// 実際のOpenSearchに接続するテストなので、レスポンスの詳細な検証は省略
	// 基本的な検証のみ行う
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Errors {
		t.Error("expected no errors in bulk operation")
	}
	if len(resp.Items) != 2 {
		t.Errorf("expected 2 items in response, got %d", len(resp.Items))
	}
}

func TestIndexDocumentCompressedToLocalOpenSearch(t *testing.T) {
	ctx := context.Background()

	client, err := New(openSearchIntegrationConfig(t, true))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	indexName := openSearchIntegrationIndex(t)

	document := DocumentWriteRequest{
		Index: indexName,
		ID:    integrationDocID("compressed-single"),
		Op:    WriteOpIndex,
		Doc: map[string]any{
			"message": "compressed integration test",
			"ts":      time.Now().Format(time.RFC3339),
		},
	}
	resp, err := client.IndexDocument(ctx, document)
	if err != nil {
		t.Fatalf("failed to index compressed request to local OpenSearch: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Result != "created" && resp.Result != "updated" {
		t.Errorf("expected result 'created' or 'updated', got '%s'", resp.Result)
	}
	// _indexはalias等から解決したindexでもよいため、応答とdocumentの対応だけを確認します。
	if err := resp.Validate(document); err != nil {
		t.Errorf("index response does not identify the written document: %v", err)
	}
}

func TestIndexDocumentsCompressedToLocalOpenSearch(t *testing.T) {
	ctx := context.Background()

	client, err := New(openSearchIntegrationConfig(t, true))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	indexName := openSearchIntegrationIndex(t)

	resp, err := client.IndexDocuments(ctx, []DocumentWriteRequest{
		{
			Index: indexName,
			ID:    integrationDocID("compressed-bulk-1"),
			Op:    WriteOpIndex,
			Doc: map[string]any{
				"message": "compressed integration test bulk 1",
				"ts":      time.Now().Format(time.RFC3339),
			},
		},
		{
			Index: indexName,
			ID:    integrationDocID("compressed-bulk-2"),
			Op:    WriteOpIndex,
			Doc: map[string]any{
				"message": "compressed integration test bulk 2",
				"ts":      time.Now().Format(time.RFC3339),
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to bulk index compressed request to local OpenSearch: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Errors {
		t.Error("expected no errors in bulk operation")
	}
	if len(resp.Items) != 2 {
		t.Errorf("expected 2 items in response, got %d", len(resp.Items))
	}
}

func readGzipBody(t *testing.T, body []byte) []byte {
	t.Helper()

	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to open gzip body: %v", err)
	}
	defer reader.Close()

	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read gzip body: %v", err)
	}
	return decoded
}

func assertGzipHeader(t *testing.T, body []byte) {
	t.Helper()

	if len(body) < 2 || body[0] != 0x1f || body[1] != 0x8b {
		t.Fatalf("expected gzip body header, got %x", body)
	}
}

// openSearchIntegrationConfigは、実OpenSearchへの接続設定を返します。他の結合テストと同じく、
// OPENSEARCH_INTEGRATION_TEST=trueで有効化し、HETEROLOG_INTEGRATION_ENV_PATHの専用fileだけから
// 接続先・認証情報・書き込むindexを読みます。
func openSearchIntegrationConfig(t *testing.T, compressRequest bool) Config {
	t.Helper()

	// 設定項目をprocess環境から外し、専用fileの値だけを環境変数に設定します（試験後に元へ戻します）。
	settings := testutil.RequireIntegrationEnvironment(t, "OPENSEARCH_INTEGRATION_TEST")
	// OPENSEARCH_TEST_INDEXは設定loaderの項目ではないため、上の処理ではprocessから外れません。
	// 専用fileにない場合もprocessの値を引き継がず、既定のindexを使います。
	if _, ok := settings["OPENSEARCH_TEST_INDEX"]; !ok {
		t.Setenv("OPENSEARCH_TEST_INDEX", "")
	}

	endpoint := strings.TrimSpace(os.Getenv("HETEROLOG_OPENSEARCH_ENDPOINT"))
	if endpoint == "" {
		t.Fatal("OPENSEARCH_INTEGRATION_TEST requires an explicit HETEROLOG_OPENSEARCH_ENDPOINT in the integration environment file")
	}

	authType := AuthType(getenvDefault("HETEROLOG_OPENSEARCH_AUTH_TYPE", string(AuthTypeBasic)))
	authConfig := AuthConfig{Type: authType}
	switch authType {
	case AuthTypeBasic:
		authConfig.Config = BasicAuthConfig{
			Username: os.Getenv("HETEROLOG_OPENSEARCH_USERNAME"),
			Password: os.Getenv("HETEROLOG_OPENSEARCH_PASSWORD"),
		}
		if os.Getenv("HETEROLOG_OPENSEARCH_USERNAME") == "" || os.Getenv("HETEROLOG_OPENSEARCH_PASSWORD") == "" {
			t.Fatal("basic authentication requires explicit HETEROLOG_OPENSEARCH_USERNAME and HETEROLOG_OPENSEARCH_PASSWORD in the integration environment file")
		}
	case AuthTypeNone:
	default:
		t.Fatalf("unsupported HETEROLOG_OPENSEARCH_AUTH_TYPE for integration test: %s", authType)
	}

	return Config{
		Endpoint: endpoint,
		Timeout:  5 * time.Second,
		// 実サービスの証明書は既定で検証します。自己署名証明書の試験環境だけ、
		// HETEROLOG_OPENSEARCH_SKIP_VERIFY_SSL=trueで明示的に検証を省略します。
		SkipVerifySSL:   getenvBoolDefault(t, "HETEROLOG_OPENSEARCH_SKIP_VERIFY_SSL", false),
		AuthConfig:      authConfig,
		CompressRequest: compressRequest,
	}
}

func openSearchIntegrationIndex(t *testing.T) string {
	t.Helper()

	indexName := getenvDefault("OPENSEARCH_TEST_INDEX", "app-log-test-index")
	if !strings.HasPrefix(indexName, "app-log") {
		t.Fatalf("integration test index must start with app-log, got %q", indexName)
	}
	return indexName
}

func getenvDefault(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBoolDefault(t *testing.T, key string, fallback bool) bool {
	t.Helper()

	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("invalid boolean value for %s: %q", key, value)
	}
	return parsed
}

func integrationDocID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
