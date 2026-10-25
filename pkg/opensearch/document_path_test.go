package opensearch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestIndexDocumentPreservesIDAsOnePathSegment(t *testing.T) {
	endpoints := []struct {
		name       string
		path       string
		wantPrefix string
	}{
		{name: "root"},
		{name: "prefix", path: "/gateway/search", wantPrefix: "/gateway/search"},
		{name: "trailing slash", path: "/gateway/search/", wantPrefix: "/gateway/search"},
		{name: "escaped prefix", path: "/gateway%20search/", wantPrefix: "/gateway%20search"},
	}
	operations := []struct {
		op   WriteOp
		path string
	}{
		{op: WriteOpIndex, path: "/logs/_doc/"},
		{op: WriteOpCreate, path: "/logs/_create/"},
	}
	ids := []struct {
		name    string
		id      string
		escaped string
	}{
		{name: "plain", id: "job-123", escaped: "job-123"},
		{name: "query", id: "job?refresh=true", escaped: "job%3Frefresh=true"},
		{name: "fragment", id: "job#fragment", escaped: "job%23fragment"},
		{name: "slash", id: "job/child", escaped: "job%2Fchild"},
		{name: "literal escape", id: "job%2Fchild", escaped: "job%252Fchild"},
		{name: "percent", id: "job%", escaped: "job%25"},
		{name: "space", id: "job name", escaped: "job%20name"},
		{name: "unicode", id: "日本", escaped: "%E6%97%A5%E6%9C%AC"},
		{name: "relative looking", id: "../child", escaped: "..%2Fchild"},
	}

	for _, endpoint := range endpoints {
		for _, operation := range operations {
			for _, id := range ids {
				t.Run(endpoint.name+"/"+string(operation.op)+"/"+id.name, func(t *testing.T) {
					client, err := New(Config{
						Endpoint:   "https://search.example.test" + endpoint.path,
						AuthConfig: AuthConfig{Type: AuthTypeNone},
					})
					if err != nil {
						t.Fatal(err)
					}
					var sent *http.Request
					client.(*realClient).httpClient.Transport = documentPathTransport(func(request *http.Request) (*http.Response, error) {
						sent = request
						body, err := json.Marshal(map[string]string{"_index": "logs", "_id": id.id, "result": "created"})
						if err != nil {
							return nil, err
						}
						return &http.Response{
							StatusCode: http.StatusCreated,
							Header:     make(http.Header),
							Body:       io.NopCloser(strings.NewReader(string(body))),
						}, nil
					})

					_, err = client.IndexDocument(context.Background(), DocumentWriteRequest{
						Index: "logs",
						ID:    id.id,
						Op:    operation.op,
						Doc:   map[string]any{"message": "payload"},
					})
					if err != nil {
						t.Fatalf("IndexDocument() error = %v", err)
					}
					if sent == nil {
						t.Fatal("IndexDocument did not send a request")
					}
					wantURI := endpoint.wantPrefix + operation.path + id.escaped
					if got := sent.URL.RequestURI(); got != wantURI {
						t.Errorf("request URI = %q, want %q", got, wantURI)
					}
					if sent.URL.RawQuery != "" || sent.URL.Fragment != "" {
						t.Errorf("document ID became URL syntax: query = %q, fragment = %q", sent.URL.RawQuery, sent.URL.Fragment)
					}
				})
			}
		}
	}
}

type documentPathTransport func(*http.Request) (*http.Response, error)

func (f documentPathTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
