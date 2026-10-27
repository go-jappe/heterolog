package opensearch

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestIndexDocumentEncodesIndexExpressionAsOnePathSegment(t *testing.T) {
	// 期待するencodeは、GoのPathEscapeでは変更されずに残る予約文字の'+'と':'も含め、
	// index名の構文の表に従います。
	// https://docs.opensearch.org/latest/api-reference/index-name-syntax/
	indexes := []struct {
		name, index, escaped string
	}{
		{"ordinary", "logs-2026.10.22", "logs-2026.10.22"},
		{"date math", "<logs-{now/d}>", "%3Clogs-%7Bnow%2Fd%7D%3E"},
		{"positive time zone", "<logs-{now/d{yyyy.MM.dd|+09:00}}>", "%3Clogs-%7Bnow%2Fd%7Byyyy.MM.dd%7C%2B09%3A00%7D%7D%3E"},
		{"negative time zone", "<logs-{now/d{yyyy.MM.dd|-08:00}}>", "%3Clogs-%7Bnow%2Fd%7Byyyy.MM.dd%7C-08%3A00%7D%7D%3E"},
		{"named time zone", "<logs-{now/d{yyyy.MM.dd|America/Los_Angeles}}>", "%3Clogs-%7Bnow%2Fd%7Byyyy.MM.dd%7CAmerica%2FLos_Angeles%7D%7D%3E"},
		{"escaped static braces", `<logs\{prod\}-{now/M}>`, "%3Clogs%5C%7Bprod%5C%7D-%7Bnow%2FM%7D%3E"},
		{"literal percent", "logs%25", "logs%2525"},
		{"unicode", "記録", "%E8%A8%98%E9%8C%B2"},
	}
	endpoints := []struct {
		name, path, prefix string
	}{
		{name: "root"},
		{name: "prefix", path: "/gateway/search/", prefix: "/gateway/search"},
		{name: "escaped prefix", path: "/gateway%20search/tenant%2Fone/", prefix: "/gateway%20search/tenant%2Fone"},
	}
	for _, endpoint := range endpoints {
		for _, operation := range []struct {
			op   WriteOp
			path string
		}{{WriteOpIndex, "_doc"}, {WriteOpCreate, "_create"}} {
			for _, index := range indexes {
				t.Run(endpoint.name+"/"+string(operation.op)+"/"+index.name, func(t *testing.T) {
					client, err := New(Config{Endpoint: "https://search.example.test" + endpoint.path, AuthConfig: AuthConfig{Type: AuthTypeNone}})
					if err != nil {
						t.Fatal(err)
					}
					calls := 0
					client.(*realClient).httpClient.Transport = documentPathTransport(func(request *http.Request) (*http.Response, error) {
						calls++
						wantURI := endpoint.prefix + "/" + index.escaped + "/" + operation.path + "/job%2Fpart%3Fkey%23tail%25"
						assertIndexRequestURI(t, request, wantURI)
						return &http.Response{
							StatusCode: http.StatusCreated, Header: make(http.Header),
							// date mathやaliasは、実際のindex名へ解決されます。
							Body: io.NopCloser(strings.NewReader(`{"_index":"logs-resolved","_id":"job/part?key#tail%","result":"created"}`)),
						}, nil
					})
					_, err = client.IndexDocument(context.Background(), DocumentWriteRequest{
						Index: index.index, ID: "job/part?key#tail%", Op: operation.op, Doc: map[string]any{"message": "retain"},
					})
					if err != nil || calls != 1 {
						t.Fatalf("IndexDocument(): calls=%d error=%v", calls, err)
					}
				})
			}
		}
	}
}

func TestIndexDocumentDoesNotInterpretIndexAsURLSyntax(t *testing.T) {
	// index名の検証はOpenSearchの役割です。serverが拒否する名前であっても、query parameterを
	// 注入したり、action/IDを省いたり、'%'を事前にdecodeしたりしてはいけません。
	for _, tt := range []struct{ index, escaped string }{
		{"logs?refresh=true", "logs%3Frefresh%3Dtrue"},
		{"logs#fragment", "logs%23fragment"},
		{"logs%2Fother", "logs%252Fother"},
		{"logs%", "logs%25"},
		{"log files", "log%20files"},
	} {
		t.Run(tt.index, func(t *testing.T) {
			client, err := New(Config{Endpoint: "https://search.example.test", AuthConfig: AuthConfig{Type: AuthTypeNone}})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			client.(*realClient).httpClient.Transport = documentPathTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				assertIndexRequestURI(t, request, "/"+tt.escaped+"/_doc/fixed")
				return &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("invalid index name"))}, nil
			})
			_, err = client.IndexDocument(context.Background(), DocumentWriteRequest{Index: tt.index, ID: "fixed", Op: WriteOpIndex, Doc: map[string]any{"message": "retain"}})
			var statusErr *HTTPStatusError
			if calls != 1 || !errors.As(err, &statusErr) || statusErr.Code != http.StatusBadRequest {
				t.Fatalf("server rejection not preserved: calls=%d error=%v", calls, err)
			}
		})
	}
}

func assertIndexRequestURI(t *testing.T, request *http.Request, wantURI string) {
	t.Helper()
	if request.URL.RawQuery != "" || request.URL.Fragment != "" {
		t.Errorf("index changed URL structure: query=%q fragment=%q", request.URL.RawQuery, request.URL.Fragment)
	}
	// 外部のOpenSearch serverやlisten中のsocketを必要とせずに、RawPathの扱いも含めて、
	// serializeされたHTTP request targetを確認します。
	var wire bytes.Buffer
	if err := request.Write(&wire); err != nil {
		t.Fatal(err)
	}
	parsed, err := http.ReadRequest(bufio.NewReader(&wire))
	if err != nil {
		t.Fatal(err)
	}
	defer parsed.Body.Close()
	if parsed.Method != http.MethodPut || parsed.RequestURI != wantURI {
		t.Errorf("HTTP target = %s %s, want PUT %s", parsed.Method, parsed.RequestURI, wantURI)
	}
}
