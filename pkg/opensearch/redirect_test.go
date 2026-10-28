package opensearch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

// redirectは、設定した送信先が本来の送信先ではないことを表します。301/302/303を追従すると
// 本文なしのGETへ変わり、転送先の応答を書き込みの結果と取り違えます。307/308を追従すると、
// 文書を別URLへ再送します。単一文書とbulkのどちらの書き込みも、redirectを追従せず、
// 元の要求一回のresponseをHTTPStatusErrorとして返す必要があります。
func TestWritesReturnEveryRedirectWithoutFollowing(t *testing.T) {
	doc := DocumentWriteRequest{Index: "app-log", ID: "doc-1", Op: WriteOpIndex, Doc: map[string]any{"message": "preserve this log"}}
	operations := []struct {
		name string
		call func(Client) error
	}{
		{"IndexDocument", func(client Client) error {
			_, err := client.IndexDocument(context.Background(), doc)
			return err
		}},
		{"IndexDocuments", func(client Client) error {
			_, err := client.IndexDocuments(context.Background(), []DocumentWriteRequest{doc})
			return err
		}},
	}
	for _, operation := range operations {
		for _, status := range []int{
			http.StatusMovedPermanently,
			http.StatusFound,
			http.StatusSeeOther,
			http.StatusTemporaryRedirect,
			http.StatusPermanentRedirect,
		} {
			t.Run(fmt.Sprintf("%s/%d", operation.name, status), func(t *testing.T) {
				var mu sync.Mutex
				var requests []string
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					mu.Lock()
					requests = append(requests, r.Method+" "+r.URL.Path)
					mu.Unlock()
					if r.URL.Path != "/redirected" {
						w.Header().Set("Location", "/redirected")
						w.WriteHeader(status)
						_, _ = io.WriteString(w, "redirected by gateway")
						return
					}
					// 転送先は、どの要求にも書き込みの成功に見える応答を返します。
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					_, _ = io.WriteString(w, `{"_index":"app-log","_id":"doc-1","_version":1,"result":"created",`+
						`"errors":false,"items":[{"index":{"_index":"app-log","_id":"doc-1","status":201,"result":"created"}}]}`)
				}))
				defer server.Close()
				client, err := New(Config{Endpoint: server.URL, Timeout: 3 * time.Second, AuthConfig: AuthConfig{Type: AuthTypeNone}})
				if err != nil {
					t.Fatal(err)
				}

				err = operation.call(client)
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.Code != status || string(statusErr.Body) != "redirected by gateway" {
					t.Fatalf("%s() error = %v, want HTTPStatusError with the original status %d and body", operation.name, err, status)
				}
				mu.Lock()
				got := slices.Clone(requests)
				mu.Unlock()
				if len(got) != 1 || got[0] == "GET /redirected" || got[0] == "PUT /redirected" || got[0] == "POST /redirected" {
					t.Fatalf("requests = %q, want only the original request without following the redirect", got)
				}
			})
		}
	}
}
