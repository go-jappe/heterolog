package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestSingleDocumentResponseMustEstablishDelivery(t *testing.T) {
	for _, clientKind := range []string{"HTTP", "custom"} {
		for _, responseKind := range []string{"resolved alias", "pipeline drop", "missing ID", "different ID", "missing index", "wrong result"} {
			t.Run(clientKind+"/"+responseKind, func(t *testing.T) {
				response := func(id string) *pkgopensearch.IndexResponse {
					out := &pkgopensearch.IndexResponse{Index: "logs-000042", ID: id, Result: "created"}
					switch responseKind {
					case "pipeline drop":
						out.Result = "noop"
					case "missing ID":
						out.ID = ""
					case "different ID":
						out.ID = "unrelated-record"
					case "missing index":
						out.Index = ""
					case "wrong result":
						out.Result = "updated"
					}
					return out
				}
				fm := newFlushManagerForDeliveryTest(nil)
				if clientKind == "custom" {
					fm.osClient = &responseContractClient{response: response}
				} else {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
						status := http.StatusCreated
						if responseKind == "pipeline drop" {
							status = http.StatusOK
						}
						w.WriteHeader(status)
						_ = json.NewEncoder(w).Encode(response(id))
					}))
					t.Cleanup(server.Close)
					client, err := pkgopensearch.New(pkgopensearch.Config{Endpoint: server.URL, AuthConfig: pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone}})
					if err != nil {
						t.Fatal(err)
					}
					fm.osClient = client
				}
				result, err := fm.sendDocumentToOpenSearch(context.Background(), map[string]any{"message": "preserve me"})
				if err != nil {
					t.Fatal(err)
				}
				if responseKind == "resolved alias" || responseKind == "pipeline drop" {
					if result.HasSendFailure() || result.TerminalError != nil {
						t.Fatalf("valid alias response failed: %+v", result)
					}
					return
				}
				var decodeErr *pkgopensearch.ResponseDecodeError
				if !errors.As(result.TerminalError, &decodeErr) || len(result.QuarantineRecords) != 1 || len(result.RetryableRecords) != 0 || len(result.DeadRecords) != 0 {
					t.Fatalf("unconfirmed response must be quarantined: %+v", result)
				}
				document, ok := result.QuarantineRecords[0].Payload["doc"].(map[string]any)
				if !ok || document["message"] != "preserve me" {
					t.Fatalf("quarantine lost original document: %+v", result.QuarantineRecords[0])
				}
			})
		}
	}
}

func TestBulkIngestDropRetriesOnlyRejectedNeighbor(t *testing.T) {
	for _, rejectEveryAttempt := range []bool{false, true} {
		name := "retry succeeds"
		if rejectEveryAttempt {
			name = "retry remains rejected"
		}
		t.Run(name, func(t *testing.T) {
			client := &pipelineDropBulkClient{rejectEveryAttempt: rejectEveryAttempt}
			fm := newFlushManagerForDeliveryTest(nil)
			fm.osClient = client
			fm.bulkRetryConfig = &MockOpenSearchFlushManagerConfig{maxRetryAttempts: 2}
			docs := []map[string]any{{"message": "filtered"}, {"message": "stored"}, {"message": "retry"}}
			result, err := fm.sendDocumentsToOpenSearch(context.Background(), docs)
			if err != nil {
				t.Fatal(err)
			}
			if len(client.calls) != 2 || len(client.calls[0]) != 3 || len(client.calls[1]) != 1 {
				t.Fatalf("requests = %+v, want initial three documents then only the rejected one", client.calls)
			}
			for i, request := range client.calls[0] {
				if request.ID == "" || request.Op != pkgopensearch.WriteOpCreate || !maps.Equal(request.Doc, docs[i]) {
					t.Fatalf("initial document[%d] = %+v, want create with preserved document %+v", i, request, docs[i])
				}
			}
			initial := client.calls[0][2]
			retry := client.calls[1][0]
			if retry.ID != initial.ID || retry.Index != initial.Index || retry.Op != initial.Op || !maps.Equal(retry.Doc, initial.Doc) {
				t.Fatalf("retry changed document identity or contents: initial=%+v retry=%+v", initial, retry)
			}
			if result.TerminalError != nil || len(result.QuarantineRecords) != 0 || len(result.DeadRecords) != 0 {
				t.Fatalf("confirmed responses became terminal failures: %+v", result)
			}
			if rejectEveryAttempt {
				if len(result.RetryableRecords) != 1 || result.RetryableRecords[0].Payload["id"] != initial.ID {
					t.Fatalf("retryable records = %+v, want only the rejected neighbor", result.RetryableRecords)
				}
				if target := openSearchTargetDeliveryResult(result); target.Status != pkglogger.DeliveryFailed {
					t.Fatalf("rejected neighbor must keep target delivery failed: %+v", target)
				}
			} else {
				if result.HasSendFailure() {
					t.Fatalf("all three operations completed: %+v", result)
				}
				if target := openSearchTargetDeliveryResult(result); target.Status != pkglogger.DeliverySucceeded {
					t.Fatalf("completed operations must succeed: %+v", target)
				}
			}
		})
	}
}

type pipelineDropBulkClient struct {
	nilIndexResponseClient
	rejectEveryAttempt bool
	calls              [][]pkgopensearch.DocumentWriteRequest
}

func (c *pipelineDropBulkClient) IndexDocuments(_ context.Context, requests []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
	snapshot := make([]pkgopensearch.DocumentWriteRequest, len(requests))
	response := &pkgopensearch.BulkResponse{}
	for i, request := range requests {
		snapshot[i] = request
		snapshot[i].Doc = maps.Clone(request.Doc)
		item := pkgopensearch.BulkItemResult{Index: request.Index, ID: request.ID, Status: http.StatusCreated, Result: "created"}
		switch request.Doc["message"] {
		case "filtered":
			item.Status, item.Result = http.StatusOK, "noop"
		case "retry":
			if len(c.calls) == 0 || c.rejectEveryAttempt {
				response.Errors = true
				item.Status, item.Result = http.StatusTooManyRequests, ""
				item.Error = &pkgopensearch.ErrorDetail{Type: "es_rejected_execution_exception", Reason: "queue full"}
			}
		}
		response.Items = append(response.Items, map[string]pkgopensearch.BulkItemResult{string(request.Op): item})
	}
	c.calls = append(c.calls, snapshot)
	return response, nil
}

type responseContractClient struct {
	nilIndexResponseClient
	response func(string) *pkgopensearch.IndexResponse
}

func (c *responseContractClient) IndexDocument(_ context.Context, request pkgopensearch.DocumentWriteRequest) (*pkgopensearch.IndexResponse, error) {
	return c.response(request.ID), nil
}
