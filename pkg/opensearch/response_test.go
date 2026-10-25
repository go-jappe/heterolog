package opensearch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestIndexDocumentValidatesSuccessfulResponse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		op    WriteOp
		body  string
		valid bool
	}{
		{"created in resolved alias", WriteOpCreate, `{"_index":"logs-000042","_id":"fixed","result":"created"}`, true},
		{"index creates", WriteOpIndex, `{"_index":"logs-000042","_id":"fixed","result":"created"}`, true},
		{"index updates", WriteOpIndex, `{"_index":"logs-000042","_id":"fixed","result":"updated"}`, true},
		{"missing ID", WriteOpCreate, `{"_index":"logs-000042","result":"created"}`, false},
		{"different ID", WriteOpCreate, `{"_index":"logs-000042","_id":"another-record","result":"created"}`, false},
		{"missing index", WriteOpCreate, `{"_id":"fixed","result":"created"}`, false},
		{"missing result", WriteOpCreate, `{"_index":"logs-000042","_id":"fixed"}`, false},
		{"create cannot update", WriteOpCreate, `{"_index":"logs-000042","_id":"fixed","result":"updated"}`, false},
		{"null response", WriteOpCreate, `null`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := New(Config{Endpoint: "https://search.example.test", AuthConfig: AuthConfig{Type: AuthTypeNone}})
			if err != nil {
				t.Fatal(err)
			}
			client.(*realClient).httpClient.Transport = uncertainResponseTransport(func(*http.Request) (*http.Response, error) {
				status := http.StatusCreated
				if tc.name == "index updates" {
					status = http.StatusOK
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			output, err := client.IndexDocument(context.Background(), DocumentWriteRequest{
				Index: "logs-write-alias", ID: "fixed", Op: tc.op, Doc: map[string]any{"message": "keep"},
			})
			if tc.valid {
				if err != nil || output == nil || output.ID != "fixed" || output.Index != "logs-000042" {
					t.Fatalf("valid response: output=%+v error=%v", output, err)
				}
				return
			}
			var decodeErr *ResponseDecodeError
			if output != nil || !errors.As(err, &decodeErr) || decodeErr.Operation != "index" {
				t.Fatalf("uncertain response: output=%+v error=%v; want index ResponseDecodeError", output, err)
			}
		})
	}
}

func TestIndexDocumentIngestDropResponse(t *testing.T) {
	const noopResponse = `{"_index":"logs-000042","_id":"fixed","_version":-3,"result":"noop","_shards":{"total":0,"successful":0,"failed":0}}`
	for _, op := range []WriteOp{WriteOpIndex, WriteOpCreate} {
		for _, tc := range []struct {
			name   string
			status int
			body   string
			valid  bool
		}{
			{"confirmed drop", http.StatusOK, noopResponse, true},
			{"created status contradicts drop", http.StatusCreated, noopResponse, false},
			{"accepted status does not confirm drop", http.StatusAccepted, noopResponse, false},
			{"missing ID", http.StatusOK, `{"_index":"logs-000042","result":"noop"}`, false},
			{"different ID", http.StatusOK, `{"_index":"logs-000042","_id":"another-record","result":"noop"}`, false},
			{"missing index", http.StatusOK, `{"_id":"fixed","result":"noop"}`, false},
		} {
			t.Run(string(op)+"/"+tc.name, func(t *testing.T) {
				client, err := New(Config{Endpoint: "https://search.example.test", AuthConfig: AuthConfig{Type: AuthTypeNone}})
				if err != nil {
					t.Fatal(err)
				}
				client.(*realClient).httpClient.Transport = uncertainResponseTransport(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
				})
				output, err := client.IndexDocument(context.Background(), DocumentWriteRequest{
					Index: "logs-write-alias", ID: "fixed", Op: op, Doc: map[string]any{"message": "filtered by pipeline"},
				})
				if tc.valid {
					if err != nil || output == nil || output.ID != "fixed" || output.Index != "logs-000042" || output.Result != "noop" {
						t.Fatalf("confirmed pipeline drop: output=%+v error=%v", output, err)
					}
					return
				}
				var decodeErr *ResponseDecodeError
				if output != nil || !errors.As(err, &decodeErr) || decodeErr.Operation != "index" {
					t.Fatalf("unconfirmed response: output=%+v error=%v; want index ResponseDecodeError", output, err)
				}
			})
		}
	}
}
