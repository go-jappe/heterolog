package opensearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestIndexDocumentsRequiresCompleteResponse(t *testing.T) {
	const body = `{"items":[{"create":{"_index":"logs","_id":"fixed","status":201,"result":"created"}}]}`
	for _, tc := range []struct {
		name  string
		valid bool
		cause error
	}{
		{name: "plain with trailing whitespace", valid: true},
		{name: "valid gzip", valid: true},
		{name: "gzip checksum failure", cause: gzip.ErrChecksum},
		{name: "read failure after JSON", cause: io.ErrUnexpectedEOF},
		{name: "trailing invalid JSON"},
		{name: "second JSON value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reader io.Reader
			switch tc.name {
			case "valid gzip", "gzip checksum failure":
				var compressed bytes.Buffer
				writer := gzip.NewWriter(&compressed)
				if _, err := writer.Write([]byte(body)); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				payload := compressed.Bytes()
				if tc.cause != nil {
					payload[len(payload)-8] ^= 0xff
				}
				var err error
				reader, err = gzip.NewReader(bytes.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
			case "read failure after JSON":
				reader = io.MultiReader(strings.NewReader(body), uncertainResponseReader{})
			case "trailing invalid JSON":
				reader = strings.NewReader(body + " broken")
			case "second JSON value":
				reader = strings.NewReader(body + " {}")
			default:
				reader = strings.NewReader(body + " \r\n\t")
			}
			transport := uncertainResponseTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(reader)}, nil
			})
			client, err := New(Config{Endpoint: "https://search.example.test", AuthConfig: AuthConfig{Type: AuthTypeNone}})
			if err != nil {
				t.Fatal(err)
			}
			client.(*realClient).httpClient.Transport = transport
			output, err := client.IndexDocuments(context.Background(), []DocumentWriteRequest{{Index: "logs", ID: "fixed", Op: WriteOpCreate, Doc: map[string]any{"message": "keep"}}})
			if tc.valid {
				if err != nil || output == nil {
					t.Fatalf("valid response: output=%+v error=%v", output, err)
				}
				return
			}
			var decodeErr *ResponseDecodeError
			if output != nil || !errors.As(err, &decodeErr) {
				t.Fatalf("unverified response: output=%+v error=%v; want ResponseDecodeError", output, err)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("error = %v, want cause %v", err, tc.cause)
			}
		})
	}
}
