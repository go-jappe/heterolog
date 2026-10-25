package opensearch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
)

func TestIndexDocumentSuccessfulResponseReadFailureRetainsUnknownOutcome(t *testing.T) {
	for _, code := range []int{http.StatusCreated, http.StatusServiceUnavailable} {
		client, err := New(Config{Endpoint: "https://search.example.test", AuthConfig: AuthConfig{Type: AuthTypeNone}})
		if err != nil {
			t.Fatal(err)
		}
		client.(*realClient).httpClient.Transport = uncertainResponseTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(uncertainResponseReader{})}, nil
		})
		_, err = client.IndexDocument(context.Background(), DocumentWriteRequest{Index: "logs", ID: "fixed", Op: WriteOpCreate, Doc: map[string]any{"message": "keep"}})
		if code == http.StatusCreated {
			var decodeErr *ResponseDecodeError
			if !errors.As(err, &decodeErr) || !errors.Is(err, io.ErrUnexpectedEOF) || decodeErr.Operation != "index" {
				t.Fatalf("success body error = %v", err)
			}
		} else {
			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) || statusErr.Code != code {
				t.Fatalf("rejected request error = %v", err)
			}
		}
	}
}

type uncertainResponseTransport func(*http.Request) (*http.Response, error)

func (f uncertainResponseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type uncertainResponseReader struct{}

func (uncertainResponseReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
