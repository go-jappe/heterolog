package clickhouse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestSuccessfulResponseReadFailureIsDistinctFromRejectedRequest(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		client, err := New(Config{Endpoint: "https://clickhouse.example.test", Table: "logs"})
		if err != nil {
			t.Fatal(err)
		}
		client.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(uncertainResponseReader{})}, nil
		})
		_, err = client.InsertLogsWithOptions(context.Background(), []LogRecord{{Timestamp: time.Unix(10, 0), Message: "keep"}}, InsertOptions{})
		if code == http.StatusOK {
			var decodeErr *ResponseDecodeError
			if !errors.As(err, &decodeErr) || !errors.Is(err, io.ErrUnexpectedEOF) {
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

type uncertainResponseReader struct{}

func (uncertainResponseReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
