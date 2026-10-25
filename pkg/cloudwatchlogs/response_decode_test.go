package cloudwatchlogs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPutLogEventsResponseValidation(t *testing.T) {
	integer := func(value int) *int { return &value }
	for _, tc := range []struct {
		name  string
		info  *RejectedLogEventsInfo
		valid bool
	}{
		{"no rejection", nil, true},
		{"old exclusive end equals count", &RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(2)}, true},
		{"expired exclusive end equals count", &RejectedLogEventsInfo{ExpiredLogEventEndIndex: integer(2)}, true},
		{"new starts at last event", &RejectedLogEventsInfo{TooNewLogEventStartIndex: integer(1)}, true},
		{"adjacent old and new ranges", &RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(1), TooNewLogEventStartIndex: integer(1)}, true},
		{"overlapping old and expired", &RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(2), ExpiredLogEventEndIndex: integer(1)}, true},
		{"negative end", &RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(-1)}, false},
		{"end exceeds count", &RejectedLogEventsInfo{ExpiredLogEventEndIndex: integer(3)}, false},
		{"inclusive start equals count", &RejectedLogEventsInfo{TooNewLogEventStartIndex: integer(2)}, false},
		{"overlapping old and new", &RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(2), TooNewLogEventStartIndex: integer(1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (&PutLogEventsOutput{RejectedLogEventsInfo: tc.info}).Validate(2)
			if (err == nil) != tc.valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, tc.valid)
			}
		})
	}
}

func TestPutLogEventsSuccessfulResponseMustEstablishOutcome(t *testing.T) {
	for _, body := range []string{"", "null", "{", `{"rejectedLogEventsInfo":{"tooNewLogEventStartIndex":1}}`, "{}"} {
		t.Run(body, func(t *testing.T) {
			client, err := New(Config{Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "logs", AccessKeyID: "test", SecretAccessKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			client.httpClient.Transport = uncertainResponseTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			output, err := client.PutLogEvents(context.Background(), PutLogEventsRequest{LogStreamName: "stream", Events: []Event{{Timestamp: time.Unix(10, 0), Message: "keep"}}, AttemptedAt: time.Unix(20, 0)})
			if body == "{}" {
				if err != nil || output == nil {
					t.Fatalf("valid empty object: output=%+v error=%v", output, err)
				}
				return
			}
			var decodeErr *ResponseDecodeError
			if !errors.As(err, &decodeErr) || output != nil {
				t.Fatalf("uncertain response: output=%+v error=%v", output, err)
			}
		})
	}
}

type uncertainResponseTransport func(*http.Request) (*http.Response, error)

func (f uncertainResponseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
