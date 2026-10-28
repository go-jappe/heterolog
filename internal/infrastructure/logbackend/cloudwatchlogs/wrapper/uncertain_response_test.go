package wrapper

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchResponseReadFailureDoesNotRetryButTransportEOFDoes(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		var cause error = io.ErrUnexpectedEOF
		if uncertain {
			cause = &cloudwatchlogs.ResponseDecodeError{Operation: "PutLogEvents", Err: cause}
		}
		client := &recordingClient{}
		client.send = func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
			if len(client.calls) == 1 {
				return nil, cause
			}
			return &cloudwatchlogs.PutLogEventsOutput{}, nil
		}
		retrying := newRetryingClient(client, fastRetryConfig(2), observer.NopTracer{}, nil)
		_, err := retrying.PutLogEvents(context.Background(), testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "keep"}}))
		if uncertain {
			require.ErrorIs(t, err, cause)
			require.Len(t, client.calls, 1)
		} else {
			require.NoError(t, err)
			require.Len(t, client.calls, 2)
		}
	}
}
