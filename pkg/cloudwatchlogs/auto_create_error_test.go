package cloudwatchlogs_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

func TestAutoCreateFailurePreservesPutAndCreationCauses(t *testing.T) {
	for _, creationErr := range []error{context.Canceled, context.DeadlineExceeded, io.ErrUnexpectedEOF} {
		t.Run(creationErr.Error(), func(t *testing.T) {
			var targets []string
			client, err := cw.New(cw.Config{
				Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "logs",
				AccessKeyID: "example", SecretAccessKey: "example", AutoCreateLogStream: true,
				HTTPClient: &http.Client{Transport: endpointRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					targets = append(targets, r.Header.Get("X-Amz-Target"))
					if len(targets) == 1 {
						return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(`{"__type":"ResourceNotFoundException","message":"stream missing"}`))}, nil
					}
					return nil, creationErr
				})},
			})
			require.NoError(t, err)
			now := time.Now()
			output, err := client.PutLogEvents(context.Background(), cw.PutLogEventsRequest{LogStreamName: "stream", Events: []cw.Event{{Timestamp: now, Message: "keep"}}, AttemptedAt: now})
			require.Nil(t, output)
			require.Equal(t, []string{"Logs_20140328.PutLogEvents", "Logs_20140328.CreateLogStream"}, targets)
			var creationFailure *cw.AutoCreateLogStreamError
			require.ErrorAs(t, err, &creationFailure)
			require.True(t, cw.IsResourceNotFound(creationFailure.PutError))
			require.ErrorIs(t, creationFailure.CreateError, creationErr)
			require.ErrorIs(t, err, creationErr)
			require.True(t, errors.Is(err, creationFailure.PutError))
			var originalPut *cw.HTTPStatusError
			require.ErrorAs(t, err, &originalPut)
			require.Equal(t, 400, originalPut.Code)
		})
	}
}
