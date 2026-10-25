package cloudwatchlogs_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

type credentialsFunc func(context.Context) (aws.Credentials, error)

func (f credentialsFunc) Retrieve(ctx context.Context) (aws.Credentials, error) { return f(ctx) }

func TestCredentialFailureMarksOnlyRequestsThatWereNotDispatched(t *testing.T) {
	for _, tt := range []struct {
		name       string
		failAt     int
		inFlight   bool
		wantCalls  int
		wantTarget string
		wantPrior  bool
	}{
		{"first put credentials", 1, false, 0, "PutLogEvents", false},
		{"create credentials", 2, false, 1, "CreateLogStream", false},
		{"second put credentials", 3, false, 2, "PutLogEvents", true},
		{"first put in flight", 1, true, 1, "", false},
		{"second put in flight", 3, true, 3, "", false},
	} {
		for _, failure := range []error{context.Canceled, context.DeadlineExceeded, io.EOF} {
			t.Run(tt.name+"/"+failure.Error(), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					credentialsCalls, httpCalls := 0, 0
					client, err := cw.New(cw.Config{
						Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "logs", AutoCreateLogStream: true,
						CredentialsProvider: credentialsFunc(func(context.Context) (aws.Credentials, error) {
							credentialsCalls++
							if !tt.inFlight && credentialsCalls == tt.failAt {
								return aws.Credentials{}, failure
							}
							return aws.Credentials{AccessKeyID: "example", SecretAccessKey: "example", CanExpire: true, Expires: time.Now().Add(time.Second)}, nil
						}),
						HTTPClient: &http.Client{Transport: endpointRoundTripFunc(func(r *http.Request) (*http.Response, error) {
							time.Sleep(2 * time.Second)
							httpCalls++
							if tt.inFlight && httpCalls == tt.failAt {
								return nil, failure
							}
							code, body := 200, `{}`
							if httpCalls == 1 {
								code, body = 400, `{"__type":"ResourceNotFoundException"}`
							}
							return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
						})},
					})
					require.NoError(t, err)
					now := time.Now()
					output, err := client.PutLogEvents(context.Background(), cw.PutLogEventsRequest{LogStreamName: "stream", AttemptedAt: now, Events: []cw.Event{{Timestamp: now, Message: "keep original"}}})
					require.Nil(t, output)
					require.ErrorIs(t, err, failure)
					require.Equal(t, tt.wantCalls, httpCalls)
					require.Equal(t, tt.failAt, credentialsCalls)
					var unsent *cw.RequestNotSentError
					if tt.inFlight {
						require.False(t, errors.As(err, &unsent), "an interrupted HTTP attempt is not pre-dispatch evidence")
						return
					}
					require.ErrorAs(t, err, &unsent)
					require.Equal(t, "Logs_20140328."+tt.wantTarget, unsent.Operation)
					require.ErrorIs(t, unsent.Err, failure)
					if tt.wantPrior {
						require.True(t, cw.IsResourceNotFound(unsent.PreviousPutError))
						require.ErrorIs(t, err, unsent.PreviousPutError)
					} else {
						require.Nil(t, unsent.PreviousPutError)
					}
					if tt.failAt == 2 {
						var creation *cw.AutoCreateLogStreamError
						require.ErrorAs(t, err, &creation)
						require.True(t, cw.IsResourceNotFound(creation.PutError))
						require.ErrorIs(t, err, creation.PutError)
					}
				})
			})
		}
	}
}
