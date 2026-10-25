package cloudwatchlogs

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	backend "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/stretchr/testify/require"
)

type credentialsFunc func(context.Context) (aws.Credentials, error)

func (f credentialsFunc) Retrieve(ctx context.Context) (aws.Credentials, error) { return f(ctx) }

func TestCredentialCancellationFlushPreservesReplayAndDeliveryEvidence(t *testing.T) {
	for _, tt := range []struct {
		name                                 string
		credentialsFailAt, transportCancelAt int
		wantHTTP, wantCode                   int
		wantStatus                           pkglogger.DeliveryStatus
	}{
		{"initial credentials", 1, 0, 0, 0, pkglogger.DeliveryRejected},
		{"creation credentials", 2, 0, 1, 400, pkglogger.DeliveryFailed},
		{"second put credentials", 3, 0, 2, 400, pkglogger.DeliveryFailed},
		{"initial put in flight", 0, 1, 1, 0, pkglogger.DeliveryUnconfirmed},
		{"second put in flight", 0, 3, 3, 0, pkglogger.DeliveryUnconfirmed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				credentialsCalls, httpCalls := 0, 0
				client, err := backend.New(backend.Config{
					Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test", AutoCreateLogStream: true,
					CredentialsProvider: credentialsFunc(func(context.Context) (aws.Credentials, error) {
						credentialsCalls++
						if credentialsCalls == tt.credentialsFailAt {
							cancel()
							return aws.Credentials{}, ctx.Err()
						}
						return aws.Credentials{AccessKeyID: "example", SecretAccessKey: "example", CanExpire: true, Expires: time.Now().Add(time.Second)}, nil
					}),
					HTTPClient: &http.Client{Transport: autoCreateTransport(func(r *http.Request) (*http.Response, error) {
						time.Sleep(2 * time.Second)
						httpCalls++
						if httpCalls == tt.transportCancelAt {
							cancel()
							return nil, ctx.Err()
						}
						code, body := 200, `{}`
						if httpCalls == 1 {
							code, body = 400, `{"__type":"ResourceNotFoundException"}`
						}
						return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
					})},
				})
				require.NoError(t, err)
				saved := &recordingCloudWatchLogsFailureHandler{}
				fm := newTestCloudWatchLogsFlushManager(client, mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{saved}))
				err = fm.Put(ctx, newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "keep original log"))
				require.ErrorIs(t, err, context.Canceled)
				require.NoError(t, fm.Close(context.Background()))
				result, ok := saved.result()
				require.True(t, ok)
				require.Equal(t, 1, result.FailedCount())
				records := result.RetryableRecords
				if tt.wantStatus == pkglogger.DeliveryUnconfirmed {
					records = result.QuarantineRecords
				}
				require.Len(t, records, 1)
				require.Equal(t, tt.wantCode, records[0].StatusCode)
				require.ErrorIs(t, records[0].Cause, context.Canceled)
				report := result.TargetDeliveryResult("cloudwatchlogs")
				require.Equal(t, tt.wantStatus, report.Status)
				require.ErrorIs(t, report.Err, context.Canceled)
				require.Equal(t, tt.wantHTTP, httpCalls)
				require.Contains(t, records[0].Payload["line"], "keep original log")
				if tt.credentialsFailAt != 0 {
					require.Equal(t, tt.credentialsFailAt, credentialsCalls)
					var unsent *backend.RequestNotSentError
					require.ErrorAs(t, records[0].Cause, &unsent)
					require.ErrorIs(t, report.Err, unsent.Err)
					if tt.credentialsFailAt > 1 {
						var refused *backend.HTTPStatusError
						require.ErrorAs(t, report.Err, &refused)
						require.Equal(t, backend.ErrorTypeResourceNotFoundException, refused.ErrorType())
					}
				}
			})
		})
	}
}
