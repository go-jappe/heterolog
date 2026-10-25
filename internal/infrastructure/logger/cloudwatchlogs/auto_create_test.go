package cloudwatchlogs

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	backend "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/stretchr/testify/require"
)

type autoCreateTransport func(*http.Request) (*http.Response, error)

func (f autoCreateTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAutoCreateFlushPreservesKnownRejectionAndFailureCauses(t *testing.T) {
	for _, scenario := range []string{"creation canceled", "creation denied", "second put canceled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			client, err := backend.New(backend.Config{
				Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test", AccessKeyID: "example", SecretAccessKey: "example", AutoCreateLogStream: true,
				HTTPClient: &http.Client{Transport: autoCreateTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					response := func(code int, body string) (*http.Response, error) {
						return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
					}
					if calls == 1 {
						return response(400, `{"__type":"ResourceNotFoundException"}`)
					}
					if calls == 2 {
						if scenario == "creation denied" {
							return response(403, `{"__type":"AccessDeniedException"}`)
						}
						if scenario == "second put canceled" {
							return response(200, `{}`)
						}
					}
					cancel()
					return nil, ctx.Err()
				})},
			})
			require.NoError(t, err)
			saved := &recordingCloudWatchLogsFailureHandler{}
			composite := mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{saved})
			fm := newTestCloudWatchLogsFlushManager(client, composite)
			err = fm.Put(ctx, newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "keep original log"))
			if scenario == "creation denied" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.NoError(t, fm.Close(context.Background()))
			result, ok := saved.result()
			require.True(t, ok)
			require.Equal(t, 1, result.FailedCount())
			records := result.RetryableRecords
			wantStatus, wantCode := pkglogger.DeliveryFailed, 400
			switch scenario {
			case "creation denied":
				records = result.DeadRecords
				wantCode = 403
			case "second put canceled":
				records = result.QuarantineRecords
				wantStatus = pkglogger.DeliveryUnconfirmed
				wantCode = 0
			}
			require.Len(t, records, 1)
			require.Equal(t, wantCode, records[0].StatusCode)
			report := result.TargetDeliveryResult("cloudwatchlogs")
			require.Equal(t, wantStatus, report.Status)
			if scenario != "second put canceled" {
				require.Equal(t, 2, calls)
				var creationFailure *backend.AutoCreateLogStreamError
				require.ErrorAs(t, records[0].Cause, &creationFailure)
				require.ErrorIs(t, report.Err, creationFailure.PutError)
				require.ErrorIs(t, report.Err, creationFailure.CreateError)
				// 自動作成を始めた最初の送信失敗は、streamが存在しないことを表します。
				require.True(t, backend.IsResourceNotFound(creationFailure.PutError), "put error = %v", creationFailure.PutError)
				require.Equal(t, records[0].Cause.Error(), records[0].FailureReason)
			} else {
				require.Equal(t, 3, calls)
			}
			if scenario != "creation denied" {
				require.ErrorIs(t, report.Err, context.Canceled)
			}
			payload := records[0].Payload
			require.Contains(t, payload["line"], "keep original log")
		})
	}
}
