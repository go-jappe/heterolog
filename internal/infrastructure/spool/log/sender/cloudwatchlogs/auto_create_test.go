package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	wrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	backend "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

type autoCreateTransport func(*http.Request) (*http.Response, error)

func (f autoCreateTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAutoCreateSpoolOutcomeTracksLogAcceptance(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{"", pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		for _, scenario := range []string{"creation canceled", "creation denied", "second put canceled", "second put malformed", "created", "already exists", "auto create disabled"} {
			t.Run(string(mode)+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var targets []string
				var puts [][]byte
				raw, err := backend.New(backend.Config{
					Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test", AccessKeyID: "example", SecretAccessKey: "example", AutoCreateLogStream: scenario != "auto create disabled",
					HTTPClient: &http.Client{Transport: autoCreateTransport(func(r *http.Request) (*http.Response, error) {
						body, err := io.ReadAll(r.Body)
						require.NoError(t, err)
						target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "Logs_20140328.")
						targets = append(targets, target)
						respond := func(code int, body string) (*http.Response, error) {
							return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
						}
						if target == "PutLogEvents" {
							puts = append(puts, body)
						}
						if len(targets) == 1 {
							return respond(400, `{"__type":"ResourceNotFoundException","message":"stream missing"}`)
						}
						if target == "CreateLogStream" {
							switch scenario {
							case "creation canceled":
								cancel()
								return nil, ctx.Err()
							case "creation denied":
								return respond(403, `{"__type":"AccessDeniedException"}`)
							case "already exists":
								return respond(400, `{"__type":"ResourceAlreadyExistsException"}`)
							default:
								return respond(200, `{}`)
							}
						}
						switch scenario {
						case "second put canceled":
							cancel()
							return nil, ctx.Err()
						case "second put malformed":
							return respond(200, `{`)
						default:
							return respond(200, `{}`)
						}
					})},
				})
				require.NoError(t, err)
				var client backend.Client = raw
				if mode != "" {
					client, err = wrapper.BuildClientChain(raw, waitRetryConfig{mode: mode}, nil, nil)
					require.NoError(t, err)
				}
				sender, err := NewCloudWatchLogsSpoolSender(fixedClock{now: time.Now()}, client)
				require.NoError(t, err)
				record := cloudWatchLogsSpoolRecord(t, "entry-a", "original message")
				before := append([]byte(nil), record.Payload...)
				report, err := sender.Send(ctx, []spool.SpoolRecord{record})
				if strings.HasSuffix(scenario, "canceled") {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
				require.NoError(t, report.Validate(1))
				require.Equal(t, before, []byte(record.Payload))
				require.False(t, report[0].AttemptedAt.IsZero())
				want := spool.RecordOutcomeStatusSuccess
				wantTargets := []string{"PutLogEvents", "CreateLogStream", "PutLogEvents"}
				switch scenario {
				case "creation canceled":
					want = spool.RecordOutcomeStatusRetryable
					wantTargets = wantTargets[:2]
				case "creation denied":
					want = spool.RecordOutcomeStatusPermanent
					wantTargets = wantTargets[:2]
				case "second put canceled", "second put malformed":
					want = spool.RecordOutcomeStatusInvalid
				case "auto create disabled":
					want = spool.RecordOutcomeStatusPermanent
					wantTargets = wantTargets[:1]
				}
				require.Equal(t, want, report[0].Status)
				require.Equal(t, wantTargets, targets, "never send logs after a failed creation or repeat a completed attempt")
				if strings.HasPrefix(scenario, "creation ") {
					var creationFailure *backend.AutoCreateLogStreamError
					require.ErrorAs(t, report[0].Err, &creationFailure)
					require.True(t, backend.IsResourceNotFound(creationFailure.PutError))
					require.ErrorIs(t, report[0].Err, creationFailure.PutError)
					require.ErrorIs(t, report[0].Err, creationFailure.CreateError)
					if scenario == "creation canceled" {
						require.ErrorIs(t, report[0].Err, context.Canceled)
					}
				}
				var sent struct {
					LogStreamName string
					LogEvents     []struct{ Message string }
				}
				require.NoError(t, json.Unmarshal(puts[0], &sent))
				require.Equal(t, "stream-a", sent.LogStreamName)
				require.Len(t, sent.LogEvents, 1)
				require.Equal(t, "original message", sent.LogEvents[0].Message)
				if len(puts) == 2 {
					require.Equal(t, puts[0], puts[1], "stream creation must not alter the original log request")
				}
			})
		}
	}
}
