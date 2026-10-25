package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	wrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	backend "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

type credentialsFunc func(context.Context) (aws.Credentials, error)

func (f credentialsFunc) Retrieve(ctx context.Context) (aws.Credentials, error) { return f(ctx) }

func TestCredentialCancellationSpoolOutcomeTracksHTTPDispatch(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{"", pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		for _, tt := range []struct {
			name              string
			credentialsFailAt int
			transportCancelAt int
			autoCreate        bool
			wantTargets       []string
			wantStatus        spool.RecordOutcomeStatus
		}{
			{"first credentials", 1, 0, false, nil, spool.RecordOutcomeStatusRetryable},
			{"creation credentials", 2, 0, true, []string{"PutLogEvents"}, spool.RecordOutcomeStatusRetryable},
			{"second put credentials", 3, 0, true, []string{"PutLogEvents", "CreateLogStream"}, spool.RecordOutcomeStatusRetryable},
			{"first put in flight", 0, 1, false, []string{"PutLogEvents"}, spool.RecordOutcomeStatusInvalid},
			{"second put in flight", 0, 3, true, []string{"PutLogEvents", "CreateLogStream", "PutLogEvents"}, spool.RecordOutcomeStatusInvalid},
			{"credentials ready", 0, 0, false, []string{"PutLogEvents"}, spool.RecordOutcomeStatusSuccess},
			{"created", 0, 0, true, []string{"PutLogEvents", "CreateLogStream", "PutLogEvents"}, spool.RecordOutcomeStatusSuccess},
		} {
			t.Run(string(mode)+"/"+tt.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var targets []string
					var puts [][]byte
					credentialsCalls := 0
					raw, err := backend.New(backend.Config{
						Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test", AutoCreateLogStream: tt.autoCreate,
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
							target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "Logs_20140328.")
							targets = append(targets, target)
							body, err := io.ReadAll(r.Body)
							require.NoError(t, err)
							if target == "PutLogEvents" {
								puts = append(puts, body)
							}
							if len(targets) == tt.transportCancelAt {
								cancel()
								return nil, ctx.Err()
							}
							code, response := 200, `{}`
							if tt.autoCreate && len(targets) == 1 {
								code, response = 400, `{"__type":"ResourceNotFoundException"}`
							}
							return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
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
					if tt.wantStatus == spool.RecordOutcomeStatusSuccess {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, context.Canceled)
						require.ErrorIs(t, report[0].Err, context.Canceled)
					}
					require.NoError(t, report.Validate(1))
					require.Equal(t, tt.wantStatus, report[0].Status)
					require.Equal(t, tt.wantTargets, targets, "no HTTP attempt or next retry may start after credential cancellation")
					require.Equal(t, before, []byte(record.Payload))
					var unsent *backend.RequestNotSentError
					if tt.credentialsFailAt != 0 {
						require.Equal(t, tt.credentialsFailAt, credentialsCalls)
						require.ErrorAs(t, report[0].Err, &unsent)
						require.ErrorIs(t, unsent.Err, context.Canceled)
						if tt.credentialsFailAt == 3 {
							require.True(t, backend.IsResourceNotFound(unsent.PreviousPutError))
							require.ErrorIs(t, report[0].Err, unsent.PreviousPutError)
						}
					} else {
						require.False(t, errors.As(report[0].Err, &unsent))
					}
					for _, body := range puts {
						var request struct {
							LogStreamName string
							LogEvents     []struct{ Message string }
						}
						require.NoError(t, json.Unmarshal(body, &request))
						require.Equal(t, "stream-a", request.LogStreamName)
						require.Len(t, request.LogEvents, 1)
						require.Equal(t, "original message", request.LogEvents[0].Message)
					}
					if len(puts) == 2 {
						require.Equal(t, puts[0], puts[1])
					}
				})
			})
		}
	}
}

func TestCredentialRetryWaitCancellationRemainsReplayable(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		t.Run(string(mode), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				credentialsCalls, httpCalls := 0, 0
				raw, err := backend.New(backend.Config{
					Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test",
					CredentialsProvider: credentialsFunc(func(context.Context) (aws.Credentials, error) {
						credentialsCalls++
						go func() { time.Sleep(10 * time.Millisecond); cancel() }()
						return aws.Credentials{}, io.EOF
					}),
					HTTPClient: &http.Client{Transport: autoCreateTransport(func(r *http.Request) (*http.Response, error) {
						httpCalls++
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
					})},
				})
				require.NoError(t, err)
				client, err := wrapper.BuildClientChain(raw, waitRetryConfig{mode: mode}, nil, nil)
				require.NoError(t, err)
				sender, err := NewCloudWatchLogsSpoolSender(fixedClock{now: time.Now()}, client)
				require.NoError(t, err)
				record := cloudWatchLogsSpoolRecord(t, "entry-a", "keep after credentials EOF")
				before := append([]byte(nil), record.Payload...)
				report, err := sender.Send(ctx, []spool.SpoolRecord{record})
				require.ErrorIs(t, err, context.Canceled)
				require.NoError(t, report.Validate(1))
				require.Equal(t, spool.RecordOutcomeStatusRetryable, report[0].Status)
				require.ErrorIs(t, report[0].Err, io.EOF)
				require.ErrorIs(t, report[0].Err, context.Canceled)
				require.Equal(t, 1, credentialsCalls)
				require.Zero(t, httpCalls)
				require.Equal(t, before, []byte(record.Payload))
			})
		})
	}
}

func TestCredentialFailureAfterRetryPreservesEarlierDispatchEvidence(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		for _, priorUnknown := range []bool{false, true} {
			for _, next := range []string{"canceled", "EOF", "unavailable", "recovered", "creation canceled", "second put credentials canceled"} {
				t.Run(string(mode)+"/unknown="+strconv.FormatBool(priorUnknown)+"/"+next, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						cause := errors.New("credential provider unavailable")
						if strings.HasSuffix(next, "canceled") {
							cause = context.Canceled
						}
						if next == "EOF" {
							cause = io.ErrUnexpectedEOF
						}
						credentialsCalls, httpCalls := 0, 0
						failAt, wantHTTP := 2, 1
						if next == "creation canceled" {
							failAt, wantHTTP = 3, 2
						}
						if next == "second put credentials canceled" {
							failAt, wantHTTP = 4, 3
						}
						autoCreate := failAt > 2
						var bodies [][]byte
						raw, err := backend.New(backend.Config{
							Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test", AutoCreateLogStream: autoCreate,
							CredentialsProvider: credentialsFunc(func(context.Context) (aws.Credentials, error) {
								credentialsCalls++
								if credentialsCalls >= failAt && next != "recovered" {
									if strings.HasSuffix(next, "canceled") {
										cancel()
									}
									return aws.Credentials{}, cause
								}
								return aws.Credentials{AccessKeyID: "example", SecretAccessKey: "example", CanExpire: true, Expires: time.Now().Add(time.Second)}, nil
							}),
							HTTPClient: &http.Client{Transport: autoCreateTransport(func(r *http.Request) (*http.Response, error) {
								httpCalls++
								body, err := io.ReadAll(r.Body)
								require.NoError(t, err)
								bodies = append(bodies, body)
								time.Sleep(2 * time.Second) // expire short-lived credentials before retry
								if httpCalls == 1 && priorUnknown {
									return nil, io.EOF
								}
								code, response := 200, `{}`
								if httpCalls == 1 {
									code = 503
								}
								if autoCreate && httpCalls == 2 {
									code, response = 400, `{"__type":"ResourceNotFoundException"}`
								}
								return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
							})},
						})
						require.NoError(t, err)
						client, err := wrapper.BuildClientChain(raw, waitRetryConfig{mode: mode}, nil, nil)
						require.NoError(t, err)
						sender, err := NewCloudWatchLogsSpoolSender(fixedClock{now: time.Now()}, client)
						require.NoError(t, err)
						record := cloudWatchLogsSpoolRecord(t, "entry-a", "preserve earlier request")
						before := append([]byte(nil), record.Payload...)
						report, err := sender.Send(ctx, []spool.SpoolRecord{record})
						require.NoError(t, report.Validate(1))
						require.Equal(t, before, []byte(record.Payload))
						if strings.HasSuffix(next, "canceled") {
							require.ErrorIs(t, err, context.Canceled)
						} else {
							require.NoError(t, err)
						}
						if next == "recovered" {
							require.Equal(t, spool.RecordOutcomeStatusSuccess, report[0].Status)
							require.Equal(t, 2, httpCalls)
							require.Equal(t, 2, credentialsCalls)
							require.Equal(t, bodies[0], bodies[1])
							return
						}
						require.Equal(t, wantHTTP, httpCalls, "credential cancellation must not dispatch its HTTP request")
						wantCredentials := failAt
						if next == "EOF" && mode == pkglogger.RetryClientModeRetry {
							wantCredentials = 3
						}
						require.Equal(t, wantCredentials, credentialsCalls)
						require.ErrorIs(t, report[0].Err, cause)
						if priorUnknown {
							require.Equal(t, spool.RecordOutcomeStatusInvalid, report[0].Status)
							require.ErrorIs(t, report[0].Err, io.EOF)
						} else {
							require.Equal(t, spool.RecordOutcomeStatusRetryable, report[0].Status)
							var refusal *backend.HTTPStatusError
							require.ErrorAs(t, report[0].Err, &refusal)
							wantCode := 503
							if autoCreate {
								wantCode = 400
							}
							require.Equal(t, wantCode, refusal.Code)
						}
					})
				})
			}
		}
	}
}
