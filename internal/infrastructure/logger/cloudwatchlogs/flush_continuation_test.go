package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

type continuationPutBody struct {
	LogStreamName string `json:"logStreamName"`
	LogEvents     []struct {
		Timestamp int64  `json:"timestamp"`
		Message   string `json:"message"`
	} `json:"logEvents"`
}

type continuationFailureHandler struct {
	errs    []error
	results []loggerfailure.SendResult
}

func (h *continuationFailureHandler) HandleFailure(_ context.Context, result loggerfailure.SendResult) error {
	h.results = append(h.results, result)
	if len(h.results) <= len(h.errs) {
		return h.errs[len(h.results)-1]
	}
	return nil
}

func newContinuationHTTPClient(t *testing.T, transport http.RoundTripper) *pkgcloudwatchlogs.HTTPClient {
	t.Helper()
	client, err := pkgcloudwatchlogs.New(pkgcloudwatchlogs.Config{
		Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test",
		AccessKeyID: "test", SecretAccessKey: "test", HTTPClient: &http.Client{Transport: transport},
	})
	require.NoError(t, err)
	return client
}

func TestBufferedCloudWatchContinuesAfterDurableErrorsAndAggregatesThem(t *testing.T) {
	ctx := context.Background()
	firstErr, secondErr := errors.New("first archive write failed"), errors.New("second archive write failed")
	failures := &continuationFailureHandler{errs: []error{firstErr, secondErr}}
	composite := mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{failures})
	var requests []continuationPutBody
	client := newContinuationHTTPClient(t, encodingLimitTransport(func(request *http.Request) (*http.Response, error) {
		var body continuationPutBody
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			return nil, err
		}
		requests = append(requests, body)
		status, response := http.StatusOK, "{}"
		if len(requests) == 2 || len(requests) == 4 {
			status, response = http.StatusTooManyRequests, `{"__type":"ThrottlingException"}`
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
	}))
	config := NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
		LogGroupName: "/app/test", LogStreamName: "stream-a",
	}, pkgcloudwatchlogs.StreamRoutingHourly)
	logger, err := NewCloudWatchLogsLogger(ctx, newTestCloudWatchLogsLoggerConfig(4, time.Hour, false),
		client, config, newTestCloudWatchLogsResourceProvider(), composite,
		observer.NopTracer{}, observer.NopMeter{}, &LoggerBase{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = logger.Close(context.Background()) })
	base := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	messages := []string{"first succeeds", "second fails", "third was already accepted", "fourth fails"}
	// 3番目の物理requestは、このflushを起こす呼び出しより前に受理されます。
	for _, index := range []int{2, 0, 3} {
		require.NoError(t, handleTestLogAt(ctx, logger, base.Add(time.Duration(index)*time.Hour), pkglogger.LogLevelInfo, messages[index]))
	}
	err = handleTestLogAt(ctx, logger, base.Add(time.Hour), pkglogger.LogLevelInfo, messages[1])
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.NoError(t, logger.Close(context.Background()))
	require.Len(t, requests, 4, "Close must not resend completed requests")
	for i, request := range requests {
		require.Equal(t, "stream-a/"+base.Add(time.Duration(i)*time.Hour).Format("2006/01/02/15"), request.LogStreamName)
		require.Len(t, request.LogEvents, 1)
		require.Contains(t, request.LogEvents[0].Message, messages[i])
		require.Equal(t, base.Add(time.Duration(i)*time.Hour).UnixMilli(), request.LogEvents[0].Timestamp)
	}
	require.Len(t, failures.results, 2)
	for i, result := range failures.results {
		require.Len(t, result.RetryableRecords, 1)
		require.Empty(t, result.DeadRecords)
		require.Empty(t, result.QuarantineRecords)
		require.Equal(t, messages[1+2*i], result.RetryableRecords[0].Payload["message"])
	}
	require.Empty(t, cloudWatchLogsBufferedMessages(logger))
}

func TestBufferedCloudWatchPersistsStoppedRequestsByKnownOutcome(t *testing.T) {
	for _, outcome := range []string{"unknown", "accepted", "rejected"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{spoolPath: t.TempDir(), target: "cancellation", dir: "cloudwatchlogs"}
			config := cloudWatchLogsSpoolRunnerFlushManagerConfigWithRoutingGranularity(spoolCfg, pkgcloudwatchlogs.StreamRoutingHourly)
			base := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
			clock := fixedCloudWatchLogsClock{t: base.Add(3 * time.Hour)}
			failureHandler, err := BuildFailureHandler(context.Background(), clock, config, nil,
				observer.NopTracer{}, observer.NopMeter{}, nil, nil, nil)
			require.NoError(t, err)
			var requests []continuationPutBody
			client := newContinuationHTTPClient(t, encodingLimitTransport(func(request *http.Request) (*http.Response, error) {
				var body continuationPutBody
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					return nil, err
				}
				requests = append(requests, body)
				cancel()
				if outcome == "unknown" {
					return nil, request.Context().Err()
				}
				status, response := http.StatusOK, "{}"
				if outcome == "rejected" {
					status, response = http.StatusTooManyRequests, `{"__type":"ThrottlingException"}`
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
			}))
			logger, err := NewCloudWatchLogsLogger(context.Background(), newTestCloudWatchLogsLoggerConfig(2, time.Hour, false),
				client, config, newTestCloudWatchLogsResourceProvider(), failureHandler,
				observer.NopTracer{}, observer.NopMeter{}, &LoggerBase{}, WithClock(clock))
			require.NoError(t, err)
			t.Cleanup(func() { _ = logger.Close(context.Background()) })
			require.NoError(t, handleTestLogAt(ctx, logger, base.Add(time.Hour), pkglogger.LogLevelInfo, "unstarted"))
			err = handleTestLogAt(ctx, logger, base, pkglogger.LogLevelInfo, "attempted")
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, logger.Close(context.Background()))
			require.Len(t, requests, 1, "stopped context must prevent the second request and Close must not resend")
			require.Len(t, requests[0].LogEvents, 1)
			require.Contains(t, requests[0].LogEvents[0].Message, "attempted")

			want := map[pkgspool.State][]string{
				pkgspool.StateActive: {"unstarted"},
			}
			if outcome == "unknown" {
				want[pkgspool.StateQuarantine] = []string{"attempted"}
			} else if outcome == "rejected" {
				want[pkgspool.StateActive] = []string{"attempted", "unstarted"}
			}
			for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
				records := readCloudWatchLogsLoggerSpoolRecords(t, cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, state))
				var messages []string
				for _, record := range records {
					doc := unmarshalCloudWatchLogsLoggerSpoolDocument(t, record)
					message := doc["message"].(string)
					messages = append(messages, message)
					occurredAt := base
					if message == "unstarted" {
						occurredAt = base.Add(time.Hour)
						// 呼び出し元の停止で送信しなかったrequestは、全backend共通の未送信codeで保存します。
						require.Equal(t, string(retryable.RetryReasonRequestNotSent), record.InitialFailureCode)
					}
					require.Equal(t, occurredAt.Format(time.RFC3339Nano), doc["timestamp"])
					require.Equal(t, "stream-a/"+occurredAt.Format("2006/01/02/15"), doc["log_stream"])
					require.Contains(t, doc["line"], message)
				}
				require.ElementsMatch(t, want[state], messages, "state %s", state)
			}
		})
	}
}

func TestCloudWatchDeliveryReportsDurableHandlingAfterCanceledAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &recordingCloudWatchLogsClient{onPut: cancel, err: context.Canceled}
	archive := &fixedCloudWatchLogsFailureHandler{report: pkglogger.DeliveryReport{
		Durables: []pkglogger.DurableResult{{Kind: pkglogger.DurableArchive, Status: pkglogger.DurableSucceeded}},
	}}
	manager := newTestCloudWatchLogsFlushManagerWithReportingFailureHandler(client,
		mustNewCloudWatchLogsReportingFailureComposite(t, []flushfailure.FailureHandlerWithReport{archive}))
	report, err := manager.PutWithReport(ctx, newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "uncertain delivery"))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, archive.reportCalls)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryUnconfirmed, report.Targets[0].Status)
	require.Len(t, report.Durables, 1)
	require.Equal(t, pkglogger.DurableSucceeded, report.Durables[0].Status)
}
