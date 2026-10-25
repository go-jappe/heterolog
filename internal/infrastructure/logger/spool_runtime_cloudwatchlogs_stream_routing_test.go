package logger

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

type cloudWatchLogsRoundTripFunc func(*http.Request) (*http.Response, error)

func (f cloudWatchLogsRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestNewBackendSpoolSupervisorUsesUTCSourceAndKeepsStoredCloudWatchStream(t *testing.T) {
	var sentStreamName string
	httpClient := &http.Client{Transport: cloudWatchLogsRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var body struct {
			LogStreamName string `json:"logStreamName"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode CloudWatch Logs request: %v", err)
		}
		sentStreamName = body.LogStreamName
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Request:    request,
		}, nil
	})}

	loggerConfig := pkglogger.LoggerConfig{
		CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{
				TimeConfig: pkglogger.TimeConfig{
					IsLocalTimeEnabled: true,
					TimeZone:           "Asia/Tokyo",
				},
			},
			CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
				FailureStoreConfig: testSpoolFailureStore(t.TempDir(), "cloudwatchlogs-clock"),
				LogGroupName:       "/app/test",
				LogStreamName:      "container/task-1",
			},
		},
		CloudWatchLogsClient: &pkgcloudwatchlogs.Config{
			Region:          "ap-northeast-1",
			Endpoint:        "https://cloudwatch.example",
			LogGroupName:    "/app/test",
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			HTTPClient:      httpClient,
		},
	}

	attemptedAt := time.Date(2026, 10, 20, 12, 34, 56, 789000000, time.UTC)
	timeSource := clock.NewFixedClock(attemptedAt)
	supervisor, err := newBackendSpoolSupervisor(
		pkglogger.LogOutputCloudWatchLogs,
		loggerConfig,
		testFailureSpoolRuntimeConfig(),
		timeSource,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("newBackendSpoolSupervisor() error = %v", err)
	}
	store, err := fsstorestore.NewSpoolStore(
		timeSource,
		loggerdurable.NewSpoolConfigAdapter(loggerConfig.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig),
		observer.NopTracer{}, observer.NopMeter{},
	)
	require.NoError(t, err)
	payload, err := json.Marshal(map[string]any{
		// spool再送は現在のstream routing設定から再計算せず、初回PutLogEvents時に
		// durable payloadへ保存した実streamをそのまま使用する。
		"log_stream": "container/task-1/2026/08/14/09",
		"timestamp":  attemptedAt.Add(-time.Minute).UTC().Format(time.RFC3339Nano),
		"line":       "spooled event",
	})
	if err != nil {
		t.Fatalf("marshal spool payload: %v", err)
	}
	ctx := context.Background()
	require.NoError(t, store.StoreByState(ctx, pkgspool.StateActive, []pkgspool.SpoolRecord{{
		OccurredAt: attemptedAt.Add(-time.Minute),
		RequestID:  "request-1",
		Payload:    payload,
	}}))
	report, err := supervisor.Runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, worker.ProcessStatusCompleted, report.ProcessReport.Status)
	require.Equal(t, 1, report.ProcessReport.RecordCount)
	require.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	require.Zero(t, report.ProcessReport.RetryRecordCount)

	want := "container/task-1/2026/08/14/09"
	if sentStreamName != want {
		t.Fatalf("sent log stream = %q, want %q", sentStreamName, want)
	}

	donePaths, err := filepath.Glob(filepath.Join(store.RootDir.DoneDir().Path(), "*", "*", "*", "*", "*", "*.hlogspool"))
	require.NoError(t, err)
	require.Len(t, donePaths, 1)
	body, err := os.ReadFile(donePaths[0])
	require.NoError(t, err)
	var doneRecord pkgspool.SpoolRecord
	require.NoError(t, json.Unmarshal(body, &doneRecord))
	require.Equal(t, "request-1", doneRecord.RequestID)
	// 送信結果に記録された時刻を確認し、loggerのlocal timezone設定にかかわらず
	// factoryが渡されたUTC時刻源をsenderへ接続したことを実処理で検証します。
	require.Equal(t, attemptedAt, doneRecord.LastResendAttemptedAt)
	require.Equal(t, time.UTC, doneRecord.LastResendAttemptedAt.Location())
	require.Equal(t, 1, doneRecord.ResendAttemptCount)
}
