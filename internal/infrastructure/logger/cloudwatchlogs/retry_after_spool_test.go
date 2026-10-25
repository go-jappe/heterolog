package cloudwatchlogs

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	spoolsendercloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/cloudwatchlogs"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

// 通常送信がthrottlingとRetry-Afterを受け取った場合は、spoolへ退避したrecordにも期限を保存し、
// 初回のspool再送もその時刻まで行わないことを確認します。期限は応答を受け取った
// 時刻から数えるため、送信開始から応答までの時間も待機に含めます。
func TestCloudWatchLogsNormalSendRetryAfterDelaysFirstSpoolReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		tracer := observer.NopTracer{}
		meter := observer.NopMeter{}
		utc := clock.NewUTCClock()
		spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{spoolPath: t.TempDir(), target: "retry-after", dir: "cloudwatchlogs"}
		failureHandler, err := BuildFailureHandler(ctx, utc, cloudWatchLogsSpoolRunnerFlushManagerConfig(spoolCfg), nil, tracer, meter, nil, nil, nil)
		require.NoError(t, err)
		// 途中のassertionが失敗してもspool handlerを終了し、bubble内の処理を残しません。
		// Drainは完了後の再呼び出しで何もしません。
		t.Cleanup(func() { _ = failureHandler.Drain(context.Background()) })
		requests := 0
		client := newContinuationHTTPClient(t, encodingLimitTransport(func(r *http.Request) (*http.Response, error) {
			requests++
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return nil, err
			}
			time.Sleep(time.Second)
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Status:     "400 Bad Request",
				Header: http.Header{
					"Retry-After":      []string{"30"},
					"X-Amzn-Errortype": []string{"ThrottlingException"},
				},
				Body:    io.NopCloser(strings.NewReader(`{"__type":"ThrottlingException","message":"Rate exceeded"}`)),
				Request: r,
			}, nil
		}))
		flushManager := newTestCloudWatchLogsFlushManager(client, failureHandler)
		flushManager.Clock = utc

		sentAt := utc.Now()
		require.NoError(t, flushManager.Put(ctx, newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "wait for retry-after", sentAt)))
		require.Equal(t, 1, requests)
		notBefore := sentAt.Add(time.Second + 30*time.Second)
		active := readCloudWatchLogsLoggerSpoolRecords(t, cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive))
		require.Len(t, active, 1)
		require.True(t, active[0].NotBefore.Equal(notBefore), "NotBefore = %s, want %s", active[0].NotBefore, notBefore)

		spoolStore, err := fsstorestore.NewSpoolStore(utc, spoolCfg, tracer, meter)
		require.NoError(t, err)
		stateStore, err := fsworker.NewStateStore(spoolCfg)
		require.NoError(t, err)
		resendClient := &recordingCloudWatchLogsSpoolResendClient{}
		spoolSender, err := spoolsendercloudwatchlogs.NewCloudWatchLogsSpoolSender(utc, resendClient)
		require.NoError(t, err)
		replayWorker, err := spoolworker.New(stateStore, cloudWatchLogsActiveSpoolWriter{store: spoolStore}, spoolSender)
		require.NoError(t, err)

		deferred, err := replayWorker.RunOnceSpoolFileProcess(ctx)
		require.NoError(t, err)
		require.Equal(t, spoolworker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
		require.Zero(t, resendClient.putCalls, "the first replay waits for Retry-After")

		time.Sleep(time.Until(notBefore))
		replayed, err := replayWorker.RunOnceSpoolFileProcess(ctx)
		require.NoError(t, err)
		require.Equal(t, spoolworker.ProcessStatusCompleted, replayed.Status)
		require.Equal(t, 1, replayed.SuccessRecordCount)
		require.Equal(t, 1, resendClient.putCalls)
		require.NoError(t, flushManager.Close(ctx))
	})
}
