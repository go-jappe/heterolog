package loki

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	backendloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	spoolsenderloki "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/loki"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

// 通常送信がRetry-Afterを受け取った場合は、spoolへ退避したrecordにも期限を保存し、
// 初回のspool再送もその時刻まで行わないことを確認します。期限は応答を受け取った
// 時刻から数えるため、送信開始から応答までの時間も待機に含めます。
func TestLokiNormalSendRetryAfterDelaysFirstSpoolReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		tracer := observer.NopTracer{}
		meter := observer.NopMeter{}
		utc := clock.NewUTCClock()
		spoolCfg := lokiLoggerSpoolTestConfig{spoolPath: t.TempDir(), target: "retry-after", dir: "loki"}
		failureHandler, err := BuildFailureHandler(ctx, utc, lokiSpoolRunnerFlushManagerConfig(spoolCfg), nil, tracer, meter, nil, nil, nil)
		require.NoError(t, err)
		// 途中のassertionが失敗してもspool handlerを終了し、bubble内の処理を残しません。
		// Drainは完了後の再呼び出しで何もしません。
		t.Cleanup(func() { _ = failureHandler.Drain(context.Background()) })
		const retryAfter = 30 * time.Second
		client := &recordingFlushManagerLokiClient{pushFunc: func([]pkgloki.Stream) error {
			time.Sleep(time.Second)
			return &pkgloki.HTTPStatusError{
				Code:               http.StatusTooManyRequests,
				Status:             "429 Too Many Requests",
				RetryAfterDuration: retryAfter,
			}
		}}
		flushManager := newTestLokiFlushManagerWithFailureHandler(client, failureHandler)
		flushManager.Clock = utc

		sentAt := utc.Now()
		require.NoError(t, flushManager.Push(ctx, newTestLokiLogEntry(pkglogger.LogLevelInfo, "wait for retry-after")))
		notBefore := sentAt.Add(time.Second + retryAfter)
		active := readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive))
		require.Len(t, active, 1)
		require.True(t, active[0].OccurredAt.Equal(sentAt))
		require.True(t, active[0].NotBefore.Equal(notBefore), "NotBefore = %s, want %s", active[0].NotBefore, notBefore)

		spoolStore, err := fsstorestore.NewSpoolStore(utc, spoolCfg, tracer, meter)
		require.NoError(t, err)
		stateStore, err := fsworker.NewStateStore(spoolCfg)
		require.NoError(t, err)
		resendClient := &recordingLokiSpoolResendClient{}
		spoolSender, err := spoolsenderloki.NewLokiSpoolSender(utc, resendClient, backendloki.PushLimits{
			MaxLineBytes:        256 * 1024,
			MaxPushRequestBytes: 4 * 1024 * 1024,
		})
		require.NoError(t, err)
		replayWorker, err := spoolworker.New(stateStore, lokiActiveSpoolWriter{store: spoolStore}, spoolSender)
		require.NoError(t, err)

		deferred, err := replayWorker.RunOnceSpoolFileProcess(ctx)
		require.NoError(t, err)
		require.Equal(t, spoolworker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
		require.Zero(t, resendClient.pushCalls, "the first replay waits for Retry-After")

		time.Sleep(time.Until(notBefore))
		replayed, err := replayWorker.RunOnceSpoolFileProcess(ctx)
		require.NoError(t, err)
		require.Equal(t, spoolworker.ProcessStatusCompleted, replayed.Status)
		require.Equal(t, 1, replayed.SuccessRecordCount)
		require.Equal(t, 1, resendClient.pushCalls)
		require.NoError(t, flushManager.Close(ctx))
	})
}
