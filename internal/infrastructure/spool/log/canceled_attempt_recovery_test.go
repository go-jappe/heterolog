package logspool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	lokisender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

type spoolCancellationTransport func(*http.Request) (*http.Response, error)

func (f spoolCancellationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// 実際のHTTP encoder、file store、transition recoveryを動かして検証します。
// 同じworkerがcancelを越えて動き続けます。後続の送信の前に完了済みの分類を
// 回復するのに、再起動もin-memory guardの消失も必要ありません。
func TestSpoolPreservesAttemptOutcomeAcrossCancellationAndTransitionRecovery(t *testing.T) {
	const transitionRetryDelay = time.Nanosecond
	for _, tt := range []struct {
		name           string
		status         int
		attemptedState spool.State
	}{
		{"unknown", 0, spool.StateQuarantine},
		// 400は入力不備または一部受理の可能性があるため、取消後もquarantineのままです。
		{"known rejected push", 400, spool.StateQuarantine},
		{"known permanent rejection", 401, spool.StateDead},
		{"known throttling", 429, spool.StateActive},
		{"known success", 204, spool.StateDone},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// 状態遷移retryの期限はtime.Nowで判定します。仮想時計でRetryDelayの経過を明示し、
			// OSの時計の分解能（Windowsでは約15.6ms）で再試行の可否が変わらないようにします。
			synctestFromWallClockForTest(t, func(t *testing.T) {
				now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
				cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "cancel-recovery", dir: "loki"}
				clk := fixedSpoolClock{now: now}
				store, err := fsstore.NewSpoolStore(clk, cfg, observer.NopTracer{}, observer.NopMeter{})
				require.NoError(t, err)
				firstLine, laterLine := "attempted-"+strings.Repeat("a", 200), "unstarted-"+strings.Repeat("b", 200)
				var records []spool.SpoolRecord
				for i, line := range []string{firstLine, laterLine} {
					payload, err := json.Marshal(map[string]any{"labels": map[string]string{"app": "recovery"}, "timestamp": now.Add(time.Duration(i) * time.Second), "line": line})
					require.NoError(t, err)
					records = append(records, spool.SpoolRecord{OccurredAt: now, RequestID: line[:9], Payload: payload})
				}
				require.NoError(t, store.StoreByState(context.Background(), spool.StateActive, records))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var sent []string
				client, err := loki.New(loki.Config{Endpoint: "http://loki.test", HTTPClient: &http.Client{Transport: spoolCancellationTransport(func(r *http.Request) (*http.Response, error) {
					var body struct {
						Streams []struct {
							Values [][2]string `json:"values"`
						} `json:"streams"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					require.Len(t, body.Streams, 1)
					require.Len(t, body.Streams[0].Values, 1, "configured request limit separates the two records")
					sent = append(sent, body.Streams[0].Values[0][1])
					status := http.StatusNoContent
					if len(sent) == 1 {
						cancel()
						if tt.status == 0 {
							return nil, context.Canceled
						}
						status = tt.status
					}
					return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
				})}})
				require.NoError(t, err)
				sender, err := lokisender.NewLokiSpoolSender(clk, client, infraloki.PushLimits{MaxLineBytes: 256, MaxPushRequestBytes: 400})
				require.NoError(t, err)
				stateStore, err := fsworker.NewStateStore(cfg)
				require.NoError(t, err)
				retry, err := worker.NewProcessingFileTransitionRetry(worker.NewProcessingFileTransitionRetryConfigAdapter(worker.ProcessingFileTransitionRetryConfig{RetryDelay: transitionRetryDelay, MaxEntries: 10}))
				require.NoError(t, err)
				w, err := worker.New(stateStore, activeSpoolWriter{store: store}, sender, worker.WithProcessingFileTransitionRetry(retry))
				require.NoError(t, err)
				_, err = w.RunOnceSpoolFileProcess(ctx)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, []string{firstLine}, sent)
				// 取消で保留した後処理は、RetryDelayが経過してから再試行できます。
				time.Sleep(transitionRetryDelay)
				_, handled, err := w.RunOnceProcessingFileTransitionRetry(context.Background())
				require.NoError(t, err)
				require.True(t, handled)

				// すべての入力は元のpayloadのまま、ちょうど1回だけ回復されます。
				// resend metadataを更新するのは実際の試行だけで、後続のrecordはactiveです。
				found := make(map[string]spool.State)
				for _, state := range []spool.State{spool.StateActive, spool.StateProcessing, spool.StateQuarantine, spool.StateDead, spool.StateDone} {
					for _, record := range readIntegrationSpoolRecords(t, stateRoot(cfg, state)) {
						require.NotContains(t, found, record.RequestID)
						found[record.RequestID] = state
						if record.RequestID == records[0].RequestID {
							require.Equal(t, records[0].Payload, record.Payload)
							require.Equal(t, 1, record.ResendAttemptCount)
						} else {
							require.Equal(t, records[1].RequestID, record.RequestID)
							require.Equal(t, records[1].Payload, record.Payload)
							require.Zero(t, record.ResendAttemptCount)
						}
					}
				}
				require.Equal(t, map[string]spool.State{records[0].RequestID: tt.attemptedState, records[1].RequestID: spool.StateActive}, found)
				for range 3 {
					_, err := w.RunOnceSpoolFileProcess(context.Background())
					require.NoError(t, err)
				}
				wantSent := []string{firstLine, laterLine}
				if tt.status == 429 {
					wantSent = []string{firstLine, firstLine, laterLine}
				}
				require.Equal(t, wantSent, sent, "replay only explicit retryable outcomes and unstarted records")
				require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateActive)))
				require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateProcessing)))
			})
		})
	}
}
