package logspool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	spoolsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestOpenSearchUncertainResponseQuarantinesWithoutReplay(t *testing.T) {
	for _, tt := range []struct {
		name      string
		response  string
		wantError bool
	}{
		{name: "truncated JSON", response: `{"items":`},
		{name: "missing document outcome", response: `{"errors":false,"items":[]}`, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				calls.Add(1)
				// backendがdocumentを受理した可能性があります。この不確かな結果をoperatorが
				// 解決するまで、自動replayは待たなければなりません。
				_, _ = io.WriteString(w, tt.response)
			}))
			defer server.Close()

			ctx := context.Background()
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			clock := fixedSpoolClock{now: now}
			cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "response-recovery", dir: "opensearch"}
			store, err := fsstorestore.NewSpoolStore(clock, cfg, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			payload, err := json.Marshal(opensearch.DocumentWriteRequest{
				Index: "logs", ID: "doc-1", Op: opensearch.WriteOpCreate,
				Doc: map[string]any{"message": "keep until outcome is known"},
			})
			require.NoError(t, err)
			require.NoError(t, store.StoreByState(ctx, spool.StateActive, []spool.SpoolRecord{{
				OccurredAt: now, RequestID: "request-1", Payload: payload,
			}}))
			stateStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			client, err := opensearch.New(opensearch.Config{
				Endpoint: server.URL, Timeout: time.Second,
				AuthConfig: opensearch.AuthConfig{Type: opensearch.AuthTypeNone},
			})
			require.NoError(t, err)
			sender, err := spoolsender.NewOpensearchSpoolSender(clock, client, 5*1024*1024)
			require.NoError(t, err)
			spoolWorker, err := worker.New(stateStore, activeSpoolWriter{store: store}, sender)
			require.NoError(t, err)
			spoolWorker.TargetBackendService = "opensearch"
			runner, err := NewRunner(spoolWorker, newTestLifecycleManager(&spoolLifecycleStoreStub{}),
				runnerConfigStub{}, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)

			first, err := runner.RunOnce(ctx)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, first.ProcessReport.QuarantineRecordCount)
			require.Zero(t, first.ProcessReport.RetryRecordCount)
			quarantined := readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateQuarantine))
			require.Len(t, quarantined, 1)
			require.JSONEq(t, string(payload), string(quarantined[0].Payload))
			require.Equal(t, "request-1", quarantined[0].RequestID)
			require.Equal(t, 1, quarantined[0].ResendAttemptCount)
			require.NotEmpty(t, quarantined[0].LastFailureReason, "retain the reason for operator inspection")

			second, err := runner.RunOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, worker.ProcessStatusNoTarget, second.ProcessReport.Status)
			for _, state := range []spool.State{spool.StateActive, spool.StateProcessing, spool.StateDead, spool.StateDone} {
				require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, state)), "unexpected records in %s", state)
			}
			require.Equal(t, quarantined, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateQuarantine)))
			require.EqualValues(t, 1, calls.Load(), "quarantined documents must not be sent in a later cycle")
		})
	}
}
