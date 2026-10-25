package logspool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	chbackend "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	chsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/clickhouse"
	cwsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/cloudwatchlogs"
	lokisender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/loki"
	ossender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

const retryAfterReplayWait = 30 * time.Second

// retryAfterBackendは、最初のrequestへRetry-Afterを返し、以降のrequestを受理する送信先です。
// 受け取った時刻を記録し、指定時刻より前に再送していないことを確認します。
type retryAfterBackend struct {
	requests []time.Time
}

func (b *retryAfterBackend) throttle() bool {
	b.requests = append(b.requests, time.Now())
	return len(b.requests) == 1
}

type retryAfterTransport func(*http.Request) (*http.Response, error)

func (f retryAfterTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func retryAfterHTTPClient(backend *retryAfterBackend, throttled, accepted func() (int, http.Header, string)) *http.Client {
	return &http.Client{Transport: retryAfterTransport(func(r *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		status, header, body := accepted()
		if backend.throttle() {
			status, header, body = throttled()
		}
		if header == nil {
			header = make(http.Header)
		}
		return &http.Response{
			StatusCode: status,
			Status:     http.StatusText(status),
			Header:     header,
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	})}
}

func retryAfterHeader(extra map[string]string) http.Header {
	header := http.Header{"Retry-After": []string{"30"}}
	for key, value := range extra {
		header.Set(key, value)
	}
	return header
}

// retryAfterOpenSearchClientは、pkg/opensearchがRetry-After付きの429から作るerrorを返します。
// pkg/opensearch.NewはHTTP transportを差し替えられないため、synctest内では応答の変換結果を
// 直接返します。Retry-Afterの解析はpkg/opensearchの試験が確認します。
type retryAfterOpenSearchClient struct {
	backend *retryAfterBackend
}

func (c *retryAfterOpenSearchClient) IndexDocument(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
	return nil, errors.New("unexpected single document request")
}

func (c *retryAfterOpenSearchClient) IndexDocuments(_ context.Context, docs []opensearch.DocumentWriteRequest) (*opensearch.BulkResponse, error) {
	if c.backend.throttle() {
		return nil, &opensearch.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests", After: retryAfterReplayWait}
	}
	items := make([]map[string]opensearch.BulkItemResult, 0, len(docs))
	for _, doc := range docs {
		items = append(items, map[string]opensearch.BulkItemResult{
			string(doc.Op): {Index: doc.Index, ID: doc.ID, Status: http.StatusCreated, Result: "created"},
		})
	}
	return &opensearch.BulkResponse{Items: items}, nil
}

type retryAfterReplayCase struct {
	name string
	// requestsPerReplayは、二件のrecordを再送するときのrequest数です。二件以上なら、
	// 最初のrequestがRetry-Afterを受けた周期では残りのrequestを送信しません。
	requestsPerReplay int
	records           func(t *testing.T, now time.Time) []spool.SpoolRecord
	newSender         func(t *testing.T, clk clock.Clock, backend *retryAfterBackend, records []spool.SpoolRecord) worker.Sender
}

func retryAfterReplayCases() []retryAfterReplayCase {
	return []retryAfterReplayCase{
		{
			name:              "loki",
			requestsPerReplay: 2,
			records: func(t *testing.T, now time.Time) []spool.SpoolRecord {
				var records []spool.SpoolRecord
				for i, line := range []string{"first-" + strings.Repeat("a", 200), "later-" + strings.Repeat("b", 200)} {
					payload, err := json.Marshal(map[string]any{"labels": map[string]string{"app": "retry-after"}, "timestamp": now.Add(time.Duration(i) * time.Second), "line": line})
					require.NoError(t, err)
					records = append(records, spool.SpoolRecord{OccurredAt: now, RequestID: line[:5], Payload: payload})
				}
				return records
			},
			newSender: func(t *testing.T, clk clock.Clock, backend *retryAfterBackend, _ []spool.SpoolRecord) worker.Sender {
				client, err := loki.New(loki.Config{Endpoint: "http://loki.test", HTTPClient: retryAfterHTTPClient(backend,
					func() (int, http.Header, string) { return http.StatusTooManyRequests, retryAfterHeader(nil), "" },
					func() (int, http.Header, string) { return http.StatusNoContent, nil, "" },
				)})
				require.NoError(t, err)
				// request上限により、二件のrecordを別々のpushへ分けます。
				sender, err := lokisender.NewLokiSpoolSender(clk, client, infraloki.PushLimits{MaxLineBytes: 256, MaxPushRequestBytes: 400})
				require.NoError(t, err)
				return sender
			},
		},
		{
			name:              "clickhouse",
			requestsPerReplay: 2,
			records: func(t *testing.T, now time.Time) []spool.SpoolRecord {
				var records []spool.SpoolRecord
				for _, id := range []string{"first", "later"} {
					payload, err := json.Marshal(chbackend.DurablePayload{
						LogRecord:                clickhouse.LogRecord{Timestamp: now, Level: "INFO", Message: id},
						InsertDeduplicationToken: "token-" + id,
						RecordID:                 "record-" + id,
						ReplayMode:               chbackend.DurableReplayModeBatch,
					})
					require.NoError(t, err)
					records = append(records, spool.SpoolRecord{OccurredAt: now, RequestID: id, Payload: payload})
				}
				return records
			},
			newSender: func(t *testing.T, clk clock.Clock, backend *retryAfterBackend, _ []spool.SpoolRecord) worker.Sender {
				client, err := clickhouse.New(clickhouse.Config{Endpoint: "http://clickhouse.test", Table: "logs", HTTPClient: retryAfterHTTPClient(backend,
					func() (int, http.Header, string) { return http.StatusServiceUnavailable, retryAfterHeader(nil), "" },
					func() (int, http.Header, string) { return http.StatusOK, nil, "" },
				)})
				require.NoError(t, err)
				// 重複排除tokenが異なるrecordは、別々のINSERTで送信します。
				sender, err := chsender.NewClickHouseSpoolSender(clk, client, 1024*1024)
				require.NoError(t, err)
				return sender
			},
		},
		{
			name:              "cloudwatchlogs",
			requestsPerReplay: 1,
			records: func(t *testing.T, now time.Time) []spool.SpoolRecord {
				var records []spool.SpoolRecord
				for i, id := range []string{"first", "later"} {
					payload, err := json.Marshal(map[string]any{
						"log_group":  "/app/test",
						"log_stream": "stream-a",
						"timestamp":  now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
						"line":       id,
						"message":    id,
					})
					require.NoError(t, err)
					records = append(records, spool.SpoolRecord{OccurredAt: now, RequestID: id, Payload: payload})
				}
				return records
			},
			newSender: func(t *testing.T, clk clock.Clock, backend *retryAfterBackend, _ []spool.SpoolRecord) worker.Sender {
				client, err := cloudwatchlogs.New(cloudwatchlogs.Config{
					Region: "ap-northeast-1", Endpoint: "https://logs.example.test", LogGroupName: "/app/test",
					AccessKeyID: "test", SecretAccessKey: "test",
					HTTPClient: retryAfterHTTPClient(backend,
						func() (int, http.Header, string) {
							return http.StatusBadRequest, retryAfterHeader(map[string]string{"X-Amzn-Errortype": "ThrottlingException"}),
								`{"__type":"ThrottlingException","message":"Rate exceeded"}`
						},
						func() (int, http.Header, string) { return http.StatusOK, nil, `{}` },
					),
				})
				require.NoError(t, err)
				sender, err := cwsender.NewCloudWatchLogsSpoolSender(clk, client)
				require.NoError(t, err)
				return sender
			},
		},
		{
			name:              "opensearch",
			requestsPerReplay: 2,
			records: func(t *testing.T, now time.Time) []spool.SpoolRecord {
				var records []spool.SpoolRecord
				for _, id := range []string{"first", "later"} {
					payload, err := json.Marshal(opensearch.DocumentWriteRequest{
						Index: "logs", ID: "doc-" + id, Op: opensearch.WriteOpCreate,
						Doc: map[string]any{"message": id},
					})
					require.NoError(t, err)
					records = append(records, spool.SpoolRecord{OccurredAt: now, RequestID: id, Payload: payload})
				}
				return records
			},
			newSender: func(t *testing.T, clk clock.Clock, backend *retryAfterBackend, records []spool.SpoolRecord) worker.Sender {
				var first opensearch.DocumentWriteRequest
				require.NoError(t, json.Unmarshal(records[0].Payload, &first))
				oneDocumentBytes, err := opensearch.BulkRequestSize([]opensearch.DocumentWriteRequest{first})
				require.NoError(t, err)
				// Bulk request上限により、二件のdocumentを別々のrequestへ分けます。
				sender, err := ossender.NewOpensearchSpoolSender(clk, &retryAfterOpenSearchClient{backend: backend}, oneDocumentBytes)
				require.NoError(t, err)
				return sender
			},
		},
	}
}

// 各backendの実際のsenderとfile storeを使い、Retry-Afterの期限より前には同じ送信先へ
// 再送しないことを確認します。期限は再送待ちのrecordへ保存され、再起動後のWorkerも
// そのrecordを読み込んだ時点で送信を控えます。
func TestSpoolReplayHonorsRetryAfterAcrossRestart(t *testing.T) {
	for _, tt := range retryAfterReplayCases() {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				clk := clock.NewUTCClock()
				cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "retry-after", dir: tt.name}
				store, err := fsstorestore.NewSpoolStore(clk, cfg, observer.NopTracer{}, observer.NopMeter{})
				require.NoError(t, err)
				records := tt.records(t, clk.Now())
				require.NoError(t, store.StoreByState(ctx, spool.StateActive, records))
				backend := &retryAfterBackend{}
				sender := tt.newSender(t, clk, backend, records)
				stateStore, err := fsworker.NewStateStore(cfg)
				require.NoError(t, err)
				newWorker := func() *worker.Worker {
					spoolWorker, err := worker.New(stateStore, activeSpoolWriter{store: store}, sender)
					require.NoError(t, err)
					return spoolWorker
				}

				spoolWorker := newWorker()
				throttled, err := spoolWorker.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, worker.ProcessStatusCompleted, throttled.Status)
				require.Equal(t, len(records), throttled.RetryRecordCount)
				require.Len(t, backend.requests, 1, "remaining requests wait for Retry-After")
				notBefore := backend.requests[0].Add(retryAfterReplayWait)
				require.True(t, throttled.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", throttled.NotBefore, notBefore)

				rescheduled := readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateActive))
				require.Len(t, rescheduled, len(records))
				for _, record := range rescheduled {
					require.True(t, record.NotBefore.Equal(notBefore), "%s NotBefore = %s", record.RequestID, record.NotBefore)
					wantAttempts := 1
					if tt.requestsPerReplay > 1 && record.RequestID != records[0].RequestID {
						wantAttempts = 0
						// LastFailureReasonは調査用に保存するerrorの文言だけを持つため、保存内容を文言で確認します。
						require.Contains(t, record.LastFailureReason, spool.ErrDeferredByRetryAfter.Error())
					}
					require.Equal(t, wantAttempts, record.ResendAttemptCount, record.RequestID)
				}

				time.Sleep(retryAfterReplayWait - time.Second)
				deferred, err := spoolWorker.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, worker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
				require.Len(t, backend.requests, 1)

				// 再起動後のWorkerは期限をmemoryに持たないが、保存した期限を読み込んで送信を控える。
				restarted := newWorker()
				restored, err := restarted.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, worker.ProcessStatusSkipped, restored.Status)
				require.Equal(t, worker.ProcessSkipReasonRetryAfter, restored.SkipReason)
				require.True(t, restored.NotBefore.Equal(notBefore))
				require.Len(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateProcessing)), len(records))
				require.Len(t, backend.requests, 1, "no request before the server-specified time")
				stillDeferred, err := restarted.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, worker.ProcessSkipReasonRetryAfter, stillDeferred.SkipReason)

				time.Sleep(time.Until(notBefore))
				resumed, err := restarted.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, worker.ProcessStatusCompleted, resumed.Status)
				require.Equal(t, len(records), resumed.SuccessRecordCount)
				require.Len(t, backend.requests, 1+tt.requestsPerReplay)
				for _, requestedAt := range backend.requests[1:] {
					require.False(t, requestedAt.Before(notBefore), "request at %s before %s", requestedAt, notBefore)
				}
				require.Len(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateDone)), len(records))
				require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateActive)))
				require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateProcessing)))
			})
		})
	}
}

type longRetryAfterSender struct{}

func (longRetryAfterSender) Send(_ context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	for inputIndex := range records {
		report = append(report, spool.RecordOutcome{
			InputIndex:  inputIndex,
			Status:      spool.RecordOutcomeStatusRetryable,
			AttemptedAt: time.Now(),
			Err:         errors.New("throttled"),
			RetryAfter:  3 * time.Hour,
		})
	}
	return report, nil
}

// backendごとのspool runner設定の待機上限が、構築した再送workerに適用されることを確認します。
func TestNewBackendRunnerSupervisorAppliesConfiguredMaxRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := BackendRuntimeOptions{
			Backend: "loki",
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: t.TempDir(),
				Target:    "retry-after-cap",
				Dir:       "loki",
			},
			Sender: longRetryAfterSender{},
		}
		opts.RunnerConfig.Worker.Finalize.RetryDelay = time.Second
		opts.RunnerConfig.Worker.Finalize.MaxEntries = 100
		opts.RunnerConfig.Worker.MaxRetryAfter = 2 * time.Minute
		opts.RunnerConfig.LifeCycle.SizePurgeInterval = time.Second
		opts.RunnerConfig.Runner.Interval = time.Second
		opts.SupervisorConfig.RestartDelay = time.Second
		utc := clock.NewUTCClock()
		supervisor, err := NewBackendRunnerSupervisor(utc, opts, observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		store, err := fsstorestore.NewSpoolStore(utc, loggerdurable.NewSpoolConfigAdapter(opts.SpoolConfig), observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		require.NoError(t, store.StoreByState(context.Background(), spool.StateActive, []spool.SpoolRecord{
			{OccurredAt: utc.Now(), RequestID: "capped", Payload: []byte(`{"line":"capped"}`)},
		}))

		report, err := supervisor.Runner.worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		wantNotBefore := time.Now().Add(2 * time.Minute)
		require.True(t, report.NotBefore.Equal(wantNotBefore), "NotBefore = %s, want %s", report.NotBefore, wantNotBefore)
		require.NotNil(t, report.RetryAfterClamp)
		require.Equal(t, 2*time.Minute, report.RetryAfterClamp.MaxRetryAfter)
	})
}
