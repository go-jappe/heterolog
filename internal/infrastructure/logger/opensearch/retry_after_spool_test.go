package opensearch

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	ossender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

// retryAfterOpenSearchClientは、応答までの時間を経過させてから、pkg/opensearchが
// Retry-After付きの429から作るerrorを返します。throttleがfalseなら全documentを受理します。
type retryAfterOpenSearchClient struct {
	throttle bool
	calls    int
}

func (c *retryAfterOpenSearchClient) IndexDocument(
	_ context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	c.calls++
	if !c.throttle {
		return nil, errors.New("unexpected single document replay")
	}
	time.Sleep(time.Second)
	return nil, &pkgopensearch.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests", After: 30 * time.Second}
}

func (c *retryAfterOpenSearchClient) IndexDocuments(
	_ context.Context,
	docs []pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.BulkResponse, error) {
	c.calls++
	if c.throttle {
		time.Sleep(time.Second)
		return nil, &pkgopensearch.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests", After: 30 * time.Second}
	}
	items := make([]map[string]pkgopensearch.BulkItemResult, 0, len(docs))
	for _, doc := range docs {
		items = append(items, map[string]pkgopensearch.BulkItemResult{
			string(doc.Op): {Index: doc.Index, ID: doc.ID, Status: http.StatusCreated, Result: "created"},
		})
	}
	return &pkgopensearch.BulkResponse{Items: items}, nil
}

func readRetryAfterSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			records = append(records, record)
		}
		return scanner.Err()
	})
	require.NoError(t, err)
	return records
}

// 通常送信が429とRetry-Afterを受け取った場合は、spoolへ退避したrecordにも期限を保存し、
// 初回のspool再送もその時刻まで行わないことを確認します。一件のIndex APIとBulk APIの
// 両方の経路で、応答を受け取った時刻から期限を数えます。
func TestOpenSearchNormalSendRetryAfterDelaysFirstSpoolReplay(t *testing.T) {
	for _, tt := range []struct {
		name    string
		entries int
	}{
		{name: "index", entries: 1},
		{name: "bulk", entries: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				tracer := observer.NopTracer{}
				meter := observer.NopMeter{}
				utc := clock.NewUTCClock()
				spoolConfig := pkgdurable.SpoolConfig{
					SpoolPath:            t.TempDir(),
					Target:               "retry-after",
					Dir:                  "opensearch",
					OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{OverloadPolicy: pkgdurable.OverloadPolicyDrop},
				}
				failureHandler, err := BuildFailureHandler(ctx, utc, NewFlushManagerConfigAdapter(pkglogger.OpenSearchFlushManagerConfig{
					Index: "test-index",
					FailureStoreConfig: pkgdurable.FailureStoreConfig{
						MaxConcurrent:      1,
						DurableType:        []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
						DurableStoreConfig: pkgdurable.DurableStoreConfig{SpoolConfig: spoolConfig},
					},
					Resource:       pkglogger.Resource{ServiceName: "retry-after-test", Cloud: &pkglogger.CloudResourceInfo{}},
					MaxItemRetries: 0,
				}), nil, tracer, meter, nil, nil, nil)
				require.NoError(t, err)
				// 途中のassertionが失敗してもspool handlerを終了し、bubble内の処理を残しません。
				// Drainは完了後の再呼び出しで何もしません。
				t.Cleanup(func() { _ = failureHandler.Drain(context.Background()) })
				client := &retryAfterOpenSearchClient{throttle: true}
				flushManager := newTestFlushManager(client, NewMockOpenSearchFlushManagerConfig(), tracer, meter)
				flushManager.Clock = utc
				flushManager.FailureHandler = failureHandler

				sentAt := utc.Now()
				entries := make([]*logging.LogEntry, 0, tt.entries)
				for range tt.entries {
					entries = append(entries, newTestApplicationLogEntry(ctx, sentAt, pkglogger.LogLevelInfo, "wait for retry-after", nil, false))
				}
				require.NoError(t, flushManager.Flush(ctx, entries))
				require.Equal(t, 1, client.calls)
				notBefore := sentAt.Add(time.Second + 30*time.Second)
				stateRoot := func(state pkgspool.State) string {
					return filepath.Join(spoolConfig.SpoolPath, spoolConfig.Target, "spool", spoolConfig.Dir, string(state))
				}
				active := readRetryAfterSpoolRecords(t, stateRoot(pkgspool.StateActive))
				require.Len(t, active, tt.entries)
				for _, record := range active {
					require.True(t, record.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", record.NotBefore, notBefore)
				}

				storeConfig := loggerdurable.NewSpoolConfigAdapter(spoolConfig)
				spoolStore, err := fsstorestore.NewSpoolStore(utc, storeConfig, tracer, meter)
				require.NoError(t, err)
				stateStore, err := fsworker.NewStateStore(storeConfig)
				require.NoError(t, err)
				resendClient := &retryAfterOpenSearchClient{}
				spoolSender, err := ossender.NewOpensearchSpoolSender(utc, resendClient, 5*1024*1024)
				require.NoError(t, err)
				replayWorker, err := spoolworker.New(stateStore, openSearchRetryAfterActiveWriter{store: spoolStore}, spoolSender)
				require.NoError(t, err)

				deferred, err := replayWorker.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, spoolworker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
				require.Zero(t, resendClient.calls, "the first replay waits for Retry-After")

				time.Sleep(time.Until(notBefore))
				replayed, err := replayWorker.RunOnceSpoolFileProcess(ctx)
				require.NoError(t, err)
				require.Equal(t, spoolworker.ProcessStatusCompleted, replayed.Status)
				require.Equal(t, tt.entries, replayed.SuccessRecordCount)
				require.Equal(t, 1, resendClient.calls)
				require.NoError(t, flushManager.Close(ctx))
			})
		})
	}
}

type openSearchRetryAfterActiveWriter struct {
	store *fsstorestore.SpoolStore
}

func (w openSearchRetryAfterActiveWriter) StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}
