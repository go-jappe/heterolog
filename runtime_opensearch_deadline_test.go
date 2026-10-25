package heterolog_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestOpenSearchPreservesStoppedRequests(t *testing.T) {
	for _, mode := range []string{"bulk_timeout", "single_timeout", "bulk_item_retry_wait"} {
		for _, control := range []bool{true, false} {
			name := mode + "/logger_deadline"
			if control {
				name = mode + "/control"
			}
			t.Run(name, func(t *testing.T) {
				var mu sync.Mutex
				var bodies []string
				var paths []string
				replays := 0
				root := t.TempDir()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					// spool runnerの初回の周期はIntervalを待たずに始まり、Closeまでに保存済みの要求を
					// 再送することがあります。spoolに記録がある間に届いた要求はrunnerの再送なので、
					// 一時的な失敗としてすぐ返して記録をactiveに残し、loggerの送信には数えません。
					if spoolHoldsRecordForTest(t, root) {
						mu.Lock()
						replays++
						mu.Unlock()
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					mu.Lock()
					bodies = append(bodies, string(body))
					paths = append(paths, r.URL.Path)
					mu.Unlock()
					if mode != "bulk_item_retry_wait" {
						select {
						case <-r.Context().Done():
							return
						case <-time.After(time.Second):
						}
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					scanner := bufio.NewScanner(bytes.NewReader(body))
					var items []any
					for scanner.Scan() {
						var meta map[string]map[string]any
						if err := json.Unmarshal(scanner.Bytes(), &meta); err != nil {
							t.Error(err)
							return
						}
						if !scanner.Scan() {
							t.Error("missing document line")
							return
						}
						for action, info := range meta {
							items = append(items, map[string]any{action: map[string]any{"_index": info["_index"], "_id": info["_id"], "status": 503, "error": map[string]any{"type": "unavailable_shards_exception", "reason": "temporary unavailability"}}})
						}
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]any{"errors": true, "items": items})
				}))
				defer server.Close()
				cfg := runtimeMemoryPressureConfig(root, nil)
				base := cfg.Loki.LokiLoggerBaseConfig
				store := cfg.Loki.FailureStoreConfig
				store.DurableStoreConfig.SpoolConfig.Dir = "opensearch"
				store.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload = false
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				cfg.Timeout = 100 * time.Millisecond
				cfg.OpenSearchClient = &pkgopensearch.Config{Endpoint: server.URL, Timeout: 2 * time.Second, AuthConfig: pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone}}
				cfg.OpenSearch = &pkglogger.OpenSearchConfig{
					OpenSearchLoggerBaseConfig:   pkglogger.OpenSearchLoggerBaseConfig{RuntimeConfig: base.RuntimeConfig, LogBodyConfig: base.LogBodyConfig, FlushInterval: time.Hour, MaxBulkDocument: 2, BufferFlushBytes: 1 << 20, MaxBulkRequestBytes: 2 << 20, NoBulkFlushModeEnabled: mode == "single_timeout"},
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{Index: "logs", MaxRetryAttempts: 1, FailureStoreConfig: store, Resource: cfg.Loki.Resource},
				}
				switch {
				case mode == "bulk_item_retry_wait" && !control:
					// 1時間の再送待機はloggerの期限内に終わらないため、負荷に関係なく期限切れになります。
					cfg.OpenSearch.MaxRetryAttempts = 2
					cfg.OpenSearch.RetryWaitTime = time.Hour
				case control:
					// 対照群はloggerの期限で中断しないことを確かめます。HTTPの往復と、fileと
					// directoryを同期するspool保存を負荷下でも終えられる期限を与えます。
					cfg.Timeout = 5 * time.Second
					if mode != "bulk_item_retry_wait" {
						cfg.OpenSearchClient.Timeout = 50 * time.Millisecond
					}
				}
				cfg.Loki = nil
				cfg.LokiClient = nil
				spool := heterolog.DefaultSpoolConfig()
				spool.RunnerConfig.OpensearchConfig.Runner.Interval = time.Hour
				logger, err := heterolog.New(context.Background(), cfg, heterolog.WithSpool(spool))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				wantCount := 1
				if mode != "single_timeout" {
					wantCount = 2
					if err := logger.Info(context.Background(), "previously accepted log"); err != nil {
						t.Fatal(err)
					}
				}
				flushErr := logger.Info(context.Background(), "trigger log")
				closeErr := logger.Close()
				records := readRuntimeMemorySpoolRecords(t, root)
				mu.Lock()
				sent := append([]string(nil), bodies...)
				sentPaths := append([]string(nil), paths...)
				replayed := replays
				mu.Unlock()
				t.Logf("flush=%v close=%v sends=%d replays=%d saved=%d", flushErr, closeErr, len(sent), replayed, len(records))
				if closeErr != nil {
					t.Errorf("Close: %v", closeErr)
				}
				if control && flushErr != nil {
					t.Errorf("control flush=%v", flushErr)
				}
				if !control && !errors.Is(flushErr, context.DeadlineExceeded) {
					t.Errorf("flush error=%v", flushErr)
				}
				if len(sent) != 1 || !strings.Contains(sent[0], "trigger log") {
					t.Fatalf("wire=%v", sent)
				}
				if wantCount == 2 && !strings.Contains(sent[0], "previously accepted log") {
					t.Fatalf("wire missing prior accepted entry=%v", sent)
				}
				if len(records) != wantCount {
					t.Fatalf("accepted log loss: saved=%d want=%d", len(records), wantCount)
				}
				// Compare the saved create requests with what reached HTTP,
				// including the fixed IDs required for safe later replay.
				wireDocs := make(map[string]any)
				if mode == "single_timeout" {
					var doc any
					if err := json.Unmarshal([]byte(sent[0]), &doc); err != nil {
						t.Fatal(err)
					}
					id := sentPaths[0][strings.LastIndex(sentPaths[0], "/")+1:]
					wireDocs[id] = doc
				} else {
					scanner := bufio.NewScanner(strings.NewReader(sent[0]))
					for scanner.Scan() {
						var meta map[string]struct {
							ID string `json:"_id"`
						}
						if err := json.Unmarshal(scanner.Bytes(), &meta); err != nil {
							t.Fatal(err)
						}
						if !scanner.Scan() {
							t.Fatal("missing wire document")
						}
						var doc any
						if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
							t.Fatal(err)
						}
						wireDocs[meta["create"].ID] = doc
					}
					if err := scanner.Err(); err != nil {
						t.Fatal(err)
					}
				}
				for _, record := range records {
					var saved struct {
						ID    string `json:"id"`
						Index string `json:"index"`
						Op    string `json:"op"`
						Doc   any    `json:"doc"`
					}
					if err := json.Unmarshal(record.Payload, &saved); err != nil {
						t.Fatal(err)
					}
					original, ok := wireDocs[saved.ID]
					if !ok || saved.ID == "" || saved.Index != "logs" || saved.Op != "create" || !reflect.DeepEqual(original, saved.Doc) {
						t.Fatalf("saved request differs from HTTP request: %+v; wire=%v", saved, wireDocs)
					}
					delete(wireDocs, saved.ID)
				}
				if len(wireDocs) != 0 {
					t.Fatalf("requests missing from durable storage: %v", wireDocs)
				}
				wantState := pkgspool.StateActive
				if !control && mode != "bulk_item_retry_wait" {
					wantState = pkgspool.StateQuarantine
				}
				stateRoot := filepath.Join(root, "memory-test", "spool", "opensearch", string(wantState))
				if got := len(readRuntimeMemorySpoolRecords(t, stateRoot)); got != wantCount {
					t.Errorf("state %s records=%d want=%d", wantState, got, wantCount)
				}
			})
		}
	}
}
