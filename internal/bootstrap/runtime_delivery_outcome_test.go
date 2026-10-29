package bootstrap_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

// 送信結果の報告を、実際のHTTPでの受け付けの証拠と照らし合わせます。特に、
// 失われた応答や不正な応答は、復旧用の記録をどこに保存したかにかかわらず、backendが
// logを拒否した証拠になりません。
func TestDeliveryPreservesAcceptanceEvidence(t *testing.T) {
	for _, backend := range []string{"loki", "cloudwatchlogs", "opensearch"} {
		for _, outcome := range []string{"success", "permanent refusal", "temporary refusal", "unknown", "local rejection"} {
			if backend == "opensearch" && outcome == "local rejection" {
				continue // 即時送信のOpenSearch APIには、手元での大きさの上限がありません。
			}
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil || !strings.Contains(string(body), "delivery evidence") {
						t.Errorf("request body=%q error=%v", body, err)
					}
					w.Header().Set("Content-Type", "application/json")
					switch outcome {
					case "permanent refusal", "temporary refusal":
						code := http.StatusBadRequest
						if outcome == "temporary refusal" {
							code = http.StatusServiceUnavailable
						}
						w.WriteHeader(code)
						_, _ = io.WriteString(w, `{"error":{"type":"test_rejection","reason":"explicit refusal"},"__type":"InvalidParameterException","message":"explicit refusal"}`)
					case "unknown":
						if backend == "loki" {
							// serverは要求を最後まで読んでから、応答を失います。
							conn, _, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Error(err)
								return
							}
							_ = conn.Close()
							return
						}
						_, _ = io.WriteString(w, `{"incomplete":`)
					default:
						switch backend {
						case "loki":
							w.WriteHeader(http.StatusNoContent)
						case "cloudwatchlogs":
							_, _ = io.WriteString(w, `{}`)
						case "opensearch":
							w.WriteHeader(http.StatusCreated)
							_ = json.NewEncoder(w).Encode(map[string]any{"_index": "logs", "_id": path.Base(r.URL.Path), "result": "created"})
						}
					}
				}))
				defer server.Close()

				cfg := deliveryEvidenceConfig(backend, server.URL)
				message := "delivery evidence"
				if outcome == "local rejection" {
					if backend == "loki" {
						cfg.Loki.MaxLineBytes = 256
						message += strings.Repeat("x", 1024)
					} else {
						cfg.MaxLogEntryBytes = 3 << 20
						message += strings.Repeat("x", cw.MaxPutLogEventBytes)
					}
				}
				logger, err := bootstrap.NewDelivery(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := logger.Close(); err != nil {
						t.Error(err)
					}
				})
				report, err := logger.Info(context.Background(), message)
				if len(report.Targets) != 1 {
					t.Fatalf("report=%+v error=%v", report, err)
				}
				want := pkglogger.DeliveryFailed
				switch outcome {
				case "success":
					want = pkglogger.DeliverySucceeded
				case "unknown":
					want = pkglogger.DeliveryUnconfirmed
				case "local rejection":
					want = pkglogger.DeliveryRejected
				}
				if got := report.Targets[0]; got.Status != want || got.Backend != backend {
					t.Errorf("target=%+v, want backend=%s status=%s", got, backend, want)
				}
				wantCalls := int32(1)
				if outcome == "local rejection" {
					wantCalls = 0
				}
				if calls.Load() != wantCalls {
					t.Errorf("HTTP calls=%d, want %d", calls.Load(), wantCalls)
				}
				if (err == nil) != (want == pkglogger.DeliverySucceeded) {
					t.Errorf("error=%v for status %s", err, want)
				}
				if report.HasTargetFailure() != want.IsFailed() || len(report.FailedTargets()) != boolCount(want.IsFailed()) {
					t.Errorf("failure helpers disagree with status %s: %+v", want, report)
				}
				if report.HasUnregisteredTarget() != (want != pkglogger.DeliverySucceeded) {
					t.Errorf("registration helpers disagree with status %s: %+v", want, report)
				}
			})
		}
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func deliveryEvidenceConfig(backend, endpoint string) pkglogger.DeliveryLoggerConfig {
	cfg := bootstrap.DefaultDeliveryConfig()
	cfg.Stdout = nil
	fs := durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}
	presentation := pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}
	switch backend {
	case "loki":
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
		cfg.LokiClient = &loki.Config{Endpoint: endpoint, Timeout: time.Second}
		cfg.Loki = &pkglogger.DeliveryLokiConfig{
			DeliveryLokiLoggerBaseConfig:   pkglogger.DeliveryLokiLoggerBaseConfig{LogBodyConfig: presentation, MaxLineBytes: 1 << 16, MaxPushRequestBytes: 1 << 20},
			DeliveryLokiFlushManagerConfig: pkglogger.DeliveryLokiFlushManagerConfig{FailureStoreConfig: fs, Resource: pkglogger.Resource{ServiceName: "delivery-test"}},
		}
	case "cloudwatchlogs":
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
		cfg.CloudWatchLogsClient = &cw.Config{Region: "ap-northeast-1", Endpoint: endpoint, LogGroupName: "logs", AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second}
		cfg.CloudWatchLogs = &pkglogger.DeliveryCloudWatchLogsConfig{
			DeliveryCloudWatchLogsLoggerBaseConfig:   pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{LogBodyConfig: presentation},
			DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{FailureStoreConfig: fs, LogGroupName: "logs", LogStreamName: "events"},
		}
	case "opensearch":
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
		cfg.OpenSearchClient = &opensearch.Config{Endpoint: endpoint, Timeout: time.Second, AuthConfig: opensearch.AuthConfig{Type: opensearch.AuthTypeNone}}
		cfg.OpenSearch = &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig:   pkglogger.DeliveryOpenSearchLoggerBaseConfig{LogBodyConfig: presentation},
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{FailureStoreConfig: fs, Index: "logs"},
		}
	}
	return cfg
}
