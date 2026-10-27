package loggerruntime_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

type retryWaitTransport func(*http.Request) (*http.Response, error)

func (f retryWaitTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRetryWaitKeepsKnownRefusalReplayable(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		t.Run(string(mode), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root := t.TempDir()
				cfg := deliveryEvidenceConfig("loki", "https://loki.example.test")
				cfg.Timeout = 50 * time.Millisecond
				cfg.Loki.LokiRetryableClientCaseTransportErrorConfig = &pkglogger.LokiRetryableClientCaseTransportErrorConfig{RetryClientMode: mode, MaxRetries: 2, BaseDelay: time.Hour, MaxDelay: time.Hour}
				var calls atomic.Int32
				cfg.LokiClient.HTTPClient = &http.Client{Transport: retryWaitTransport(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil || !strings.Contains(string(body), "known refusal") {
						t.Errorf("body=%q err=%v", body, err)
					}
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("temporarily unavailable"))}, nil
				})}
				cfg.Loki.FailureStoreConfig = durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeFileArchive}, FileArchiveConfig: durable.DeliveryFileArchiveConfig{
					Target: "retry-wait", MaxFileSize: 1024 * 1024, FailureFileArchivePath: root, Dir: "archive", DirCaseRetryable: "retryable", DirCasePermanent: "permanent", FileNameFormat: pkgfsstore.FailureArchiveFileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
					MaxFileBackups: 2, MaxFileAge: 1, MaxDirSize: 10 * 1024 * 1024, UsageCacheTTL: time.Hour, CompressFormat: pkgfsstore.CompressGzip, RotationType: pkgfsstore.FileRotationDaily, RotationDayOfWeek: time.Sunday, MaxWriteRetries: 0, FileOperationRetryWaitTime: time.Millisecond,
					OverloadPolicyConfig: durable.FileArchiveOverloadPolicyConfig{OverloadPolicy: durable.OverloadPolicyDrop, MinAvailableDiskSpace: durable.MinFileArchiveAvailableDiskSpace},
				}}
				logger, err := loggerruntime.NewDelivery(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				report, logErr := logger.Info(context.Background(), "known refusal")
				if closeErr := logger.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				if calls.Load() != 1 {
					t.Fatalf("requests=%d want1", calls.Load())
				}
				if logErr == nil || len(report.Targets) != 1 {
					t.Fatalf("report=%+v err=%v", report, logErr)
				}
				if mode == pkglogger.RetryClientModeRetryPolicy && report.Targets[0].Status != pkglogger.DeliveryFailed {
					t.Errorf("wait exceeds deadline: expected confirmed failure, got %+v", report.Targets)
				}
				// 全体の結果は、子loggerの報告が届く前に自分の期限に達することがあります。その
				// Unconfirmedの結果は正しく、それでもCloseはdurableな処理を終える必要があります。
				found := 0
				err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
					if walkErr != nil {
						return walkErr
					}
					if d.IsDir() {
						return nil
					}
					file, err := os.Open(path)
					if err != nil {
						return err
					}
					defer file.Close()
					scanner := bufio.NewScanner(file)
					for scanner.Scan() {
						var rec struct {
							FailureCategory string         `json:"failure_category"`
							StatusCode      int            `json:"status_code"`
							FailureCode     string         `json:"failure_code"`
							Payload         map[string]any `json:"payload"`
						}
						if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
							continue
						}
						if rec.Payload["message"] != "known refusal" {
							continue
						}
						found++
						if rec.FailureCategory != "retryable" || rec.StatusCode != 503 {
							t.Errorf("saved category=%q status=%d reason=%q; want retryable HTTP503 (path=%s)", rec.FailureCategory, rec.StatusCode, rec.FailureCode, path)
						}
					}
					return scanner.Err()
				})
				if err != nil {
					t.Fatal(err)
				}
				if found != 1 {
					t.Errorf("saved matching records=%d want1", found)
				}
			})
		})
	}
}
