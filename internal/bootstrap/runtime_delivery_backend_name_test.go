package bootstrap_test

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Delivery loggerのAPIは、送信を始める前に拒否した場合（Close開始後・entryの上限超過）に
// multi loggerが作る送達結果も、各宛先が自身の結果に記録するのと同じ論理出力名（file・stdout）を
// Backendに使います。fileはerror・debugのroleを分離しても、default・error・debugを一つの宛先
// fileとして報告します。7つの出力先それぞれの名前は、internal/infrastructure/loggerの
// TestDeliveryLoggerSynthesizedResultsNameEveryOutputByLogicalNameが確認します。
func TestDeliveryRejectionsUseLogicalBackendNames(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name             string
		maxLogEntryBytes int
		closeFirst       bool
		message          string
		wantErr          error
	}{
		{name: "closing", maxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, closeFirst: true, message: "after close", wantErr: pkglogger.ErrLoggerClosed},
		{name: "entry_too_large", maxLogEntryBytes: 64, message: strings.Repeat("x", 100), wantErr: pkglogger.ErrLogEntryTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fileCfg := directoryWhitespaceConfig(t.TempDir(), "delivery-backend-names", "logs").File
			// error・debugのroleを別fileへ分離し、物理roleが三つでも宛先fileは一つであることを確かめます。
			layout := fileCfg.FsstoreConfig.Layout
			layout.ErrorLogDir = "logs_error"
			layout.IsSeparateErrorLogsEnabled = true
			layout.IsSeparateDebugLogsEnabled = true
			layout.SeparateErrorLogsPrefix = "error_"
			layout.SeparateDebugLogsPrefix = "debug_"
			cfg := loggerruntime.DefaultDeliveryConfig()
			cfg.MaxLogEntryBytes = tc.maxLogEntryBytes
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile, pkglogger.LogOutputStdout}
			cfg.File = &pkglogger.DeliveryFileLoggerConfig{
				FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
					Layout:   layout,
					Rotation: pkgfsstore.FileRotationConfig{MaxFileSize: fileCfg.FsstoreConfig.Lifecycle.MaxFileSize},
					Duration: fileCfg.FsstoreConfig.Duration,
					Retry:    fileCfg.FsstoreConfig.Retry,
				},
				TimeConfig:    fileCfg.TimeConfig,
				LogBodyConfig: fileCfg.LogBodyConfig,
			}
			logger, err := bootstrap.NewDelivery(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "Delivery logger Close", logger.Close, logger.ForceClose, nil)
			if tc.closeFirst {
				if err := boundedwait.Close(t, "Delivery logger Close", logger.Close, logger.ForceClose); err != nil {
					t.Fatal(err)
				}
			}

			report, err := logger.Info(ctx, tc.message)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Info() error = %v, want %v", err, tc.wantErr)
			}
			var backends []string
			for _, target := range report.Targets {
				if target.Status != pkglogger.DeliveryRejected {
					t.Errorf("target %+v, want rejected before sending", target)
				}
				backends = append(backends, target.Backend)
			}
			slices.Sort(backends)
			if want := []string{pkglogger.LogOutputFile.String(), pkglogger.LogOutputStdout.String()}; !slices.Equal(backends, want) {
				t.Fatalf("report backends = %q, want the logical output names %q", backends, want)
			}
		})
	}
}

// blockingDeliveryTransportは、releaseが閉じるまでrequestへ応答しません。requestのcontextも
// 見ないため、宛先の結果より先にDelivery APIの期限が切れます。待機は停止の検出のため10秒で
// 打ち切ります。
type blockingDeliveryTransport struct{ release chan struct{} }

func (tr blockingDeliveryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	select {
	case <-tr.release:
	case <-time.After(10 * time.Second):
	}
	return nil, errors.New("blocking test transport released")
}

// 宛先が期限内に結果を返さない場合にmulti loggerが作る送達結果も、論理出力名（loki）で
// 宛先を示します。
func TestDeliveryTimeoutUsesLogicalBackendName(t *testing.T) {
	release := make(chan struct{})
	cfg := deliveryEvidenceConfig("loki", "http://loki.invalid")
	cfg.Timeout = 50 * time.Millisecond
	cfg.LokiClient.HTTPClient = &http.Client{Transport: blockingDeliveryTransport{release: release}}
	logger, err := bootstrap.NewDelivery(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanupは後に登録したものから実行されるため、宛先の解放をCloseより後に登録します。
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "Delivery logger Close", logger.Close, logger.ForceClose, nil)
	t.Cleanup(sync.OnceFunc(func() { close(release) }))

	report, err := logger.Info(context.Background(), "delivery evidence")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Info() error = %v, want the Delivery deadline", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("report = %+v", report)
	}
	if target := report.Targets[0]; target.Backend != pkglogger.LogOutputLoki.String() || target.Status != pkglogger.DeliveryUnconfirmed {
		t.Fatalf("target = %+v, want loki/%s", target, pkglogger.DeliveryUnconfirmed)
	}
}
