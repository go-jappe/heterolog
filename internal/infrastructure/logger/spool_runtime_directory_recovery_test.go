package logger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

// 再送runnerは送信だけを試験用senderへ置き換え、保存先と保全通知のcallbackは
// production構築のものを使います。callbackの接続が欠けると警告が届かず失敗します。
func TestFailureSpoolStorePreservationUsesRegisteredWriterOnWarn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		spoolConfig := cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig
		store := newFailureSpoolNotificationTestStore(t, spoolConfig)
		active := store.RootDir.ActiveDir().Path()
		sender := &replaceActiveAfterSend{active: active}
		plan, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
			contractKind: pkglogger.LoggerContractSync, config: cfg,
		}})
		require.NoError(t, err)
		require.Len(t, plan, 1)
		plan[0].sender = sender
		pipelines, err := newFailureSpoolPipelineSet(context.Background(), plan, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = pipelines.close() })
		var warnings []*logging.LogEntry
		writer := &spoolstorehandler.SpoolStoreHandler{
			Store: store,
			OnWarn: func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				warnings = append(warnings, entry)
			},
		}
		require.NoError(t, pipelines.registerWriter(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, writer))
		storeFailureSpoolNotificationTestRecord(t, store, `{"message":"retained"}`)
		// 再送後のactiveへの再保存にも、登録済みwriterと同じ通知先が届きます。
		report, err := pipelines.runtime.runners[0].supervisor.Runner.RunOnce(context.Background())
		require.NoError(t, err)
		synctest.Wait()
		require.NoError(t, sender.replacementErr)
		require.Equal(t, 1, sender.calls)
		require.Equal(t, 1, report.ProcessReport.RetryRecordCount)
		require.Len(t, warnings, 1)
		require.Equal(t, "failure_spool_runner", warnings[0].Source)
		require.Equal(t, pkglogger.LoggerContractSync, warnings[0].Fields["logger_contract"])
		require.Equal(t, pkglogger.LogOutputOpenSearch, warnings[0].Fields["backend"])
		require.Equal(t, active, warnings[0].Fields["original_path"])
		require.Error(t, warnings[0].Err)
		preserved := warnings[0].Fields["retained_path"].(string)
		content, err := os.ReadFile(preserved)
		require.NoError(t, err)
		require.Equal(t, "obstruction", string(content))
		files, err := filepath.Glob(filepath.Join(active, "*", "*", "*", "*", "*", "*.hlogspool"))
		require.NoError(t, err)
		require.Len(t, files, 1)
	})
}

func TestFailureSpoolRecoveryPreservationWarningCanRequestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		store := newFailureSpoolNotificationTestStore(t, cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig)
		sender := &replaceActiveAfterSend{active: store.RootDir.ActiveDir().Path()}
		var runtime *FailureSpoolRuntime
		var warning *logging.LogEntry
		closed := false
		var err error
		runtime, err = NewFailureSpoolRecoveryRuntime(
			FailureSpoolRecoveryConfigs{Sync: cfg}, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{},
			WithFailureSpoolRecoveryOnWarn(func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				warning = entry
				runtime.RequestClose()
				closed = true
			}),
			withFailureSpoolRecoveryTestSender(sender),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = runtime.Close() })
		storeFailureSpoolNotificationTestRecord(t, store, `{"message":"retry me"}`)
		require.NoError(t, runtime.Start(context.Background()))
		synctest.Wait()
		require.True(t, closed, "OnWarn must not wait for the runner goroutine that produced it")
		require.NoError(t, sender.replacementErr)
		require.Equal(t, 1, sender.calls)
		require.NotNil(t, warning)
		require.Equal(t, sender.active, warning.Fields["original_path"])
		content, err := os.ReadFile(warning.Fields["retained_path"].(string))
		require.NoError(t, err)
		require.Equal(t, "obstruction", string(content))
		require.False(t, runtime.runners[0].supervisor.IsRunning())
	})
}

type replaceActiveAfterSend struct {
	active         string
	calls          int
	replacementErr error
}

func (s *replaceActiveAfterSend) Send(_ context.Context, records []pkgspool.SpoolRecord) (pkgspool.ExportReport, error) {
	s.calls++
	// claim後のactiveの空ディレクトリ群を残したまま、管理者操作による妨害を再現します。
	s.replacementErr = os.Rename(s.active, s.active+"-before-recovery")
	if s.replacementErr == nil {
		s.replacementErr = os.WriteFile(s.active, []byte("obstruction"), 0o600)
	}
	if s.replacementErr != nil {
		return nil, s.replacementErr
	}
	report := make(pkgspool.ExportReport, len(records))
	for index := range records {
		report[index] = pkgspool.RecordOutcome{
			InputIndex: index, Status: pkgspool.RecordOutcomeStatusRetryable,
			AttemptedAt: time.Now(), Err: errors.New("retryable backend failure"),
		}
	}
	return report, nil
}
