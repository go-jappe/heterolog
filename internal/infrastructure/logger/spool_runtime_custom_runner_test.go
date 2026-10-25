package logger

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

// withFailureSpoolRecoveryTestSenderは復旧専用runtimeのbackend送信だけを置き換えます。
// 保存先、保全通知のcallback、worker、runner、supervisorはproductionと同じ経路で構築します。
func withFailureSpoolRecoveryTestSender(sender worker.Sender) FailureSpoolRecoveryOption {
	return func(options *failureSpoolRecoveryOptions) { options.sender = sender }
}

// newFailureSpoolNotificationTestStoreは、runtimeが再送するrootへ試験のrecordを置くため、
// writerと同じ設定のSpoolStoreを作ります。
func newFailureSpoolNotificationTestStore(t testing.TB, cfg pkgdurable.SpoolConfig) *fsstorestore.SpoolStore {
	t.Helper()
	store, err := fsstorestore.NewSpoolStore(
		clock.NewUTCClock(),
		loggerdurable.NewSpoolConfigAdapter(cfg),
		observer.NopTracer{}, observer.NopMeter{},
	)
	require.NoError(t, err)
	return store
}

// storeFailureSpoolNotificationTestRecordはrunnerが最初の周期で送信するactive recordを保存します。
func storeFailureSpoolNotificationTestRecord(t testing.TB, store *fsstorestore.SpoolStore, payload string) {
	t.Helper()
	require.NoError(t, store.StoreByState(context.Background(), pkgspool.StateActive, []pkgspool.SpoolRecord{{
		OccurredAt: time.Now(), Payload: []byte(payload),
	}}))
}

// failingFailureSpoolNotificationSenderは全recordを再送可能として分類したうえで送信errorを
// 返します。workerはrecordをactiveへ戻し、そのerrorをrunnerの実行結果として報告します。
type failingFailureSpoolNotificationSender struct {
	err error
}

func (s failingFailureSpoolNotificationSender) Send(_ context.Context, records []pkgspool.SpoolRecord) (pkgspool.ExportReport, error) {
	report := make(pkgspool.ExportReport, len(records))
	for index := range records {
		report[index] = pkgspool.RecordOutcome{
			InputIndex: index, Status: pkgspool.RecordOutcomeStatusRetryable,
			AttemptedAt: time.Now(), Err: s.err,
		}
	}
	return report, s.err
}
