package clickhouse

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	wrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	backend "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestRetryWaitCancellationPreservesFailureRecords(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		for _, known := range []bool{true, false} {
			name := string(mode) + "/unknown EOF"
			if known {
				name = string(mode) + "/known 503"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var lastErr error = io.EOF
					if known {
						lastErr = &backend.HTTPStatusError{Code: 503}
					}
					calls := 0
					attempt := func() error { go func() { time.Sleep(10 * time.Millisecond); cancel() }(); return lastErr }
					raw := &cancellationClickHouseClient{insert: func(_ context.Context, records []backend.LogRecord, _ backend.InsertOptions) (*backend.InsertResponse, error) {
						calls++
						require.Len(t, records, 1)
						require.Equal(t, "keep record", records[0].Message)
						return nil, attempt()
					}}
					client, err := wrapper.BuildClientChain(raw, NewRetryConfigProvider(&pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{RetryClientMode: mode, MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond}), nil, nil)
					require.NoError(t, err)
					saved := &cancellationClickHouseHandler{}
					composite, err := flushfailure.NewCompositeFailureHandler([]flushfailure.FailureHandler{saved}, 1)
					require.NoError(t, err)
					fm := NewClickHouseFlushManager(newTestClickHouseFlushManagerBase(t), client, newTestClickHouseFlushManagerConfig(), composite, newTestClickHouseResourceProvider(), observer.NopTracer{}, observer.NopMeter{})
					err = fm.Flush(ctx, []*logging.LogEntry{newTestLogEntry(pkglogger.LogLevelInfo, "keep record", nil)})
					require.ErrorIs(t, err, context.Canceled)
					require.NoError(t, fm.Close(context.Background()))
					require.Len(t, saved.results, 1)
					result := saved.results[0]
					require.Equal(t, 1, calls, "must not dispatch another attempt after cancellation")
					records := result.QuarantineRecords
					if known {
						records = result.RetryableRecords
						require.Empty(t, result.QuarantineRecords)
					} else {
						require.Empty(t, result.RetryableRecords)
					}
					require.Empty(t, result.DeadRecords)
					require.Len(t, records, 1)
					for _, record := range records {
						// 最後の送信errorと呼び出し元の停止をrecordの原因に保持し、
						// 保存するFailureReasonもその原因を表します。
						require.ErrorIs(t, record.Cause, lastErr)
						require.ErrorIs(t, record.Cause, context.Canceled)
						require.Equal(t, record.Cause.Error(), record.FailureReason)
					}
					// ClickHouse has no public Delivery backend. Its flush result
					// must retain causes for durable handling and diagnostics.
					require.ErrorIs(t, result.TerminalError, lastErr)
					require.ErrorIs(t, result.TerminalError, context.Canceled)
				})
			})
		}
	}
}
