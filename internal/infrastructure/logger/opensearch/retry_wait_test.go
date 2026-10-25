package opensearch

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	wrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	backend "github.com/go-jappe/heterolog/pkg/opensearch"
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
					raw := cancellationClient{bulk: func(_ context.Context, docs []backend.DocumentWriteRequest) (*backend.BulkResponse, error) {
						calls++
						require.Len(t, docs, 2)
						return nil, attempt()
					}}
					client, err := wrapper.BuildClientChain(raw, NewRetryConfigProvider(&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{RetryClientMode: mode, MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond}), nil, nil)
					require.NoError(t, err)
					saved := &cancellationFailureHandler{observations: make(chan cancellationObservation, 1)}
					fm := newReportingFlushManagerForDeliveryTest(saved)
					fm.osClient = client
					entry := newFlushManagerDeliveryTestEntry()
					entry.Message = "keep record"
					err = fm.Flush(ctx, []*logging.LogEntry{entry, entry})
					require.ErrorIs(t, err, context.Canceled)
					require.NoError(t, fm.Close(context.Background()))
					var observed cancellationObservation
					select {
					case observed = <-saved.observations:
					default:
						t.Fatal("failure handler not called")
					}
					require.NoError(t, observed.contextErr)
					result := observed.result
					require.Equal(t, 1, calls, "must not dispatch another attempt after cancellation")
					records := result.QuarantineRecords
					wantStatus := pkglogger.DeliveryUnconfirmed
					if known {
						records = result.RetryableRecords
						wantStatus = pkglogger.DeliveryFailed
						require.Empty(t, result.QuarantineRecords)
					} else {
						require.Empty(t, result.RetryableRecords)
					}
					require.Empty(t, result.DeadRecords)
					require.Len(t, records, 2)
					for _, record := range records {
						// 最後の送信errorと呼び出し元の停止をrecordの原因に保持し、
						// 保存するFailureReasonもその原因を表します。
						require.ErrorIs(t, record.Cause, lastErr)
						require.ErrorIs(t, record.Cause, context.Canceled)
						require.Equal(t, record.Cause.Error(), record.FailureReason)
					}
					target := result.TargetDeliveryResult("opensearch")
					require.Equal(t, wantStatus, target.Status)
					require.ErrorIs(t, target.Err, lastErr)
					require.ErrorIs(t, target.Err, context.Canceled)
				})
			})
		}
	}
}
