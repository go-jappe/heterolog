package cloudwatchlogs

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	wrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	backend "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
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
					raw := &recordingCloudWatchLogsClient{putFunc: func(_ string, events []backend.Event) (*backend.PutLogEventsOutput, error) {
						calls++
						require.Len(t, events, 1)
						return nil, attempt()
					}}
					client, err := wrapper.BuildClientChain(raw, NewRetryConfigProvider(&pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{RetryClientMode: mode, MaxRetries: 2, BaseDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond}), nil, nil)
					require.NoError(t, err)
					saved := &recordingCloudWatchLogsFailureHandler{}
					composite := mustNewCloudWatchLogsFailureComposite(t, []flushfailure.FailureHandler{saved})
					fm := newTestCloudWatchLogsFlushManager(client, composite)
					err = fm.Put(ctx, newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "keep record"))
					require.ErrorIs(t, err, context.Canceled)
					require.NoError(t, fm.Close(context.Background()))
					result, ok := saved.result()
					require.True(t, ok)
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
					require.Len(t, records, 1)
					for _, record := range records {
						require.ErrorIs(t, record.Cause, lastErr)
						require.ErrorIs(t, record.Cause, context.Canceled)
					}
					target := result.TargetDeliveryResult("cloudwatchlogs")
					require.Equal(t, wantStatus, target.Status)
					require.ErrorIs(t, target.Err, lastErr)
					require.ErrorIs(t, target.Err, context.Canceled)
				})
			})
		}
	}
}
