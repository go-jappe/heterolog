package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	retryablepolicy "github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestClickHouseFlushCancellationPreservesEachRequestOutcome(t *testing.T) {
	for _, outcome := range []string{"unknown", "acknowledged", "known rejection", "before first request"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &cancellationClickHouseClient{}
			client.insert = func(context.Context, []pkgclickhouse.LogRecord, pkgclickhouse.InsertOptions) (*pkgclickhouse.InsertResponse, error) {
				if len(client.tokens) != 2 {
					return &pkgclickhouse.InsertResponse{StatusCode: 200}, nil
				}
				cancel()
				switch outcome {
				case "acknowledged":
					return &pkgclickhouse.InsertResponse{StatusCode: 200}, nil
				case "known rejection":
					return nil, &pkgclickhouse.HTTPStatusError{Code: http.StatusUnauthorized}
				default:
					return nil, ctx.Err()
				}
			}
			saved := &cancellationClickHouseHandler{}
			composite, err := flushfailure.NewCompositeFailureHandler([]flushfailure.FailureHandler{saved}, 1)
			require.NoError(t, err)
			base := newTestClickHouseFlushManagerBase(t)
			fm := NewClickHouseFlushManager(base, client, newTestClickHouseFlushManagerConfig(), composite,
				newTestClickHouseResourceProvider(), observer.NopTracer{}, observer.NopMeter{})
			entries := []*logging.LogEntry{
				newTestLogEntry(pkglogger.LogLevelInfo, "message one", nil),
				newTestLogEntry(pkglogger.LogLevelInfo, "message two", nil),
				newTestLogEntry(pkglogger.LogLevelInfo, "message end", nil),
			}
			limit := 0
			for _, entry := range entries {
				entry.OccurredAt = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
				size, err := measureClickHouseLogEntryJSONEachRowBytes(base, fm.resource, entry)
				require.NoError(t, err)
				limit = max(limit, size)
			}
			base.config.(*clickHouseLoggerConfigAdapter).cfg.MaxInsertRequestBytes = limit
			if outcome == "before first request" {
				cancel()
			}
			require.ErrorIs(t, fm.Flush(ctx, entries), context.Canceled)
			require.NoError(t, fm.Close(context.Background()))
			require.NoError(t, saved.contextErr)
			require.True(t, saved.hasDeadline)
			var retryable, quarantine, dead []loggerfailure.FailedRecord
			for _, result := range saved.results {
				retryable = append(retryable, result.RetryableRecords...)
				quarantine = append(quarantine, result.QuarantineRecords...)
				dead = append(dead, result.DeadRecords...)
			}
			if outcome == "before first request" {
				require.Empty(t, client.tokens)
				require.Len(t, retryable, 3)
				require.Empty(t, quarantine)
				require.Empty(t, dead)
				for _, record := range retryable {
					requireClickHouseRequestNotSentRecord(t, record)
				}
				return
			}
			require.Len(t, client.tokens, 2, "remaining request must not be sent with an ended context")
			require.Len(t, retryable, 1)
			require.Equal(t, "message end", retryable[0].Payload["message"])
			requireClickHouseRequestNotSentRecord(t, retryable[0])
			require.Equal(t, infraclickhouse.DurableReplayModeBatch, retryable[0].Payload[infraclickhouse.DurablePayloadFieldReplayMode])
			unsentToken := retryable[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken]
			require.NotEmpty(t, unsentToken)
			require.NotContains(t, client.tokens, unsentToken)
			switch outcome {
			case "unknown":
				require.Len(t, quarantine, 1)
				require.Equal(t, "message two", quarantine[0].Payload["message"])
				require.Zero(t, quarantine[0].StatusCode)
				require.Equal(t, loggerfailure.FailureCode(retryablepolicy.RetryReasonUnknownOutcome), quarantine[0].FailureCode)
				require.Equal(t, client.tokens[1], quarantine[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken])
				require.Empty(t, dead)
			case "acknowledged":
				require.Empty(t, quarantine)
				require.Empty(t, dead)
			case "known rejection":
				require.Empty(t, quarantine)
				require.Len(t, dead, 1)
				require.Equal(t, "message two", dead[0].Payload["message"])
				require.Equal(t, http.StatusUnauthorized, dead[0].StatusCode)
			}
		})
	}
}

// requireClickHouseRequestNotSentRecordは、呼び出し元の停止によりclientへ渡さなかったrowが、
// 送信前の失敗としてbatch単位の再送対象に残っていることを検証します。
func requireClickHouseRequestNotSentRecord(t *testing.T, record loggerfailure.FailedRecord) {
	t.Helper()
	require.Zero(t, record.StatusCode)
	require.Equal(t, loggerfailure.FailureCode(retryablepolicy.RetryReasonRequestNotSent), record.FailureCode)
	require.Equal(t, infraclickhouse.DurableReplayModeBatch, record.Payload[infraclickhouse.DurablePayloadFieldReplayMode])
}

type cancellationClickHouseClient struct {
	recordingClickHouseClient
	insert func(context.Context, []pkgclickhouse.LogRecord, pkgclickhouse.InsertOptions) (*pkgclickhouse.InsertResponse, error)
	tokens []string
}

func (c *cancellationClickHouseClient) InsertLogsWithOptions(ctx context.Context, records []pkgclickhouse.LogRecord, opts pkgclickhouse.InsertOptions) (*pkgclickhouse.InsertResponse, error) {
	c.tokens = append(c.tokens, opts.InsertDeduplicationToken)
	return c.insert(ctx, records, opts)
}

type cancellationClickHouseHandler struct {
	results     []loggerfailure.SendResult
	contextErr  error
	hasDeadline bool
}

func (h *cancellationClickHouseHandler) HandleFailure(ctx context.Context, result loggerfailure.SendResult) error {
	h.contextErr = errors.Join(h.contextErr, ctx.Err())
	_, h.hasDeadline = ctx.Deadline()
	h.results = append(h.results, result)
	return ctx.Err()
}
