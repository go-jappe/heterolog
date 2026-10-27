package loki

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/stretchr/testify/require"
)

// cancellationは、物理requestの間にも、requestの送信中にも起こり得ます。
// 既に確認応答を受けたrequestを、決してrecoveryへ含めてはいけません。
func TestLokiFlushCancellationPreservesEachRequestOutcome(t *testing.T) {
	for _, outcome := range []string{"unknown", "earlier failure", "acknowledged", "known rejection", "rejected push", "before first request"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &cancellationLokiClient{}
			client.push = func(context.Context, []pkgloki.Stream) error {
				if client.calls == 1 && outcome == "earlier failure" {
					return &pkgloki.HTTPStatusError{Code: http.StatusServiceUnavailable}
				}
				if client.calls != 2 {
					return nil
				}
				cancel()
				switch outcome {
				case "acknowledged":
					return nil
				case "known rejection":
					return &pkgloki.HTTPStatusError{Code: http.StatusUnauthorized}
				case "rejected push":
					return &pkgloki.HTTPStatusError{Code: http.StatusBadRequest}
				default:
					return ctx.Err()
				}
			}
			saved := &cancellationLokiHandler{}
			fm := newTestLokiFlushManagerWithFailureHandler(client, mustNewLokiFailureComposite(t, []flushfailure.FailureHandler{saved}))
			entries := []*logging.LogEntry{
				newTestLokiLogEntry(pkglogger.LogLevelInfo, "message one"),
				newTestLokiLogEntry(pkglogger.LogLevelInfo, "message two"),
				newTestLokiLogEntry(pkglogger.LogLevelInfo, "message end"),
			}
			// 1件のrequestは上限に収まり、2件では収まりません。これにより、
			// mockのplanではなく実際のplannerを通して物理的な分割を起こします。
			limit := 0
			lineLimit := 0
			for _, rec := range fm.convertLogEntriesToEntryRecords(entries) {
				size, err := pkgloki.PushRequestSize([]pkgloki.Stream{{Labels: rec.labels, Entries: []pkgloki.Entry{rec.pushEntry}}})
				require.NoError(t, err)
				limit = max(limit, size)
				lineLimit = max(lineLimit, len(rec.pushEntry.Line))
			}
			fm.LoggerBase.config = newTestLokiLoggerConfigWithPushLimits(lineLimit, limit)
			if outcome == "before first request" {
				cancel()
			}
			require.ErrorIs(t, fm.Flush(ctx, entries), context.Canceled)
			require.NoError(t, fm.Close(context.Background()))
			require.Len(t, saved.results, 1)
			result := saved.results[0]
			require.NoError(t, saved.contextErr)
			require.True(t, saved.hasDeadline, "recovery must retain its own bounded lifetime")
			if outcome == "before first request" {
				require.Zero(t, client.calls)
				require.Len(t, result.RetryableRecords, 3)
				require.Empty(t, result.QuarantineRecords)
				require.Empty(t, result.DeadRecords)
				for _, record := range result.RetryableRecords {
					requireLokiRequestNotSentRecord(t, record)
				}
				return
			}
			require.Equal(t, 2, client.calls)
			if outcome == "earlier failure" {
				require.Len(t, result.RetryableRecords, 2)
				require.Equal(t, "message one", result.RetryableRecords[0].Payload["message"])
				require.Equal(t, "message end", result.RetryableRecords[1].Payload["message"])
				requireLokiRequestNotSentRecord(t, result.RetryableRecords[1])
				require.Len(t, result.QuarantineRecords, 1)
				require.Equal(t, "message two", result.QuarantineRecords[0].Payload["message"])
				require.Empty(t, result.DeadRecords)
				require.Zero(t, result.SuccessCount())
				return
			}
			require.Len(t, result.RetryableRecords, 1)
			require.Equal(t, "message end", result.RetryableRecords[0].Payload["message"])
			requireLokiRequestNotSentRecord(t, result.RetryableRecords[0])
			switch outcome {
			case "unknown":
				require.Len(t, result.QuarantineRecords, 1)
				require.Equal(t, "message two", result.QuarantineRecords[0].Payload["message"])
				require.Equal(t, pkglogger.DeliveryUnconfirmed, result.QuarantineRecords[0].DeliveryStatus)
				require.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonUnknownOutcome), result.QuarantineRecords[0].FailureCode)
				require.Empty(t, result.DeadRecords)
				require.Equal(t, 1, result.SuccessCount())
			case "acknowledged":
				require.Empty(t, result.QuarantineRecords)
				require.Empty(t, result.DeadRecords)
				require.Equal(t, 2, result.SuccessCount())
			case "known rejection":
				// 受け取った明示的な応答の分類は、呼び出し元の停止後も維持します。
				require.Empty(t, result.QuarantineRecords)
				require.Len(t, result.DeadRecords, 1)
				require.Equal(t, "message two", result.DeadRecords[0].Payload["message"])
				require.Equal(t, http.StatusUnauthorized, result.DeadRecords[0].StatusCode)
			case "rejected push":
				require.Empty(t, result.DeadRecords)
				require.Len(t, result.QuarantineRecords, 1)
				require.Equal(t, "message two", result.QuarantineRecords[0].Payload["message"])
				require.Equal(t, http.StatusBadRequest, result.QuarantineRecords[0].StatusCode)
				require.Equal(t, pkglogger.DeliveryFailed, result.QuarantineRecords[0].DeliveryStatus)
			}
		})
	}
}

// requireLokiRequestNotSentRecordは、呼び出し元の停止によりclientへ渡さなかったentryが、
// 送信前の拒否として再送対象に残っていることを検証します。
func requireLokiRequestNotSentRecord(t *testing.T, record loggerfailure.FailedRecord) {
	t.Helper()
	require.Zero(t, record.StatusCode)
	require.Equal(t, pkglogger.DeliveryRejected, record.DeliveryStatus)
	require.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonRequestNotSent), record.FailureCode)
	require.ErrorIs(t, record.Cause, context.Canceled)
}

func TestLokiImmediateCancellationStillCompletesDurableHandling(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		name := "normal"
		if delivery {
			name = "delivery report"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &cancellationLokiClient{push: func(context.Context, []pkgloki.Stream) error {
				cancel()
				return ctx.Err()
			}}
			saved := &cancellationLokiHandler{}
			composite := mustNewLokiReportingFailureComposite(t, []flushfailure.FailureHandlerWithReport{saved})
			fm := newTestLokiFlushManagerWithReportingFailureHandler(client, composite)
			entry := newTestLokiLogEntry(pkglogger.LogLevelInfo, "accepted immediate log")
			if delivery {
				report, err := fm.PushWithReport(ctx, entry)
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, report.Targets, 1)
				require.Equal(t, pkglogger.DeliveryUnconfirmed, report.Targets[0].Status)
				require.Len(t, report.Durables, 1)
				require.Equal(t, pkglogger.DurableSucceeded, report.Durables[0].Status)
			} else {
				require.ErrorIs(t, fm.Push(ctx, entry), context.Canceled)
			}
			require.Len(t, saved.results, 1)
			require.NoError(t, saved.contextErr)
			require.Len(t, saved.results[0].QuarantineRecords, 1)
			require.Equal(t, entry.Message, saved.results[0].QuarantineRecords[0].Payload["message"])
			require.Empty(t, saved.results[0].RetryableRecords)
			require.NoError(t, fm.Close(context.Background()))
		})
	}
}

type cancellationLokiClient struct {
	recordingFlushManagerLokiClient
	push  func(context.Context, []pkgloki.Stream) error
	calls int
}

func (c *cancellationLokiClient) Push(ctx context.Context, streams []pkgloki.Stream) error {
	c.calls++
	return c.push(ctx, streams)
}

type cancellationLokiHandler struct {
	results     []loggerfailure.SendResult
	contextErr  error
	hasDeadline bool
}

func (h *cancellationLokiHandler) HandleFailure(ctx context.Context, result loggerfailure.SendResult) error {
	h.contextErr = errors.Join(h.contextErr, ctx.Err())
	_, h.hasDeadline = ctx.Deadline()
	h.results = append(h.results, result)
	return ctx.Err()
}

func (h *cancellationLokiHandler) HandleFailureWithReport(ctx context.Context, result loggerfailure.SendResult) (pkglogger.DeliveryReport, error) {
	err := h.HandleFailure(ctx, result)
	return pkglogger.DeliveryReport{Durables: []pkglogger.DurableResult{{Kind: pkglogger.DurableArchive, Status: pkglogger.DurableSucceeded}}}, err
}
