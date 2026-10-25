package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseFlushJoinsBackendCauseWhenDurableHandlingFails(t *testing.T) {
	backendErr := &pkgclickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
	durableErr := errors.New("durable store unavailable")
	for _, tc := range []struct {
		name       string
		handler    *flushfailure.CompositeFailureHandler
		wantHandle error
	}{
		{name: "no failure handler", wantHandle: flushfailure.ErrNoFailureHandler},
		{name: "failure handler error", handler: mustNewClickHouseCauseTestComposite(t, durableErr), wantHandle: durableErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flushManager := NewClickHouseFlushManager(
				newTestClickHouseFlushManagerBase(t),
				&recordingClickHouseClient{err: backendErr},
				newTestClickHouseFlushManagerConfig(),
				tc.handler,
				newTestClickHouseResourceProvider(),
				nil,
				nil,
			)

			err := flushManager.Flush(context.Background(), []*logging.LogEntry{
				newTestLogEntry(pkglogger.LogLevelInfo, "not stored", nil),
			})

			// durable処理を完了できなかった場合は、送信先の拒否理由も呼び出し元へ返します。
			require.ErrorIs(t, err, tc.wantHandle)
			require.ErrorIs(t, err, backendErr)
		})
	}
}

// 別のINSERT requestが受理された場合も、退避処理を完了できなかったrequestの元のerrorを
// 返してerrors.Asで判別できることを確認します（requestごとの結果に原因を保持します）。
func TestClickHouseFlushKeepsBackendCauseForPartiallyAcceptedFlush(t *testing.T) {
	rejection := &pkgclickhouse.HTTPStatusError{Code: http.StatusForbidden, Status: "403 Forbidden"}
	durableErr := errors.New("durable store unavailable")
	handler := &recordingClickHouseCauseTestHandler{err: durableErr}
	composite, err := flushfailure.NewCompositeFailureHandler([]flushfailure.FailureHandler{handler}, 1)
	require.NoError(t, err)
	client := &recordingClickHouseClient{errs: []error{nil, rejection}}
	base := newTestClickHouseFlushManagerBase(t)
	entries := []*logging.LogEntry{
		newTestLogEntry(pkglogger.LogLevelInfo, "accepted request", nil),
		newTestLogEntry(pkglogger.LogLevelInfo, "rejected request", nil),
	}
	// 発生時刻を固定し、計測したrowとflush時に作るrowを同じ大きさにします。
	occurredAt := time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)
	for _, entry := range entries {
		entry.OccurredAt = occurredAt
	}
	// 各rowは単独で上限内に収まり、二件をまとめると上限を超えるようにします。
	total := 0
	for _, entry := range entries {
		rowBytes, err := measureClickHouseLogEntryJSONEachRowBytes(base, newTestClickHouseResourceProvider(), entry)
		require.NoError(t, err)
		total += rowBytes
	}
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.MaxInsertRequestBytes = total - 1
	base.config = NewClickHouseLoggerConfigAdapter(cfg)
	flushManager := NewClickHouseFlushManager(
		base,
		client,
		newTestClickHouseFlushManagerConfig(),
		composite,
		newTestClickHouseResourceProvider(),
		nil,
		nil,
	)

	err = flushManager.Flush(context.Background(), entries)

	require.Equal(t, 2, client.batchCount(), "want one INSERT request per row")
	require.ErrorIs(t, err, durableErr)
	var statusErr *pkgclickhouse.HTTPStatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, http.StatusForbidden, statusErr.Code)
	results := handler.snapshot()
	require.Len(t, results, 1, "only the rejected request reaches durable handling")
	require.Equal(t, 1, results[0].FailedCount())
}

// recordingClickHouseCauseTestHandlerは受け取ったSendResultを記録し、固定のerrorを返します。
type recordingClickHouseCauseTestHandler struct {
	mu      sync.Mutex
	err     error
	results []loggerfailure.SendResult
}

func (h *recordingClickHouseCauseTestHandler) HandleFailure(_ context.Context, result loggerfailure.SendResult) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.results = append(h.results, result)
	return h.err
}

func (h *recordingClickHouseCauseTestHandler) snapshot() []loggerfailure.SendResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]loggerfailure.SendResult(nil), h.results...)
}

func TestClickHouseFlushDoesNotDuplicateBackendCauseAlreadyReturned(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &cancelingClickHouseCauseTestClient{cancel: cancel}
	flushManager := NewClickHouseFlushManager(
		newTestClickHouseFlushManagerBase(t),
		client,
		newTestClickHouseFlushManagerConfig(),
		nil,
		newTestClickHouseResourceProvider(),
		nil,
		nil,
	)

	err := flushManager.Flush(ctx, []*logging.LogEntry{
		newTestLogEntry(pkglogger.LogLevelInfo, "canceled in flight", nil),
	})

	require.ErrorIs(t, err, flushfailure.ErrNoFailureHandler)
	require.ErrorIs(t, err, context.Canceled)
	// 送信の戻り値が既に同じ原因を含む場合は、TerminalErrorを重ねて加えません。
	assert.Equal(t, 1, countWrappedErrors(err, context.Canceled), "error = %v", err)
}

// countWrappedErrorsは、errのwrap階層（errors.Joinを含む）にtargetと同一のerrorが
// いくつ含まれるかを返します。errorの文言ではなく構造で重複を判定します。
func countWrappedErrors(err, target error) int {
	if err == nil {
		return 0
	}
	if err == target {
		return 1
	}
	count := 0
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, inner := range wrapped.Unwrap() {
			count += countWrappedErrors(inner, target)
		}
	case interface{ Unwrap() error }:
		count += countWrappedErrors(wrapped.Unwrap(), target)
	}
	return count
}

func TestClickHouseFlushReturnsOnlyBackendResultWhenDurableHandlingCompletes(t *testing.T) {
	flushManager := NewClickHouseFlushManager(
		newTestClickHouseFlushManagerBase(t),
		&recordingClickHouseClient{err: &pkgclickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable}},
		newTestClickHouseFlushManagerConfig(),
		mustNewClickHouseCauseTestComposite(t, nil),
		newTestClickHouseResourceProvider(),
		nil,
		nil,
	)

	// 送信失敗をdurable処理で引き受けた場合は、従来どおりflush自体は失敗にしません。
	require.NoError(t, flushManager.Flush(context.Background(), []*logging.LogEntry{
		newTestLogEntry(pkglogger.LogLevelInfo, "stored for replay", nil),
	}))
}

func mustNewClickHouseCauseTestComposite(t *testing.T, err error) *flushfailure.CompositeFailureHandler {
	t.Helper()
	composite, compositeErr := flushfailure.NewCompositeFailureHandler(
		[]flushfailure.FailureHandler{clickHouseCauseTestHandler{err: err}},
		1,
	)
	require.NoError(t, compositeErr)
	return composite
}

// clickHouseCauseTestHandlerは、durable処理の結果として固定のerrorを返します。
type clickHouseCauseTestHandler struct {
	err error
}

func (h clickHouseCauseTestHandler) HandleFailure(context.Context, loggerfailure.SendResult) error {
	return h.err
}

// cancelingClickHouseCauseTestClientは、INSERT中に呼び出し元を停止させてcontext errorを返します。
type cancelingClickHouseCauseTestClient struct {
	cancel context.CancelFunc
}

func (c *cancelingClickHouseCauseTestClient) InsertLogsWithOptions(
	ctx context.Context,
	_ []pkgclickhouse.LogRecord,
	_ pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	c.cancel()
	return nil, ctx.Err()
}

func TestClickHouseFlushManagerQuarantinesSingleRecordRejections(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   loggerfailure.FailureCode
	}{
		{
			// 一件でも上限を超える場合は、上限設定の変更で配送できる可能性があるため隔離します。
			name:       "request too large",
			err:        &pkgclickhouse.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"},
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   loggerfailure.FailureCode(retryable.RetryReasonRequestTooLarge),
		},
		{
			name: "record value",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseNumber,
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   loggerfailure.FailureCode(retryable.RetryReasonRecordRejected),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flushManager := NewClickHouseFlushManager(
				newTestClickHouseFlushManagerBase(t),
				&recordingClickHouseClient{err: tc.err},
				newTestClickHouseFlushManagerConfig(),
				nil,
				newTestClickHouseResourceProvider(),
				nil,
				nil,
			)
			entries := []*logging.LogEntry{newTestLogEntry(pkglogger.LogLevelInfo, "single rejected", nil)}

			result, err := flushManager.insertEntryRecords(
				context.Background(),
				flushManager.convertLogEntriesToEntryRecords(entries),
				"single-record-token",
			)

			require.NoError(t, err)
			require.Empty(t, result.RetryableRecords)
			require.Empty(t, result.DeadRecords)
			require.Len(t, result.QuarantineRecords, 1)
			assert.Equal(t, tc.wantStatus, result.QuarantineRecords[0].StatusCode)
			assert.Equal(t, tc.wantCode, result.QuarantineRecords[0].FailureCode)
			assert.Equal(t, "single rejected", result.QuarantineRecords[0].Payload["message"])
		})
	}
}
