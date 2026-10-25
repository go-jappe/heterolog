package opensearch

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func retryAfterTestDocuments() []pkgopensearch.DocumentWriteRequest {
	return []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Doc: map[string]any{"message": "one"}, Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-2", Doc: map[string]any{"message": "two"}, Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-3", Doc: map[string]any{"message": "six"}, Op: pkgopensearch.WriteOpIndex},
	}
}

func retryAfterTestRecords(t *testing.T, documents []pkgopensearch.DocumentWriteRequest) []spool.SpoolRecord {
	t.Helper()

	records := make([]spool.SpoolRecord, 0, len(documents))
	for index, document := range documents {
		records = append(records, spoolRecordForDocument(t, fmt.Sprintf("request-%d", index+1), document))
	}
	return records
}

func TestSendDefersRemainingPlannedRequestsAfterRetryAfter(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		retryAfter time.Duration
		wantCalls  int
	}{
		{name: "retry-after", retryAfter: 30 * time.Second, wantCalls: 1},
		// Retry-Afterがなければ、従来どおり同じ周期で残りのrequestも送信する。
		{name: "without retry-after", wantCalls: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 6, 9, 15, 50, 0, 0, time.UTC)
			documents := retryAfterTestDocuments()
			oneDocumentSize, err := pkgopensearch.BulkRequestSize(documents[:1])
			require.NoError(t, err)
			// pkg/opensearchはRetry-Afterの秒数をAfterへ設定する。
			throttleErr := &pkgopensearch.HTTPStatusError{
				Code:   http.StatusTooManyRequests,
				Status: "429 Too Many Requests",
				After:  tt.retryAfter,
			}
			client := &fakeOpenSearchClient{}
			client.indexDocuments = func(
				_ context.Context,
				docs []pkgopensearch.DocumentWriteRequest,
			) (*pkgopensearch.BulkResponse, error) {
				if client.indexDocumentsCallCount == 1 {
					return nil, throttleErr
				}
				return successBulkResponseForDocuments(docs), nil
			}
			sender := newTestSender(now, client)
			sender.maxBulkRequestBytes = oneDocumentSize
			records := retryAfterTestRecords(t, documents)

			report, err := sender.Send(context.Background(), records)

			require.NoError(t, err)
			require.NoError(t, report.Validate(len(records)))
			throttled := assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, now)
			assert.ErrorIs(t, throttled.Err, throttleErr)
			assert.Equal(t, tt.retryAfter, throttled.RetryAfter)
			require.Equal(t, tt.wantCalls, client.indexDocumentsCallCount)
			if tt.retryAfter == 0 {
				assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
				assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
				return
			}
			for _, inputIndex := range []int{1, 2} {
				deferred := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, time.Time{})
				assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
				assert.ErrorIs(t, deferred.Err, throttleErr)
				assert.Zero(t, deferred.RetryAfter)
			}
		})
	}
}

func TestSendDefersSplitHalfAfterRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 51, 0, 0, time.UTC)
	throttleErr := &pkgopensearch.HTTPStatusError{
		Code:   http.StatusServiceUnavailable,
		Status: "503 Service Unavailable",
		After:  30 * time.Second,
	}
	client := &fakeOpenSearchClient{}
	client.indexDocuments = func(
		_ context.Context,
		docs []pkgopensearch.DocumentWriteRequest,
	) (*pkgopensearch.BulkResponse, error) {
		switch client.indexDocumentsCallCount {
		case 1:
			return nil, &pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		case 2:
			return nil, throttleErr
		default:
			return successBulkResponseForDocuments(docs), nil
		}
	}
	sender := newTestSender(now, client)
	documents := retryAfterTestDocuments()[:2]
	records := retryAfterTestRecords(t, documents)

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	throttled := assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, now)
	assert.Equal(t, 30*time.Second, throttled.RetryAfter)
	// 分けた後半は未送信のまま、初回requestの送信時刻を残して再送対象にする。
	deferred := assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
	assert.Equal(t, []int{2, 1}, documentCounts(client.indexDocumentsCalls))
}
