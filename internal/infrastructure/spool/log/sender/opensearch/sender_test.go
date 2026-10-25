package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

type fakeOpenSearchClient struct {
	indexDocumentCalled bool

	indexDocumentsCallCount int
	indexDocumentsDocs      []pkgopensearch.DocumentWriteRequest
	indexDocumentsCalls     [][]pkgopensearch.DocumentWriteRequest
	indexDocumentsResponse  *pkgopensearch.BulkResponse
	indexDocumentsErr       error
	onIndexDocuments        func(context.Context, []pkgopensearch.DocumentWriteRequest)
	indexDocuments          func(context.Context, []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error)
}

var _ pkgopensearch.Client = (*fakeOpenSearchClient)(nil)

func (c *fakeOpenSearchClient) IndexDocument(
	ctx context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	_ = ctx
	_ = doc

	c.indexDocumentCalled = true
	return nil, errors.New("unexpected IndexDocument call")
}

func (c *fakeOpenSearchClient) IndexDocuments(
	ctx context.Context,
	docs []pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.BulkResponse, error) {
	c.indexDocumentsCallCount++
	c.indexDocumentsDocs = append([]pkgopensearch.DocumentWriteRequest(nil), docs...)
	c.indexDocumentsCalls = append(
		c.indexDocumentsCalls,
		append([]pkgopensearch.DocumentWriteRequest(nil), docs...),
	)

	if c.onIndexDocuments != nil {
		c.onIndexDocuments(ctx, docs)
	}
	if c.indexDocuments != nil {
		return c.indexDocuments(ctx, docs)
	}

	return c.indexDocumentsResponse, c.indexDocumentsErr
}

func TestSendReturnsEmptyReportWithoutBackendCall(t *testing.T) {
	t.Parallel()

	client := &fakeOpenSearchClient{}
	sender := newTestSender(time.Time{}, client)

	actual, err := sender.Send(context.Background(), nil)

	require.NoError(t, err)
	assert.Equal(t, spool.ExportReport{}, actual)
	assert.Zero(t, client.indexDocumentsCallCount)
	assert.False(t, client.indexDocumentCalled)
}

func TestSendSkipsBackendWhenAllRecordsAreInvalid(t *testing.T) {
	t.Parallel()

	records := []spool.SpoolRecord{
		{RequestID: "empty-payload"},
		{
			RequestID: "missing-index",
			Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				ID:  "doc-1",
				Doc: map[string]any{"message": "missing index"},
				Op:  pkgopensearch.WriteOpIndex,
			}),
		},
	}
	client := &fakeOpenSearchClient{}
	sender := newTestSender(time.Time{}, client)

	actual, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, actual.Validate(len(records)))
	assert.Len(t, actual, len(records))
	assertOutcome(t, actual, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assertOutcome(t, actual, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.Equal(t, len(records), countOutcomeStatus(actual, spool.RecordOutcomeStatusInvalid))
	assert.Zero(t, countOutcomeStatus(actual, spool.RecordOutcomeStatusSuccess))
	assert.Zero(t, countOutcomeStatus(actual, spool.RecordOutcomeStatusRetryable))
	assert.Zero(t, countOutcomeStatus(actual, spool.RecordOutcomeStatusPermanent))
	assert.Zero(t, client.indexDocumentsCallCount)
}

func TestSendClassifiesBulkResultByInputIndexAndReportsAttemptedAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 12, 34, 56, 0, time.UTC)
	successRecord := validSpoolRecord(t, "request-success", "logs", "doc-success")
	retryableRecord := validSpoolRecord(t, "request-retryable", "logs", "doc-retryable")
	quarantineRecord := validSpoolRecord(t, "request-quarantine", "logs", "doc-quarantine")
	deadRecord := validSpoolRecord(t, "request-dead", "logs", "doc-dead")
	invalidRecord := spool.SpoolRecord{
		RequestID: "request-invalid",
		Payload:   []byte("{"),
	}
	client := &fakeOpenSearchClient{
		indexDocumentsResponse: &pkgopensearch.BulkResponse{
			Errors: true,
			Items: []map[string]pkgopensearch.BulkItemResult{
				bulkItem("index", "logs", "doc-success", 201, "", ""),
				bulkItem("index", "logs", "doc-retryable", 503, "unavailable", "node unavailable"),
				bulkItem("index", "logs", "doc-quarantine", 400, "mapper_parsing_exception", "mapping failed"),
				bulkItem("index", "logs", "doc-dead", 400, "illegal_argument_exception", "request configuration failed"),
			},
		},
	}
	sender := newTestSender(now, client)
	records := []spool.SpoolRecord{
		successRecord,
		retryableRecord,
		quarantineRecord,
		deadRecord,
		invalidRecord,
	}
	inputBeforeSend := cloneSpoolRecords(records)

	actual, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, actual.Validate(len(records)))
	require.Len(t, actual, len(records))
	assertOutcome(t, actual, 0, spool.RecordOutcomeStatusSuccess, now)
	retryableOutcome := assertOutcome(t, actual, 1, spool.RecordOutcomeStatusRetryable, now)
	quarantineOutcome := assertOutcome(t, actual, 2, spool.RecordOutcomeStatusInvalid, now)
	deadOutcome := assertOutcome(t, actual, 3, spool.RecordOutcomeStatusPermanent, now)
	invalidOutcome := assertOutcome(t, actual, 4, spool.RecordOutcomeStatusInvalid, time.Time{})

	var retryableErr *bulkDocumentFailureError
	require.ErrorAs(t, retryableOutcome.Err, &retryableErr)
	assert.Equal(t, "doc-retryable", retryableErr.documentID)
	assert.Equal(t, 503, retryableErr.status)
	assert.Equal(t, "unavailable", retryableErr.errorType)
	assert.Equal(t, "node unavailable", retryableErr.reason)

	var quarantineErr *bulkDocumentFailureError
	require.ErrorAs(t, quarantineOutcome.Err, &quarantineErr)
	assert.Equal(t, "doc-quarantine", quarantineErr.documentID)
	assert.Equal(t, 400, quarantineErr.status)
	assert.Equal(t, "mapper_parsing_exception", quarantineErr.errorType)
	assert.Equal(t, "mapping failed", quarantineErr.reason)

	var deadErr *bulkDocumentFailureError
	require.ErrorAs(t, deadOutcome.Err, &deadErr)
	assert.Equal(t, "doc-dead", deadErr.documentID)
	assert.Equal(t, 400, deadErr.status)
	assert.Equal(t, "illegal_argument_exception", deadErr.errorType)
	assert.Equal(t, "request configuration failed", deadErr.reason)
	assert.ErrorIs(t, invalidOutcome.Err, ErrDecodeSpoolPayload)
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusSuccess))
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusRetryable))
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusPermanent))
	assert.Equal(t, 2, countOutcomeStatus(actual, spool.RecordOutcomeStatusInvalid))

	require.Equal(t, 1, client.indexDocumentsCallCount)
	require.Len(t, client.indexDocumentsDocs, 4)
	assert.Equal(t, []string{"doc-success", "doc-retryable", "doc-quarantine", "doc-dead"}, documentIDs(client.indexDocumentsDocs))

	// 送信試行時刻はoutcomeのAttemptedAtで返し、Sendへ渡したrecordの再送メタデータは変更しません。
	for index, record := range records {
		assert.True(t, record.FirstResendAttemptedAt.IsZero(), "records[%d] FirstResendAttemptedAt", index)
		assert.True(t, record.LastResendAttemptedAt.IsZero(), "records[%d] LastResendAttemptedAt", index)
		assert.Zero(t, record.ResendAttemptCount, "records[%d] ResendAttemptCount", index)
	}
	assert.Equal(t, inputBeforeSend, records)
}

func TestSendReturnsRetryableRecordsOnTransportError(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 13, 0, 0, 0, time.UTC)
	firstAttemptedAt := now.Add(-24 * time.Hour)
	lastAttemptedAt := now.Add(-12 * time.Hour)
	record := validSpoolRecord(t, "request-retry", "logs", "doc-retry")
	record.FirstResendAttemptedAt = firstAttemptedAt
	record.LastResendAttemptedAt = lastAttemptedAt
	record.ResendAttemptCount = 2
	// HTTP clientが返す型付きの切断errorです。分類は文言ではなくerror番号で行います。
	transportErr := &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.ECONNRESET)}
	client := &fakeOpenSearchClient{
		indexDocumentsErr: transportErr,
	}
	sender := newTestSender(now, client)
	records := []spool.SpoolRecord{record}
	inputBeforeSend := cloneSpoolRecords(records)

	actual, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, actual.Validate(1))
	assert.Len(t, actual, 1)
	retryableOutcome := assertOutcome(t, actual, 0, spool.RecordOutcomeStatusRetryable, now)
	assert.ErrorIs(t, retryableOutcome.Err, transportErr)
	assert.ErrorIs(t, retryableOutcome.Err, syscall.ECONNRESET)
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusRetryable))
	// 今回の試行はoutcomeだけで返し、Sendへ渡したrecordの既存メタデータは保持します。
	assert.True(t, records[0].FirstResendAttemptedAt.Equal(firstAttemptedAt))
	assert.True(t, records[0].LastResendAttemptedAt.Equal(lastAttemptedAt))
	assert.Equal(t, 2, records[0].ResendAttemptCount)
	assert.Equal(t, inputBeforeSend, records)
	assert.Equal(t, 1, client.indexDocumentsCallCount)
}

func TestSendReturnsContextErrorBeforeBackendSend(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record := validSpoolRecord(t, "request-canceled", "logs", "doc-canceled")
	client := &fakeOpenSearchClient{
		indexDocumentsResponse: successBulkResponse("logs", "doc-canceled"),
	}
	sender := newTestSender(time.Date(2026, 6, 9, 14, 0, 0, 0, time.UTC), client)

	actual, err := sender.Send(ctx, []spool.SpoolRecord{record})

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, actual.Validate(1))
	retryableOutcome := assertOutcome(t, actual, 0, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, retryableOutcome.Err, context.Canceled)
	assert.Zero(t, client.indexDocumentsCallCount)
}

func TestSendReturnsContextErrorWhenBackendErrorHappensAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	record := validSpoolRecord(t, "request-canceled", "logs", "doc-canceled")
	client := &fakeOpenSearchClient{
		indexDocumentsErr: errors.New("transport failed"),
		onIndexDocuments: func(context.Context, []pkgopensearch.DocumentWriteRequest) {
			cancel()
		},
	}
	sender := newTestSender(time.Date(2026, 6, 9, 14, 30, 0, 0, time.UTC), client)

	actual, err := sender.Send(ctx, []spool.SpoolRecord{record})

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, actual.Validate(1))
	unknownOutcome := assertOutcome(
		t,
		actual,
		0,
		spool.RecordOutcomeStatusInvalid,
		time.Date(2026, 6, 9, 14, 30, 0, 0, time.UTC),
	)
	assert.ErrorIs(t, unknownOutcome.Err, context.Canceled)
	assert.Equal(t, 1, client.indexDocumentsCallCount)
}

func TestSendReturnsAttemptedRecordsToQuarantineWhenBulkResponseIsNil(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 14, 45, 0, 0, time.UTC)
	record := validSpoolRecord(t, "request-retryable", "logs", "doc-retryable")
	client := &fakeOpenSearchClient{}
	sender := newTestSender(now, client)

	actual, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.ErrorIs(t, err, ErrUnclassifiableBulkResponse)
	require.ErrorIs(t, err, infraopensearch.ErrNilBulkResponse)
	require.NoError(t, actual.Validate(1))
	retryableOutcome := assertOutcome(t, actual, 0, spool.RecordOutcomeStatusInvalid, now)
	assert.ErrorIs(t, retryableOutcome.Err, infraopensearch.ErrNilBulkResponse)
	assert.Equal(t, 1, client.indexDocumentsCallCount)
}

func TestSendReturnsAttemptedRecordsToQuarantineWhenBulkResponseIsIncomplete(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 14, 50, 0, 0, time.UTC)
	tests := []struct {
		name     string
		records  []spool.SpoolRecord
		response *pkgopensearch.BulkResponse
	}{
		{
			name: "errors_false_without_items",
			records: []spool.SpoolRecord{
				validSpoolRecord(t, "request-1", "logs", "doc-1"),
			},
			response: &pkgopensearch.BulkResponse{Errors: false},
		},
		{
			name: "errors_true_with_missing_item",
			records: []spool.SpoolRecord{
				validSpoolRecord(t, "request-1", "logs", "doc-1"),
				validSpoolRecord(t, "request-2", "logs", "doc-2"),
			},
			response: &pkgopensearch.BulkResponse{
				Errors: true,
				Items: []map[string]pkgopensearch.BulkItemResult{
					bulkItem("index", "logs", "doc-1", 201, "", ""),
				},
			},
		},
		{
			name: "item_without_status",
			records: []spool.SpoolRecord{
				validSpoolRecord(t, "request-1", "logs", "doc-1"),
			},
			response: &pkgopensearch.BulkResponse{
				Errors: false,
				Items: []map[string]pkgopensearch.BulkItemResult{
					bulkItem("index", "logs", "doc-1", 0, "", ""),
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeOpenSearchClient{indexDocumentsResponse: tt.response}
			sender := newTestSender(now, client)

			report, err := sender.Send(context.Background(), tt.records)

			require.ErrorIs(t, err, ErrUnclassifiableBulkResponse)
			require.ErrorIs(t, err, infraopensearch.ErrInvalidBulkResponse)
			require.NoError(t, report.Validate(len(tt.records)))
			assert.Zero(t, countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
			assert.Zero(t, countOutcomeStatus(report, spool.RecordOutcomeStatusPermanent))
			assert.Zero(t, countOutcomeStatus(report, spool.RecordOutcomeStatusRetryable))
			assert.Equal(t, len(tt.records), countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
			for inputIndex := range tt.records {
				assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusInvalid, now)
			}
			assert.Equal(t, 1, client.indexDocumentsCallCount)
		})
	}
}

func TestSendClassifiesDuplicateDocumentIDsByInputOrder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 0, 0, 0, time.UTC)
	client := &fakeOpenSearchClient{
		indexDocumentsResponse: &pkgopensearch.BulkResponse{
			Errors: true,
			Items: []map[string]pkgopensearch.BulkItemResult{
				bulkItem("index", "logs", "same-doc", 503, "unavailable", "node unavailable"),
				bulkItem("index", "logs", "same-doc", 400, "mapper_parsing_exception", "mapping failed"),
				bulkItem("index", "logs", "unique-doc", 201, "", ""),
			},
		},
	}
	sender := newTestSender(now, client)
	records := []spool.SpoolRecord{
		validSpoolRecord(t, "request-1", "logs", "same-doc"),
		validSpoolRecord(t, "request-2", "logs", "same-doc"),
		validSpoolRecord(t, "request-3", "logs", "unique-doc"),
	}

	actual, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, actual.Validate(len(records)))
	require.Len(t, actual, len(records))
	assertOutcome(t, actual, 0, spool.RecordOutcomeStatusRetryable, now)
	assertOutcome(t, actual, 1, spool.RecordOutcomeStatusInvalid, now)
	assertOutcome(t, actual, 2, spool.RecordOutcomeStatusSuccess, now)
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusSuccess))
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusRetryable))
	assert.Equal(t, 1, countOutcomeStatus(actual, spool.RecordOutcomeStatusInvalid))
	assert.Equal(t, 1, client.indexDocumentsCallCount)
	require.Len(t, client.indexDocumentsDocs, 3)
	assert.Equal(t, []string{"same-doc", "same-doc", "unique-doc"}, documentIDs(client.indexDocumentsDocs))
}

func TestSendClassifiesRequestLevelHTTPError(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 15, 0, 0, time.UTC)
	tests := []struct {
		name       string
		statusCode int
		wantStatus spool.RecordOutcomeStatus
	}{
		{name: "service unavailable is retryable", statusCode: 503, wantStatus: spool.RecordOutcomeStatusRetryable},
		{name: "forbidden is permanent", statusCode: 403, wantStatus: spool.RecordOutcomeStatusPermanent},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			requestErr := &pkgopensearch.HTTPStatusError{Code: tt.statusCode}
			client := &fakeOpenSearchClient{indexDocumentsErr: requestErr}
			sender := newTestSender(now, client)

			report, err := sender.Send(
				context.Background(),
				[]spool.SpoolRecord{validSpoolRecord(t, "request", "logs", "doc")},
			)

			require.NoError(t, err)
			require.NoError(t, report.Validate(1))
			outcome := assertOutcome(t, report, 0, tt.wantStatus, now)
			assert.ErrorIs(t, outcome.Err, requestErr)
			assert.Equal(t, 1, client.indexDocumentsCallCount)
		})
	}
}

func TestSendClassifiesDurableRequestErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 20, 0, 0, time.UTC)
	tests := []struct {
		name       string
		err        error
		wantStatus spool.RecordOutcomeStatus
	}{
		{
			name: "circuit breaker open is retryable",
			err: retryable.NewCircuitBreakerOpenError(
				"opensearch bulk",
				now.Add(time.Minute),
			),
			wantStatus: spool.RecordOutcomeStatusRetryable,
		},
		{
			name:       "unknown result is quarantined",
			err:        errors.New("unclassified result"),
			wantStatus: spool.RecordOutcomeStatusInvalid,
		},
		{
			name: "response decode failure is quarantined",
			err: &pkgopensearch.ResponseDecodeError{
				Operation: "bulk",
				Err:       errors.New("malformed response"),
			},
			wantStatus: spool.RecordOutcomeStatusInvalid,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeOpenSearchClient{indexDocumentsErr: tt.err}
			sender := newTestSender(now, client)
			report, err := sender.Send(
				context.Background(),
				[]spool.SpoolRecord{validSpoolRecord(t, "request", "logs", "doc")},
			)

			require.NoError(t, err)
			require.NoError(t, report.Validate(1))
			outcome := assertOutcome(t, report, 0, tt.wantStatus, now)
			assert.ErrorIs(t, outcome.Err, tt.err)
			assert.Equal(t, 1, client.indexDocumentsCallCount)
		})
	}
}

func TestSendRejectsOversizedRequestBeforeBackendAndSendsRemainingRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 25, 0, 0, time.UTC)
	oversizedDocument := pkgopensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    "oversized",
		Doc:   map[string]any{"message": "document exceeds configured limit"},
		Op:    pkgopensearch.WriteOpIndex,
	}
	validDocument := pkgopensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    "valid",
		Doc:   map[string]any{"message": "valid"},
		Op:    pkgopensearch.WriteOpIndex,
	}
	oversizedRequestSize, err := pkgopensearch.BulkRequestSize(
		[]pkgopensearch.DocumentWriteRequest{oversizedDocument},
	)
	require.NoError(t, err)
	client := &fakeOpenSearchClient{
		indexDocumentsResponse: successBulkResponse("logs", "valid"),
	}
	sender := newTestSender(now, client)
	sender.maxBulkRequestBytes = oversizedRequestSize - 1
	records := []spool.SpoolRecord{
		spoolRecordForDocument(t, "oversized-request", oversizedDocument),
		spoolRecordForDocument(t, "valid-request", validDocument),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	rejected := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, rejected.Err, infraopensearch.ErrRequestTooLarge)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.indexDocumentsCallCount)
	assert.Equal(t, []string{"valid"}, documentIDs(client.indexDocumentsDocs))
}

func TestSendRejectsRecordWhoseBulkMetadataExceedsRequestLimitBeforeBackend(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 26, 0, 0, time.UTC)
	document := pkgopensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    strings.Repeat("metadata-id-", 32),
		Doc:   map[string]any{"message": "small"},
		Op:    pkgopensearch.WriteOpIndex,
	}
	documentSize, err := infraopensearch.DocumentBodySize(document)
	require.NoError(t, err)
	recordSize, err := pkgopensearch.BulkRequestSize([]pkgopensearch.DocumentWriteRequest{document})
	require.NoError(t, err)
	require.Less(t, documentSize, recordSize-1)
	client := &fakeOpenSearchClient{}
	sender := newTestSender(now, client)
	sender.maxBulkRequestBytes = recordSize - 1

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{
		spoolRecordForDocument(t, "metadata-too-large", document),
	})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	rejected := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, rejected.Err, infraopensearch.ErrRequestTooLarge)
	assert.Zero(t, client.indexDocumentsCallCount)
}

func TestSendSplitsByConfiguredCompleteNDJSONLimitBeforeBackend(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 27, 0, 0, time.UTC)
	documents := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Doc: map[string]any{"message": "one"}, Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-2", Doc: map[string]any{"message": "two"}, Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-3", Doc: map[string]any{"message": "three"}, Op: pkgopensearch.WriteOpIndex},
	}
	twoDocumentSize, err := pkgopensearch.BulkRequestSize(documents[:2])
	require.NoError(t, err)
	client := &fakeOpenSearchClient{
		indexDocuments: func(
			_ context.Context,
			docs []pkgopensearch.DocumentWriteRequest,
		) (*pkgopensearch.BulkResponse, error) {
			return successBulkResponseForDocuments(docs), nil
		},
	}
	sender := newTestSender(now, client)
	sender.maxBulkRequestBytes = twoDocumentSize
	records := make([]spool.SpoolRecord, 0, len(documents))
	for index, document := range documents {
		records = append(records, spoolRecordForDocument(
			t,
			fmt.Sprintf("request-%d", index+1),
			document,
		))
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for inputIndex := range records {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusSuccess, now)
	}
	assert.Equal(t, []int{2, 1}, documentCounts(client.indexDocumentsCalls))
	firstRequestSize, err := pkgopensearch.BulkRequestSize(client.indexDocumentsCalls[0])
	require.NoError(t, err)
	assert.Equal(t, twoDocumentSize, firstRequestSize)
}

func TestSendReturnsUnattemptedPlannedRequestsToRetryableWhenContextEnds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 28, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	documents := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Doc: map[string]any{"message": "one"}, Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-2", Doc: map[string]any{"message": "two"}, Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-3", Doc: map[string]any{"message": "three"}, Op: pkgopensearch.WriteOpIndex},
	}
	twoDocumentSize, err := pkgopensearch.BulkRequestSize(documents[:2])
	require.NoError(t, err)
	client := &fakeOpenSearchClient{
		indexDocuments: func(
			_ context.Context,
			docs []pkgopensearch.DocumentWriteRequest,
		) (*pkgopensearch.BulkResponse, error) {
			cancel()
			return successBulkResponseForDocuments(docs), nil
		},
	}
	sender := newTestSender(now, client)
	sender.maxBulkRequestBytes = twoDocumentSize
	records := make([]spool.SpoolRecord, 0, len(documents))
	for index, document := range documents {
		records = append(records, spoolRecordForDocument(
			t,
			fmt.Sprintf("request-%d", index+1),
			document,
		))
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
	retryable := assertOutcome(t, report, 2, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, retryable.Err, context.Canceled)
	assert.Equal(t, []int{2}, documentCounts(client.indexDocumentsCalls))
}

func TestSendSplitsRequestEntityTooLargeBatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 30, 0, 0, time.UTC)
	requestErr := &pkgopensearch.HTTPStatusError{Code: 413}
	client := &fakeOpenSearchClient{
		indexDocuments: func(
			_ context.Context,
			docs []pkgopensearch.DocumentWriteRequest,
		) (*pkgopensearch.BulkResponse, error) {
			if len(docs) > 1 {
				return nil, requestErr
			}
			return successBulkResponse(docs[0].Index, docs[0].ID), nil
		},
	}
	sender := newTestSender(now, client)
	records := []spool.SpoolRecord{
		validSpoolRecord(t, "request-1", "logs", "doc-1"),
		validSpoolRecord(t, "request-2", "logs", "doc-2"),
		validSpoolRecord(t, "request-3", "logs", "doc-3"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assert.Equal(t, len(records), countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
	// 最初はbatch全体を送り、413で拒否されたrequestを分割して、各documentを一度だけ配送します。
	// 分割の順序と粒度には依存しません。
	require.NotEmpty(t, client.indexDocumentsCalls)
	assert.ElementsMatch(t, []string{"doc-1", "doc-2", "doc-3"}, documentIDs(client.indexDocumentsCalls[0]))
	assert.ElementsMatch(t, []string{"doc-1", "doc-2", "doc-3"}, acceptedDocumentIDs(client.indexDocumentsCalls, func(docs []pkgopensearch.DocumentWriteRequest) bool {
		return len(docs) == 1
	}))
}

func TestSendClassifiesSingleDocumentRequestEntityTooLargeAsInvalidAfterAttempt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 45, 0, 0, time.UTC)
	requestErr := &pkgopensearch.HTTPStatusError{Code: 413}
	client := &fakeOpenSearchClient{indexDocumentsErr: requestErr}
	sender := newTestSender(now, client)

	report, err := sender.Send(
		context.Background(),
		[]spool.SpoolRecord{validSpoolRecord(t, "request", "logs", "too-large")},
	)

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	outcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, now)
	assert.ErrorIs(t, outcome.Err, requestErr)
	assert.Equal(t, 1, client.indexDocumentsCallCount)
}

func TestConvertSpoolRecordToWriteDocumentRequestValidatesPayload(t *testing.T) {
	t.Parallel()

	validReq := pkgopensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    "doc-valid",
		Doc:   map[string]any{"message": "ok"},
		Op:    pkgopensearch.WriteOpIndex,
	}
	tests := []struct {
		name       string
		record     spool.SpoolRecord
		want       pkgopensearch.DocumentWriteRequest
		wantErr    error
		wantField  string
		wantReason error
		// storedValueは、errors.Asで取り出せるfieldや原因に残してはならない保存された値です。
		storedValue string
	}{
		{
			name:   "valid",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, validReq)},
			want:   validReq,
		},
		{
			name:    "empty payload",
			record:  spool.SpoolRecord{},
			wantErr: ErrEmptySpoolPayload,
		},
		{
			name:    "invalid json",
			record:  spool.SpoolRecord{Payload: []byte("{")},
			wantErr: ErrDecodeSpoolPayload,
		},
		{
			name: "hard-coded direct payload",
			record: spool.SpoolRecord{Payload: []byte(
				`{"index":"direct-logs","id":"direct-doc","doc":{"message":"direct"},"op":"create"}`,
			)},
			want: pkgopensearch.DocumentWriteRequest{
				Index: "direct-logs",
				ID:    "direct-doc",
				Doc:   map[string]any{"message": "direct"},
				Op:    pkgopensearch.WriteOpCreate,
			},
		},
		{
			name: "missing index",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				ID:  "doc-missing-index",
				Doc: map[string]any{"message": "missing index"},
				Op:  pkgopensearch.WriteOpIndex,
			})},
			wantErr:   ErrMissingSpoolPayloadField,
			wantField: "index",
		},
		{
			name: "blank index",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				Index: "   ",
				ID:    "doc-blank-index",
				Doc:   map[string]any{"message": "blank index"},
				Op:    pkgopensearch.WriteOpIndex,
			})},
			wantErr:     ErrInvalidSpoolPayloadField,
			wantField:   "index",
			wantReason:  pkgopensearch.ErrIndexRequired,
			storedValue: "   ",
		},
		{
			name: "index with surrounding whitespace",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				Index: " stored-index ",
				ID:    "doc-padded-index",
				Doc:   map[string]any{"message": "padded index"},
				Op:    pkgopensearch.WriteOpIndex,
			})},
			wantErr:     ErrInvalidSpoolPayloadField,
			wantField:   "index",
			wantReason:  pkgopensearch.ErrIndexHasSurroundingWhitespace,
			storedValue: " stored-index ",
		},
		{
			name: "missing id",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				Index: "logs",
				Doc:   map[string]any{"message": "missing id"},
				Op:    pkgopensearch.WriteOpIndex,
			})},
			wantErr:   ErrMissingSpoolPayloadField,
			wantField: "id",
		},
		{
			name: "nil doc",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				Index: "logs",
				ID:    "doc-nil-doc",
				Op:    pkgopensearch.WriteOpIndex,
			})},
			wantErr:   ErrMissingSpoolPayloadField,
			wantField: "doc",
		},
		{
			name: "missing op",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				Index: "logs",
				ID:    "doc-missing-op",
				Doc:   map[string]any{"message": "missing op"},
			})},
			wantErr:   ErrMissingSpoolPayloadField,
			wantField: "op",
		},
		{
			name: "unsupported op",
			record: spool.SpoolRecord{Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
				Index: "logs",
				ID:    "doc-unsupported-op",
				Doc:   map[string]any{"message": "unsupported op"},
				Op:    pkgopensearch.WriteOp("update"),
			})},
			wantErr:     ErrInvalidSpoolPayloadField,
			wantField:   "op",
			storedValue: "update",
		},
	}

	sender := &OpensearchSpoolSender{}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := sender.convertSpoolRecordToWriteDocumentRequest(tt.record)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				var fieldErr *SpoolPayloadFieldError
				if tt.wantField == "" {
					assert.False(t, errors.As(err, &fieldErr), "payload-level error must not name a field: %v", err)
					return
				}
				require.ErrorAs(t, err, &fieldErr)
				assert.Equal(t, tt.wantField, fieldErr.Field)
				if tt.wantReason != nil {
					// index名の検証はpkg/opensearchが担当します。値を含まないその理由を原因として保持します。
					assert.ErrorIs(t, err, tt.wantReason)
				}
				if tt.storedValue != "" {
					// 保存されたpayloadの値は、errors.Asで取り出せるfieldにも原因にも残しません。
					location, found := errortree.ExportedFieldContaining(err, tt.storedValue)
					assert.False(t, found, "%s keeps the stored value", location)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, actual)
		})
	}
}

func TestNewOpensearchSpoolSender(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 16, 0, 0, 0, time.UTC)
	client := &fakeOpenSearchClient{}

	sender, err := NewOpensearchSpoolSender(fixedClock{now: now}, client, testMaxBulkRequestBytes())

	require.NoError(t, err)
	require.NotNil(t, sender)
	assert.Equal(t, now, sender.Clock.Now())
	assert.Same(t, client, sender.osClient)

	_, err = NewOpensearchSpoolSender(nil, client, testMaxBulkRequestBytes())
	assert.ErrorIs(t, err, ErrNilClock)

	_, err = NewOpensearchSpoolSender(fixedClock{now: now}, nil, testMaxBulkRequestBytes())
	assert.ErrorIs(t, err, ErrNilClient)

	_, err = NewOpensearchSpoolSender(
		fixedClock{now: now},
		client,
		0,
	)
	assert.ErrorIs(t, err, infraopensearch.ErrInvalidMaxBulkRequestBytes)
}

func newTestSender(now time.Time, client *fakeOpenSearchClient) *OpensearchSpoolSender {
	return &OpensearchSpoolSender{
		Clock:               fixedClock{now: now},
		osClient:            client,
		maxBulkRequestBytes: testMaxBulkRequestBytes(),
	}
}

func testMaxBulkRequestBytes() int {
	return 5 * 1024 * 1024
}

func validSpoolRecord(t *testing.T, requestID string, index string, docID string) spool.SpoolRecord {
	t.Helper()

	return spool.SpoolRecord{
		OccurredAt: time.Date(2026, 6, 9, 1, 2, 3, 0, time.UTC),
		RequestID:  requestID,
		Payload: mustMarshalDocumentWriteRequest(t, pkgopensearch.DocumentWriteRequest{
			Index: index,
			ID:    docID,
			Doc:   map[string]any{"request_id": requestID},
			Op:    pkgopensearch.WriteOpIndex,
		}),
	}
}

func spoolRecordForDocument(
	t *testing.T,
	requestID string,
	document pkgopensearch.DocumentWriteRequest,
) spool.SpoolRecord {
	t.Helper()

	return spool.SpoolRecord{
		OccurredAt: time.Date(2026, 6, 9, 1, 2, 3, 0, time.UTC),
		RequestID:  requestID,
		Payload:    mustMarshalDocumentWriteRequest(t, document),
	}
}

func mustMarshalDocumentWriteRequest(
	t *testing.T,
	req pkgopensearch.DocumentWriteRequest,
) []byte {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"index": req.Index,
		"id":    req.ID,
		"doc":   req.Doc,
		"op":    req.Op,
	})
	require.NoError(t, err)

	return payload
}

func successBulkResponse(index string, id string) *pkgopensearch.BulkResponse {
	return &pkgopensearch.BulkResponse{
		Errors: false,
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkItem("index", index, id, 201, "", ""),
		},
	}
}

func successBulkResponseForDocuments(
	documents []pkgopensearch.DocumentWriteRequest,
) *pkgopensearch.BulkResponse {
	items := make([]map[string]pkgopensearch.BulkItemResult, 0, len(documents))
	for _, document := range documents {
		items = append(items, bulkItem(
			string(document.Op),
			document.Index,
			document.ID,
			http.StatusCreated,
			"",
			"",
		))
	}
	return &pkgopensearch.BulkResponse{Errors: false, Items: items}
}

func bulkItem(
	action string,
	index string,
	id string,
	status int,
	errType string,
	errReason string,
) map[string]pkgopensearch.BulkItemResult {
	var detail *pkgopensearch.ErrorDetail
	if errType != "" || errReason != "" {
		detail = &pkgopensearch.ErrorDetail{
			Type:   pkgopensearch.ErrorType(errType),
			Reason: errReason,
		}
	}
	result := ""
	if status >= 200 && status < 300 {
		if action == string(pkgopensearch.WriteOpIndex) && status == http.StatusOK {
			result = "updated"
		} else {
			result = "created"
		}
	}

	return map[string]pkgopensearch.BulkItemResult{
		action: {
			Index:  index,
			ID:     id,
			Status: status,
			Result: result,
			Error:  detail,
		},
	}
}

func documentIDs(docs []pkgopensearch.DocumentWriteRequest) []string {
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}

	return ids
}

// callDocumentIDsは、送信したrequestごとのdocument IDを返します。
func callDocumentIDs(calls [][]pkgopensearch.DocumentWriteRequest) [][]string {
	ids := make([][]string, 0, len(calls))
	for _, docs := range calls {
		ids = append(ids, documentIDs(docs))
	}
	return ids
}

// acceptedDocumentIDsは、acceptedが受理したとみなすrequestのdocument IDを送信順に連結します。
// 同じdocumentを二度受理した場合は重複して現れます。
func acceptedDocumentIDs(
	calls [][]pkgopensearch.DocumentWriteRequest,
	accepted func([]pkgopensearch.DocumentWriteRequest) bool,
) []string {
	var ids []string
	for _, docs := range calls {
		if accepted(docs) {
			ids = append(ids, documentIDs(docs)...)
		}
	}
	return ids
}

func documentCounts(calls [][]pkgopensearch.DocumentWriteRequest) []int {
	counts := make([]int, 0, len(calls))
	for _, docs := range calls {
		counts = append(counts, len(docs))
	}
	return counts
}

func assertOutcome(
	t *testing.T,
	report spool.ExportReport,
	wantInputIndex int,
	wantStatus spool.RecordOutcomeStatus,
	wantAttemptedAt time.Time,
) spool.RecordOutcome {
	t.Helper()

	var matched []spool.RecordOutcome
	for _, outcome := range report {
		if outcome.InputIndex == wantInputIndex {
			matched = append(matched, outcome)
		}
	}
	require.Len(t, matched, 1, "outcomes for input index %d", wantInputIndex)
	actual := matched[0]
	assert.Equal(t, wantInputIndex, actual.InputIndex)
	assert.Equal(t, wantStatus, actual.Status)
	assert.True(
		t,
		actual.AttemptedAt.Equal(wantAttemptedAt),
		"attempted at = %v, want %v",
		actual.AttemptedAt,
		wantAttemptedAt,
	)
	if wantStatus == spool.RecordOutcomeStatusSuccess {
		assert.NoError(t, actual.Err)
	} else {
		assert.Error(t, actual.Err)
	}
	return actual
}

func countOutcomeStatus(report spool.ExportReport, status spool.RecordOutcomeStatus) int {
	count := 0
	for _, outcome := range report {
		if outcome.Status == status {
			count++
		}
	}
	return count
}

// cloneSpoolRecordsは、Sendへ渡すrecordとpayloadの内容を送信前の値として複製します。
// 送信後に渡したsliceの要素と比較し、senderが入力recordを変更しないことを確認します。
func cloneSpoolRecords(records []spool.SpoolRecord) []spool.SpoolRecord {
	cloned := make([]spool.SpoolRecord, len(records))
	for index, record := range records {
		record.Payload = append(json.RawMessage(nil), record.Payload...)
		cloned[index] = record
	}
	return cloned
}

func TestSendIsolatesDocumentRejectedForWholeBulkRequest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 45, 0, 0, time.UTC)
	payloadErr := &pkgopensearch.HTTPStatusError{
		Code: http.StatusBadRequest,
		Body: []byte(`{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}`),
	}
	client := &fakeOpenSearchClient{
		indexDocuments: func(
			_ context.Context,
			docs []pkgopensearch.DocumentWriteRequest,
		) (*pkgopensearch.BulkResponse, error) {
			// request全体を拒否したため、どのdocumentも処理されていません。
			for _, doc := range docs {
				if doc.ID == "doc-bad" {
					return nil, payloadErr
				}
			}
			return successBulkResponseForDocuments(docs), nil
		},
	}
	sender := newTestSender(now, client)
	records := []spool.SpoolRecord{
		validSpoolRecord(t, "request-1", "logs", "doc-1"),
		validSpoolRecord(t, "request-2", "logs", "doc-bad"),
		validSpoolRecord(t, "request-3", "logs", "doc-3"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	// 分割して原因のdocumentだけを入力不備として隔離し、他のdocumentは配送します。
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	rejected := assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	assert.ErrorIs(t, rejected.Err, payloadErr)
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	// 最初はbatch全体を送り、分割後は原因のdocumentを単独で送って拒否を確認します。
	// 他のdocumentは一度だけ配送し、分割の順序と粒度には依存しません。
	require.NotEmpty(t, client.indexDocumentsCalls)
	assert.ElementsMatch(t, []string{"doc-1", "doc-bad", "doc-3"}, documentIDs(client.indexDocumentsCalls[0]))
	assert.ElementsMatch(t, []string{"doc-1", "doc-3"}, acceptedDocumentIDs(client.indexDocumentsCalls, func(docs []pkgopensearch.DocumentWriteRequest) bool {
		return !slices.ContainsFunc(docs, func(doc pkgopensearch.DocumentWriteRequest) bool { return doc.ID == "doc-bad" })
	}))
	assert.Contains(t, callDocumentIDs(client.indexDocumentsCalls), []string{"doc-bad"}, "the rejected document is confirmed alone")
}

func TestSendKeepsRequestConfigurationRejectionDeadWithoutSplitting(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 15, 50, 0, 0, time.UTC)
	configErr := &pkgopensearch.HTTPStatusError{
		Code: http.StatusBadRequest,
		Body: []byte(`{"error":{"type":"illegal_argument_exception","reason":"request configuration failed"},"status":400}`),
	}
	client := &fakeOpenSearchClient{indexDocumentsErr: configErr}
	sender := newTestSender(now, client)
	records := []spool.SpoolRecord{
		validSpoolRecord(t, "request-1", "logs", "doc-1"),
		validSpoolRecord(t, "request-2", "logs", "doc-2"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	// document内容に依存しないrequest設定の不備は、分けて再送しても解消しないため配送不能です。
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusPermanent, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusPermanent, now)
	assert.Equal(t, 1, client.indexDocumentsCallCount)
}
