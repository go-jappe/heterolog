package opensearch

import (
	"context"
	"net/http"
	"testing"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_IsolatesDocumentRejectedForWholeBulk(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	accepted := make(map[any]int)
	mockClient.bulkHandler = func(
		_ context.Context,
		docs []pkgopensearch.DocumentWriteRequest,
	) (*pkgopensearch.BulkResponse, error) {
		// request全体を拒否したため、どのdocumentも処理されていません。
		for _, doc := range docs {
			if doc.Doc["message"] == "bad document" {
				return nil, &pkgopensearch.HTTPStatusError{
					Code: http.StatusBadRequest,
					Body: []byte(`{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}`),
				}
			}
		}
		for _, doc := range docs {
			accepted[doc.Doc["message"]]++
		}
		return successBulkResponse(docs), nil
	}
	flushManager := newTestFlushManager(
		mockClient,
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)
	docs := []map[string]any{
		{"message": "first"},
		{"message": "bad document"},
		{"message": "third"},
	}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)

	require.NoError(t, err)
	// 分割して原因のdocumentだけを入力不備として隔離し、他のdocumentは配送します。
	assert.Equal(t, 2, res.SuccessCount())
	require.Len(t, res.QuarantineRecords, 1)
	assert.Empty(t, res.RetryableRecords)
	assert.Empty(t, res.DeadRecords)
	assert.Equal(t, http.StatusBadRequest, res.QuarantineRecords[0].StatusCode)
	assert.Equal(t, docs[1], res.QuarantineRecords[0].Payload["doc"])
	// 分割の位置や順序ではなく、正常なdocumentが一度だけ受理され、送信回数が二分割の上限
	// （2*入力件数-1回）以内であることを検証します。
	assert.Equal(t, map[any]int{"first": 1, "third": 1}, accepted)
	assert.LessOrEqual(t, mockClient.IndexDocumentsCallCount, 2*len(docs)-1)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_KeepsRequestConfigurationRejectionDead(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.bulkErr = &pkgopensearch.HTTPStatusError{
		Code: http.StatusBadRequest,
		Body: []byte(`{"error":{"type":"illegal_argument_exception","reason":"request configuration failed"},"status":400}`),
	}
	flushManager := newTestFlushManager(
		mockClient,
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, []map[string]any{{"message": "first"}, {"message": "second"}})

	require.NoError(t, err)
	// document内容に依存しないrequest設定の不備は、分けて再送しても解消しないため配送不能です。
	assert.Len(t, res.DeadRecords, 2)
	assert.Empty(t, res.QuarantineRecords)
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
}
