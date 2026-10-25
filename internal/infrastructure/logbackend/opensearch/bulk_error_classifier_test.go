package opensearch

import (
	"errors"
	"testing"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyFailedBulkDocsValidatesCompleteResponseWhenErrorsIsFalse(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: pkgopensearch.WriteOpIndex},
	}
	resp := &pkgopensearch.BulkResponse{
		Errors: false,
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkClassifierItem("index", "doc-1", 201, nil),
		},
	}

	classification, err := ClassifyFailedBulkDocs(resp, reqs)

	require.NoError(t, err)
	assert.Empty(t, classification.RetryableFailures)
	assert.Empty(t, classification.DeadFailures)
	assert.Empty(t, classification.QuarantineFailures)
}

func TestClassifyFailedBulkDocsRejectsMissingResponseItemRegardlessOfErrorsFlag(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-2", Op: pkgopensearch.WriteOpIndex},
	}

	for _, errorsFlag := range []bool{false, true} {
		errorsFlag := errorsFlag
		t.Run(map[bool]string{false: "errors_false", true: "errors_true"}[errorsFlag], func(t *testing.T) {
			t.Parallel()

			resp := &pkgopensearch.BulkResponse{
				Errors: errorsFlag,
				Items: []map[string]pkgopensearch.BulkItemResult{
					bulkClassifierItem("index", "doc-1", 201, nil),
				},
			}

			classification, err := ClassifyFailedBulkDocs(resp, reqs)

			requireBulkResponseValidationError(t, err, BulkResponseValidationError{
				Kind: ErrBulkResponseItemCountMismatch, Index: -1, GotItems: 1, WantItems: 2,
			})
			assert.Empty(t, classification.RetryableFailures)
			assert.Empty(t, classification.DeadFailures)
			assert.Empty(t, classification.QuarantineFailures)
		})
	}
}

func TestClassifyFailedBulkDocsRejectsExtraResponseItem(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: pkgopensearch.WriteOpIndex},
	}
	resp := &pkgopensearch.BulkResponse{
		Errors: true,
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkClassifierItem("index", "doc-1", 201, nil),
			bulkClassifierItem("index", "doc-1", 503, &pkgopensearch.ErrorDetail{
				Type:   "unavailable",
				Reason: "node unavailable",
			}),
		},
	}

	classification, err := ClassifyFailedBulkDocs(resp, reqs)

	requireBulkResponseValidationError(t, err, BulkResponseValidationError{
		Kind: ErrBulkResponseItemCountMismatch, Index: -1, GotItems: 2, WantItems: 1,
	})
	assert.Empty(t, classification.RetryableFailures)
	assert.Empty(t, classification.DeadFailures)
	assert.Empty(t, classification.QuarantineFailures)
}

func TestClassifyFailedBulkDocsRejectsInvalidItemStatus(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: pkgopensearch.WriteOpIndex},
	}
	tests := []struct {
		name     string
		status   int
		detail   *pkgopensearch.ErrorDetail
		wantKind error
	}{
		{
			name:     "missing_status",
			status:   0,
			wantKind: ErrBulkResponseItemInvalidStatus,
		},
		{
			name:     "status_above_HTTP_range",
			status:   600,
			wantKind: ErrBulkResponseItemInvalidStatus,
		},
		{
			name:   "success_status_with_error_object",
			status: 200,
			detail: &pkgopensearch.ErrorDetail{
				Type:   "unexpected_error",
				Reason: "contradictory response",
			},
			wantKind: ErrBulkResponseItemSuccessWithError,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := &pkgopensearch.BulkResponse{
				Errors: tt.detail != nil,
				Items: []map[string]pkgopensearch.BulkItemResult{
					bulkClassifierItem("index", "doc-1", tt.status, tt.detail),
				},
			}

			classification, err := ClassifyFailedBulkDocs(resp, reqs)

			requireBulkResponseValidationError(t, err, BulkResponseValidationError{Kind: tt.wantKind, Index: 0, Status: tt.status})
			assert.Empty(t, classification.RetryableFailures)
			assert.Empty(t, classification.DeadFailures)
			assert.Empty(t, classification.QuarantineFailures)
		})
	}
}

func TestClassifyFailedBulkDocsRejectsMissingResponseIndex(t *testing.T) {
	t.Parallel()

	item := bulkClassifierItem("index", "doc-1", 201, nil)
	result := item["index"]
	result.Index = ""
	item["index"] = result

	classification, err := ClassifyFailedBulkDocs(
		&pkgopensearch.BulkResponse{Items: []map[string]pkgopensearch.BulkItemResult{item}},
		[]pkgopensearch.DocumentWriteRequest{{
			Index: "logs-write",
			ID:    "doc-1",
			Op:    pkgopensearch.WriteOpIndex,
		}},
	)

	requireBulkResponseValidationError(t, err, BulkResponseValidationError{Kind: ErrBulkResponseItemMissingIndex, Index: 0})
	assert.Empty(t, classification.RetryableFailures)
	assert.Empty(t, classification.DeadFailures)
	assert.Empty(t, classification.QuarantineFailures)
}

func TestClassifyFailedBulkDocsAcceptsOperationSpecificSuccessResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		op     pkgopensearch.WriteOp
		status int
		result string
	}{
		{name: "create created", op: pkgopensearch.WriteOpCreate, status: 201, result: "created"},
		{name: "index created", op: pkgopensearch.WriteOpIndex, status: 201, result: "created"},
		{name: "index updated", op: pkgopensearch.WriteOpIndex, status: 200, result: "updated"},
		{name: "create dropped by pipeline", op: pkgopensearch.WriteOpCreate, status: 200, result: "noop"},
		{name: "index dropped by pipeline", op: pkgopensearch.WriteOpIndex, status: 200, result: "noop"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			item := bulkClassifierItem(string(tt.op), "doc-1", tt.status, nil)
			bulkResult := item[string(tt.op)]
			bulkResult.Result = tt.result
			item[string(tt.op)] = bulkResult

			classification, err := ClassifyFailedBulkDocs(
				&pkgopensearch.BulkResponse{Items: []map[string]pkgopensearch.BulkItemResult{item}},
				[]pkgopensearch.DocumentWriteRequest{{Index: "logs", ID: "doc-1", Op: tt.op}},
			)

			require.NoError(t, err)
			assert.Empty(t, classification.RetryableFailures)
			assert.Empty(t, classification.DeadFailures)
			assert.Empty(t, classification.QuarantineFailures)
		})
	}
}

func TestClassifyFailedBulkDocsRejectsMalformedOperationSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		op     pkgopensearch.WriteOp
		status int
		result string
	}{
		{name: "create status 200", op: pkgopensearch.WriteOpCreate, status: 200, result: "created"},
		{name: "create result updated", op: pkgopensearch.WriteOpCreate, status: 201, result: "updated"},
		{name: "index status 200 with created", op: pkgopensearch.WriteOpIndex, status: 200, result: "created"},
		{name: "index status 201 with updated", op: pkgopensearch.WriteOpIndex, status: 201, result: "updated"},
		{name: "index unexpected 2xx", op: pkgopensearch.WriteOpIndex, status: 202, result: "created"},
		{name: "index missing result", op: pkgopensearch.WriteOpIndex, status: 201, result: ""},
		{name: "create noop with created status", op: pkgopensearch.WriteOpCreate, status: 201, result: "noop"},
		{name: "index noop with created status", op: pkgopensearch.WriteOpIndex, status: 201, result: "noop"},
		{name: "index noop with accepted status", op: pkgopensearch.WriteOpIndex, status: 202, result: "noop"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			item := bulkClassifierItem(string(tt.op), "doc-1", tt.status, nil)
			bulkResult := item[string(tt.op)]
			bulkResult.Result = tt.result
			item[string(tt.op)] = bulkResult

			classification, err := ClassifyFailedBulkDocs(
				&pkgopensearch.BulkResponse{Items: []map[string]pkgopensearch.BulkItemResult{item}},
				[]pkgopensearch.DocumentWriteRequest{{Index: "logs", ID: "doc-1", Op: tt.op}},
			)

			requireBulkResponseValidationError(t, err, BulkResponseValidationError{
				Kind: ErrBulkResponseItemInvalidSuccessResult, Index: 0, WantAction: string(tt.op), Status: tt.status, Result: tt.result,
			})
			assert.Empty(t, classification.RetryableFailures)
			assert.Empty(t, classification.DeadFailures)
			assert.Empty(t, classification.QuarantineFailures)
		})
	}
}

func TestClassifyFailedBulkDocsAcceptsResolvedIndexForAliasOrDataStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		detail *pkgopensearch.ErrorDetail
	}{
		{name: "success", status: 201},
		{
			name:   "failure",
			status: 400,
			detail: &pkgopensearch.ErrorDetail{
				Type:   pkgopensearch.ErrorTypeMapperParsing,
				Reason: "mapping failed",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			item := bulkClassifierItem("index", "doc-1", tt.status, tt.detail)
			bulkResult := item["index"]
			bulkResult.Index = "logs-000001"
			item["index"] = bulkResult

			classification, err := ClassifyFailedBulkDocs(
				&pkgopensearch.BulkResponse{Items: []map[string]pkgopensearch.BulkItemResult{item}},
				[]pkgopensearch.DocumentWriteRequest{{
					Index: "logs-write",
					ID:    "doc-1",
					Op:    pkgopensearch.WriteOpIndex,
				}},
			)

			require.NoError(t, err)
			assert.Empty(t, classification.RetryableFailures)
			assert.Empty(t, classification.DeadFailures)
			if tt.status < 300 {
				assert.Empty(t, classification.QuarantineFailures)
				return
			}
			require.Len(t, classification.QuarantineFailures, 1)
			assert.Equal(t, 0, classification.QuarantineFailures[0].InputIndex)
		})
	}
}

func TestClassifyFailedBulkDocsClassifiesCompleteMixedResponse(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "success", Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "retryable", Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "quarantine", Op: pkgopensearch.WriteOpIndex},
	}
	resp := &pkgopensearch.BulkResponse{
		Errors: true,
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkClassifierItem("index", "success", 201, nil),
			bulkClassifierItem("index", "retryable", 503, &pkgopensearch.ErrorDetail{
				Type:   "unavailable",
				Reason: "node unavailable",
			}),
			bulkClassifierItem("index", "quarantine", 400, &pkgopensearch.ErrorDetail{
				Type:   "mapper_parsing_exception",
				Reason: "mapping failed",
			}),
		},
	}

	classification, err := ClassifyFailedBulkDocs(resp, reqs)

	require.NoError(t, err)
	require.Len(t, classification.RetryableFailures, 1)
	assert.Equal(t, "retryable", classification.RetryableFailures[0].Request.ID)
	assert.Equal(t, 1, classification.RetryableFailures[0].InputIndex)
	assert.Empty(t, classification.DeadFailures)
	require.Len(t, classification.QuarantineFailures, 1)
	assert.Equal(t, "quarantine", classification.QuarantineFailures[0].Request.ID)
	assert.Equal(t, 2, classification.QuarantineFailures[0].InputIndex)
}

func TestClassifyFailedBulkDocsAllowsDuplicateRequestIDsByInputOrder(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "same-id", Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "same-id", Op: pkgopensearch.WriteOpIndex},
	}
	resp := &pkgopensearch.BulkResponse{
		Errors: true,
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkClassifierItem("index", "same-id", 503, &pkgopensearch.ErrorDetail{
				Type:   "unavailable",
				Reason: "node unavailable",
			}),
			bulkClassifierItem("index", "same-id", 400, &pkgopensearch.ErrorDetail{
				Type:   "mapper_parsing_exception",
				Reason: "mapping failed",
			}),
		},
	}

	classification, err := ClassifyFailedBulkDocs(resp, reqs)

	require.NoError(t, err)
	require.Len(t, classification.RetryableFailures, 1)
	assert.Equal(t, 0, classification.RetryableFailures[0].InputIndex)
	assert.Empty(t, classification.DeadFailures)
	require.Len(t, classification.QuarantineFailures, 1)
	assert.Equal(t, 1, classification.QuarantineFailures[0].InputIndex)
}

func TestClassifyFailedBulkDocsRejectsResponseOrderMismatch(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: pkgopensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-2", Op: pkgopensearch.WriteOpIndex},
	}
	resp := &pkgopensearch.BulkResponse{
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkClassifierItem("index", "doc-2", 201, nil),
			bulkClassifierItem("index", "doc-1", 201, nil),
		},
	}

	classification, err := ClassifyFailedBulkDocs(resp, reqs)

	requireBulkResponseValidationError(t, err, BulkResponseValidationError{
		Kind: ErrBulkResponseItemIDMismatch, Index: 0, GotID: "doc-2", WantID: "doc-1",
	})
	assert.Empty(t, classification.RetryableFailures)
	assert.Empty(t, classification.DeadFailures)
	assert.Empty(t, classification.QuarantineFailures)
}

func TestClassifyFailedBulkDocsTreatsFixedIDCreateConflictAsSuccess(t *testing.T) {
	t.Parallel()

	reqs := []pkgopensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: pkgopensearch.WriteOpCreate},
	}
	resp := &pkgopensearch.BulkResponse{
		Errors: true,
		Items: []map[string]pkgopensearch.BulkItemResult{
			bulkClassifierItem("create", "doc-1", 409, &pkgopensearch.ErrorDetail{
				Type:   "version_conflict_engine_exception",
				Reason: "document already exists",
			}),
		},
	}

	classification, err := ClassifyFailedBulkDocs(resp, reqs)

	require.NoError(t, err)
	assert.Empty(t, classification.RetryableFailures)
	assert.Empty(t, classification.DeadFailures)
	assert.Empty(t, classification.QuarantineFailures)
	assert.Equal(t, 1, classification.StatusCounts[409])
}

func TestClassifyFailedBulkDocsQuarantinesOnlyKnownRecordPayloadErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		errorType      pkgopensearch.ErrorType
		wantQuarantine bool
	}{
		{name: "mapper parsing", errorType: pkgopensearch.ErrorTypeMapperParsing, wantQuarantine: true},
		{name: "document parsing", errorType: pkgopensearch.ErrorTypeDocumentParsing, wantQuarantine: true},
		{name: "xcontent parsing", errorType: pkgopensearch.ErrorTypeXContentParse, wantQuarantine: true},
		{name: "parse", errorType: pkgopensearch.ErrorTypeParse, wantQuarantine: true},
		{name: "strict dynamic mapping", errorType: pkgopensearch.ErrorTypeStrictDynamicMapping, wantQuarantine: true},
		{name: "generic illegal argument stays dead", errorType: pkgopensearch.ErrorTypeIllegalArgument},
		{name: "unrelated terminal failure stays dead", errorType: "index_not_found_exception"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			classification, err := ClassifyFailedBulkDocs(
				&pkgopensearch.BulkResponse{
					Errors: true,
					Items: []map[string]pkgopensearch.BulkItemResult{
						bulkClassifierItem("index", "doc-1", 400, &pkgopensearch.ErrorDetail{
							Type:   tt.errorType,
							Reason: "terminal item failure",
						}),
					},
				},
				[]pkgopensearch.DocumentWriteRequest{{
					Index: "logs",
					ID:    "doc-1",
					Op:    pkgopensearch.WriteOpIndex,
				}},
			)

			require.NoError(t, err)
			assert.Empty(t, classification.RetryableFailures)
			if tt.wantQuarantine {
				assert.Empty(t, classification.DeadFailures)
				require.Len(t, classification.QuarantineFailures, 1)
				assert.Equal(t, tt.errorType, classification.QuarantineFailures[0].ErrorType)
				assert.Equal(t, 1, classification.QuarantineFailedCount)
				return
			}

			require.Len(t, classification.DeadFailures, 1)
			assert.Equal(t, tt.errorType, classification.DeadFailures[0].ErrorType)
			assert.Empty(t, classification.QuarantineFailures)
			assert.Equal(t, 1, classification.DeadFailedCount)
		})
	}
}

func bulkClassifierItem(
	action string,
	id string,
	status int,
	detail *pkgopensearch.ErrorDetail,
) map[string]pkgopensearch.BulkItemResult {
	result := ""
	if status >= 200 && status < 300 {
		if action == string(pkgopensearch.WriteOpIndex) && status == 200 {
			result = "updated"
		} else {
			result = "created"
		}
	}
	return map[string]pkgopensearch.BulkItemResult{
		action: {
			Index:  "logs",
			ID:     id,
			Status: status,
			Result: result,
			Error:  detail,
		},
	}
}

// requireBulkResponseValidationErrorは、応答の不正をerrors.Is・errors.Asで判定し、
// 不正の種類と、検出した位置・値を比較します。
func requireBulkResponseValidationError(t *testing.T, err error, want BulkResponseValidationError) {
	t.Helper()
	require.ErrorIs(t, err, ErrInvalidBulkResponse)
	require.ErrorIs(t, err, want.Kind)
	var invalid *BulkResponseValidationError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, want, *invalid)
}

// 既存のlog出力と同じ文言を保ちます。
func TestBulkResponseValidationErrorFormat(t *testing.T) {
	t.Parallel()

	operationErr := errors.New("unsupported operation")
	for _, tt := range []struct {
		err  BulkResponseValidationError
		want string
	}{
		{BulkResponseValidationError{Kind: ErrBulkResponseItemCountMismatch, Index: -1, GotItems: 1, WantItems: 2}, "bulk response item count mismatch: got=1 want=2"},
		{BulkResponseValidationError{Kind: ErrBulkRequestMissingIndex, Index: 1}, "reqs[1] missing index"},
		{BulkResponseValidationError{Kind: ErrBulkRequestMissingID, Index: 1}, "reqs[1] missing id"},
		{BulkResponseValidationError{Kind: ErrBulkRequestInvalidOperation, Index: 1, Err: operationErr}, "reqs[1] has invalid operation: unsupported operation"},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemActionCount, Index: 1}, "response items[1] is empty or contains multiple actions"},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemMissingID, Index: 1}, "response items[1] is missing _id"},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemMissingIndex, Index: 1}, "response items[1] is missing _index"},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemInvalidStatus, Index: 1, Status: 600}, "response items[1] has invalid HTTP status: 600"},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemSuccessWithError, Index: 1, Status: 200}, "response items[1] has success status 200 with error object"},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemIDMismatch, Index: 1, GotID: "doc-2", WantID: "doc-1"}, `response items[1] _id mismatch: got="doc-2" want="doc-1"`},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemActionMismatch, Index: 1, WantID: "doc-1", GotAction: "index", WantAction: "create"}, `response items[1] action mismatch for _id="doc-1": got="index" want="create"`},
		{BulkResponseValidationError{Kind: ErrBulkResponseItemInvalidSuccessResult, Index: 1, WantAction: "create", Status: 200, Result: "updated"}, `response items[1] has invalid success result for action "create": status=200 result="updated"`},
	} {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
	if err := (&BulkResponseValidationError{Kind: ErrBulkRequestInvalidOperation, Err: operationErr}); !errors.Is(err, operationErr) {
		t.Fatal("invalid operation error does not keep its cause")
	}
}
