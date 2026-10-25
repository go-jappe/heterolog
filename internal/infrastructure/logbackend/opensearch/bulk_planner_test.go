package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestPlanBulkRequestsRejectsOversizedRecordWithoutRejectingOtherRecords(t *testing.T) {
	t.Parallel()

	records := []BulkRecord{
		testBulkRecord(4, "doc-4", "accepted"),
		testBulkRecord(9, "doc-9", strings.Repeat("x", 128)),
		testBulkRecord(15, "doc-15", "also accepted"),
	}
	acceptedRequestSize, err := pkgopensearch.BulkRequestSize(
		[]pkgopensearch.DocumentWriteRequest{records[0].Document},
	)
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	oversizedRequestSize, err := pkgopensearch.BulkRequestSize(
		[]pkgopensearch.DocumentWriteRequest{records[1].Document},
	)
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	if acceptedRequestSize >= oversizedRequestSize {
		t.Fatalf("accepted request size = %d, oversized request size = %d", acceptedRequestSize, oversizedRequestSize)
	}

	plan, err := PlanBulkRequests(records, oversizedRequestSize-1)
	if err != nil {
		t.Fatalf("PlanBulkRequests() error = %v", err)
	}
	if len(plan.Rejected) != 1 {
		t.Fatalf("rejected count = %d, want 1", len(plan.Rejected))
	}
	if got := plan.Rejected[0].Record.InputIndex; got != 9 {
		t.Fatalf("rejected input index = %d, want 9", got)
	}
	if !errors.Is(plan.Rejected[0].Err, ErrRequestTooLarge) {
		t.Fatalf("rejected error = %v, want ErrRequestTooLarge", plan.Rejected[0].Err)
	}
	if len(plan.Requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(plan.Requests))
	}
	assertBulkRecordIndexes(t, plan.Requests[0].Records, []int{4, 15})
	assertPlannedBulkRequestSize(t, plan.Requests[0])
}

func TestPlanBulkRequestsRejectsSingleRecordWhoseMetadataMakesRequestTooLarge(t *testing.T) {
	t.Parallel()

	record := testBulkRecord(7, strings.Repeat("id", 128), "small")
	requestBytes, err := pkgopensearch.BulkRequestSize(
		[]pkgopensearch.DocumentWriteRequest{record.Document},
	)
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	plan, err := PlanBulkRequests([]BulkRecord{record}, requestBytes-1)
	if err != nil {
		t.Fatalf("PlanBulkRequests() error = %v", err)
	}
	if len(plan.Requests) != 0 {
		t.Fatalf("request count = %d, want 0", len(plan.Requests))
	}
	if len(plan.Rejected) != 1 || !errors.Is(plan.Rejected[0].Err, ErrRequestTooLarge) {
		t.Fatalf("plan = %+v, want one ErrRequestTooLarge rejection", plan)
	}
}

func TestPlanBulkRequestsSplitsAtExactNDJSONByteLimitAndPreservesInputIndexes(t *testing.T) {
	t.Parallel()

	records := []BulkRecord{
		testBulkRecord(20, "doc-20", strings.Repeat("a", 32)),
		testBulkRecord(3, "doc-3", strings.Repeat("b", 32)),
		testBulkRecord(11, "doc-11", strings.Repeat("c", 32)),
	}
	twoDocumentSize, err := pkgopensearch.BulkRequestSize([]pkgopensearch.DocumentWriteRequest{
		records[0].Document,
		records[1].Document,
	})
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	threeDocumentSize, err := pkgopensearch.BulkRequestSize([]pkgopensearch.DocumentWriteRequest{
		records[0].Document,
		records[1].Document,
		records[2].Document,
	})
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	if twoDocumentSize >= threeDocumentSize {
		t.Fatalf("two-document size = %d, three-document size = %d", twoDocumentSize, threeDocumentSize)
	}

	plan, err := PlanBulkRequests(records, twoDocumentSize)
	if err != nil {
		t.Fatalf("PlanBulkRequests() error = %v", err)
	}
	if len(plan.Rejected) != 0 {
		t.Fatalf("rejected count = %d, want 0", len(plan.Rejected))
	}
	if len(plan.Requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(plan.Requests))
	}
	assertBulkRecordIndexes(t, plan.Requests[0].Records, []int{20, 3})
	assertBulkRecordIndexes(t, plan.Requests[1].Records, []int{11})
	assertPlannedBulkRequestSize(t, plan.Requests[0])
	assertPlannedBulkRequestSize(t, plan.Requests[1])
	if plan.Requests[0].ByteSize != twoDocumentSize {
		t.Fatalf("first request size = %d, exact limit = %d", plan.Requests[0].ByteSize, twoDocumentSize)
	}
}

func TestDocumentBodySizeUsesEncodedUTF8Bytes(t *testing.T) {
	t.Parallel()

	record := testBulkRecord(0, "doc-0", "あ")
	size, err := DocumentBodySize(record.Document)
	if err != nil {
		t.Fatalf("DocumentBodySize() error = %v", err)
	}

	want := len([]byte(`{"message":"あ"}`))
	if size != want {
		t.Fatalf("DocumentBodySize() = %d, want UTF-8 byte size %d", size, want)
	}
}

func TestPlanBulkRequestsReturnsErrorWhenDocumentCannotBeEncodedAsJSON(t *testing.T) {
	t.Parallel()

	record := testBulkRecord(12, "doc-12", "unused")
	record.Document.Doc = map[string]any{"unsupported": make(chan struct{})}

	plan, err := PlanBulkRequests([]BulkRecord{record}, 1024)
	var measureErr *BulkRecordMeasureError
	if !errors.As(err, &measureErr) || measureErr.InputIndex != 12 {
		t.Fatalf("PlanBulkRequests() error = %#v, want BulkRecordMeasureError for input index 12", err)
	}
	var unsupported *json.UnsupportedTypeError
	if !errors.As(err, &unsupported) {
		t.Fatalf("PlanBulkRequests() error = %v, want the JSON encoding error as cause", err)
	}
	if len(plan.Requests) != 0 || len(plan.Rejected) != 0 {
		t.Fatalf("plan = %+v, want empty plan on contract error", plan)
	}
}

func TestValidateMaxBulkRequestBytesRejectsNonPositiveValues(t *testing.T) {
	t.Parallel()

	tests := []int{0, -1}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("value_%d", test), func(t *testing.T) {
			t.Parallel()
			if err := ValidateMaxBulkRequestBytes(test); !errors.Is(err, ErrInvalidMaxBulkRequestBytes) {
				t.Fatalf("ValidateMaxBulkRequestBytes() error = %v, want ErrInvalidMaxBulkRequestBytes", err)
			}
		})
	}
}

func TestPlannedBulkRequestDocumentsReturnsDocumentsInRecordOrder(t *testing.T) {
	t.Parallel()

	request := PlannedBulkRequest{Records: []BulkRecord{
		testBulkRecord(8, "doc-8", "first"),
		testBulkRecord(2, "doc-2", "second"),
	}}
	documents := request.Documents()
	if len(documents) != 2 || documents[0].ID != "doc-8" || documents[1].ID != "doc-2" {
		t.Fatalf("Documents() = %+v, want doc-8 then doc-2", documents)
	}

	documents[0].ID = "changed"
	if request.Records[0].Document.ID != "doc-8" {
		t.Fatalf("Documents() changed planned record ID to %q", request.Records[0].Document.ID)
	}
}

func testBulkRecord(inputIndex int, id, message string) BulkRecord {
	return BulkRecord{
		InputIndex: inputIndex,
		Document: pkgopensearch.DocumentWriteRequest{
			Index: "logs-2026",
			ID:    id,
			Doc:   map[string]any{"message": message},
			Op:    pkgopensearch.WriteOpIndex,
		},
	}
}

func assertBulkRecordIndexes(t *testing.T, records []BulkRecord, want []int) {
	t.Helper()
	if len(records) != len(want) {
		t.Fatalf("record count = %d, want %d", len(records), len(want))
	}
	for index := range want {
		if records[index].InputIndex != want[index] {
			t.Fatalf(
				"record[%d] input index = %d, want %d",
				index,
				records[index].InputIndex,
				want[index],
			)
		}
	}
}

func assertPlannedBulkRequestSize(t *testing.T, request PlannedBulkRequest) {
	t.Helper()
	size, err := pkgopensearch.BulkRequestSize(request.Documents())
	if err != nil {
		t.Fatalf("BulkRequestSize() error = %v", err)
	}
	if request.ByteSize != size {
		t.Fatalf("planned ByteSize = %d, canonical request size = %d", request.ByteSize, size)
	}
}
