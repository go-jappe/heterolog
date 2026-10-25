package clickhouse

import (
	"errors"
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

func TestPlanInsertRequestsUsesExactJSONEachRowBytesAndPreservesOrder(t *testing.T) {
	records := []pkgclickhouse.LogRecord{
		{Timestamp: time.Unix(1, 0).UTC(), Message: "first"},
		{Timestamp: time.Unix(2, 0).UTC(), Message: "second"},
		{Timestamp: time.Unix(3, 0).UTC(), Message: "third"},
	}
	firstTwoBody, err := pkgclickhouse.MarshalJSONEachRow(records[:2])
	if err != nil {
		t.Fatalf("marshal first two records: %v", err)
	}

	plan, err := PlanInsertRequests(records, len(firstTwoBody))
	if err != nil {
		t.Fatalf("PlanInsertRequests: %v", err)
	}
	if len(plan.Rejected) != 0 {
		t.Fatalf("Rejected = %#v, want none", plan.Rejected)
	}
	if len(plan.Requests) != 2 {
		t.Fatalf("Requests = %d, want 2", len(plan.Requests))
	}
	if got := plan.Requests[0].InputIndexes; len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("first request indexes = %v, want [0 1]", got)
	}
	if got := plan.Requests[1].InputIndexes; len(got) != 1 || got[0] != 2 {
		t.Fatalf("second request indexes = %v, want [2]", got)
	}
	for requestIndex, request := range plan.Requests {
		body, err := pkgclickhouse.MarshalJSONEachRow(request.Records)
		if err != nil {
			t.Fatalf("marshal request %d: %v", requestIndex, err)
		}
		if len(body) != request.UncompressedBytes {
			t.Fatalf(
				"request %d measured bytes = %d, actual = %d",
				requestIndex,
				request.UncompressedBytes,
				len(body),
			)
		}
		if request.UncompressedBytes > len(firstTwoBody) {
			t.Fatalf(
				"request %d bytes = %d, exceeds max %d",
				requestIndex,
				request.UncompressedBytes,
				len(firstTwoBody),
			)
		}
	}
}

func TestPlanInsertRequestsRejectsSingleOversizedRecordBeforePlanning(t *testing.T) {
	record := pkgclickhouse.LogRecord{
		Timestamp: time.Unix(1, 0).UTC(),
		Message:   "oversized",
	}
	body, err := pkgclickhouse.MarshalJSONEachRow([]pkgclickhouse.LogRecord{record})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}

	plan, err := PlanInsertRequests([]pkgclickhouse.LogRecord{record}, len(body)-1)
	if err != nil {
		t.Fatalf("PlanInsertRequests: %v", err)
	}
	if len(plan.Requests) != 0 {
		t.Fatalf("Requests = %#v, want none", plan.Requests)
	}
	if len(plan.Rejected) != 1 {
		t.Fatalf("Rejected = %d, want 1", len(plan.Rejected))
	}
	rejected := plan.Rejected[0]
	if rejected.InputIndex != 0 || rejected.UncompressedBytes != len(body) {
		t.Fatalf("Rejected = %+v, want input 0 and %d bytes", rejected, len(body))
	}
	if !errors.Is(rejected.Err, ErrLogRecordTooLarge) {
		t.Fatalf("Rejected error = %v, want ErrLogRecordTooLarge", rejected.Err)
	}
}

func TestPlanInsertRequestsFixesDefaultTimestampBeforeMeasuring(t *testing.T) {
	plan, err := PlanInsertRequests([]pkgclickhouse.LogRecord{{Message: "zero timestamp"}}, 1024)
	if err != nil {
		t.Fatalf("PlanInsertRequests: %v", err)
	}
	if len(plan.Requests) != 1 || len(plan.Requests[0].Records) != 1 {
		t.Fatalf("Requests = %#v, want one record", plan.Requests)
	}
	request := plan.Requests[0]
	if request.Records[0].Timestamp.IsZero() {
		t.Fatal("planned record timestamp is zero")
	}
	body, err := pkgclickhouse.MarshalJSONEachRow(request.Records)
	if err != nil {
		t.Fatalf("marshal planned request: %v", err)
	}
	if len(body) != request.UncompressedBytes {
		t.Fatalf(
			"measured bytes = %d, actual = %d",
			request.UncompressedBytes,
			len(body),
		)
	}
}

func TestPlanInsertRequestsSeparatesJSONEncodingFailure(t *testing.T) {
	records := []pkgclickhouse.LogRecord{{
		Timestamp: time.Unix(1, 0).UTC(),
		Fields:    map[string]any{"unsupported": make(chan int)},
	}}

	plan, err := PlanInsertRequests(records, 1024)
	if err != nil {
		t.Fatalf("PlanInsertRequests: %v", err)
	}
	if len(plan.Requests) != 0 || len(plan.Rejected) != 1 {
		t.Fatalf("plan = %+v, want one rejected record", plan)
	}
	if plan.Rejected[0].Err == nil {
		t.Fatal("encoding rejection error = nil")
	}
}

func TestPlanInsertRequestsRejectsNonPositiveHardLimit(t *testing.T) {
	for _, maxBytes := range []int{0, -1} {
		if _, err := PlanInsertRequests(nil, maxBytes); !errors.Is(err, ErrInvalidMaxInsertRequestBytes) {
			t.Fatalf("PlanInsertRequests(max=%d) error = %v", maxBytes, err)
		}
	}
}
