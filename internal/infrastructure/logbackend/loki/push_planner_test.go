package loki

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestPlanPushRequestsRejectsOversizedLineWithoutRejectingOtherRecords(t *testing.T) {
	t.Parallel()

	records := []PushRecord{
		testPushRecord(0, "accepted"),
		testPushRecord(1, strings.Repeat("x", 17)),
		testPushRecord(2, "also accepted"),
	}
	plan, err := PlanPushRequests(records, PushLimits{
		MaxLineBytes:        16,
		MaxPushRequestBytes: 1024,
	})
	if err != nil {
		t.Fatalf("PlanPushRequests() error = %v", err)
	}

	if len(plan.Rejected) != 1 {
		t.Fatalf("rejected count = %d, want 1", len(plan.Rejected))
	}
	if got := plan.Rejected[0].Record.InputIndex; got != 1 {
		t.Fatalf("rejected input index = %d, want 1", got)
	}
	if !errors.Is(plan.Rejected[0].Err, ErrLogLineTooLarge) {
		t.Fatalf("rejected error = %v, want ErrLogLineTooLarge", plan.Rejected[0].Err)
	}
	if len(plan.Requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(plan.Requests))
	}
	assertPushRecordIndexes(t, plan.Requests[0].Records, []int{0, 2})
}

func TestPlanPushRequestsSplitsBeforeNetworkAtCanonicalJSONLimit(t *testing.T) {
	t.Parallel()

	records := []PushRecord{
		testPushRecord(0, strings.Repeat("a", 32)),
		testPushRecord(1, strings.Repeat("b", 32)),
		testPushRecord(2, strings.Repeat("c", 32)),
	}
	twoRecordSize, err := pkgloki.PushRequestSize(streamsFromPushRecords(records[:2]))
	if err != nil {
		t.Fatalf("PushRequestSize() error = %v", err)
	}
	threeRecordSize, err := pkgloki.PushRequestSize(streamsFromPushRecords(records))
	if err != nil {
		t.Fatalf("PushRequestSize() error = %v", err)
	}
	if twoRecordSize >= threeRecordSize {
		t.Fatalf("two-record size = %d, three-record size = %d", twoRecordSize, threeRecordSize)
	}

	plan, err := PlanPushRequests(records, PushLimits{
		MaxLineBytes:        64,
		MaxPushRequestBytes: twoRecordSize,
	})
	if err != nil {
		t.Fatalf("PlanPushRequests() error = %v", err)
	}
	if len(plan.Rejected) != 0 {
		t.Fatalf("rejected count = %d, want 0", len(plan.Rejected))
	}
	if len(plan.Requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(plan.Requests))
	}
	assertPushRecordIndexes(t, plan.Requests[0].Records, []int{0, 1})
	assertPushRecordIndexes(t, plan.Requests[1].Records, []int{2})
	for index, request := range plan.Requests {
		size, err := pkgloki.PushRequestSize(request.Streams)
		if err != nil {
			t.Fatalf("request[%d] size error = %v", index, err)
		}
		if size > twoRecordSize {
			t.Fatalf("request[%d] size = %d, limit = %d", index, size, twoRecordSize)
		}
	}
}

func TestPlanPushRequestsRejectsSingleRecordThatCannotFitRequest(t *testing.T) {
	t.Parallel()

	record := testPushRecord(7, "line")
	record.Labels["large"] = strings.Repeat("v", 256)
	size, err := pkgloki.PushRequestSize(streamsFromPushRecords([]PushRecord{record}))
	if err != nil {
		t.Fatalf("PushRequestSize() error = %v", err)
	}
	plan, err := PlanPushRequests([]PushRecord{record}, PushLimits{
		MaxLineBytes:        16,
		MaxPushRequestBytes: size - 1,
	})
	if err != nil {
		t.Fatalf("PlanPushRequests() error = %v", err)
	}
	if len(plan.Rejected) != 1 || !errors.Is(plan.Rejected[0].Err, ErrPushRecordTooLarge) {
		t.Fatalf("plan = %+v, want one ErrPushRecordTooLarge rejection", plan)
	}
}

func TestPlanPushRequestsMeasuresLineLimitInUTF8Bytes(t *testing.T) {
	t.Parallel()

	record := testPushRecord(0, "あい") // 2 rune、UTF-8では6 bytes。
	plan, err := PlanPushRequests([]PushRecord{record}, PushLimits{
		MaxLineBytes:        5,
		MaxPushRequestBytes: 1024,
	})
	if err != nil {
		t.Fatalf("PlanPushRequests() error = %v", err)
	}
	if len(plan.Requests) != 0 {
		t.Fatalf("request count = %d, want 0", len(plan.Requests))
	}
	if len(plan.Rejected) != 1 || !errors.Is(plan.Rejected[0].Err, ErrLogLineTooLarge) {
		t.Fatalf("plan = %+v, want one ErrLogLineTooLarge rejection", plan)
	}
}

func testPushRecord(index int, line string) PushRecord {
	return PushRecord{
		InputIndex: index,
		Labels: map[string]string{
			"appName":  "test",
			"env":      "test",
			"loglevel": "info",
		},
		Entry: pkgloki.Entry{
			Timestamp: time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC),
			Line:      line,
		},
	}
}

// assertPushStreamsは、streamの並び順に依存せず、label setごとにentryを送信順で比較します。
// 同じlabel setのentryは一つのstreamへまとめる必要があります。
func assertPushStreams(t *testing.T, name string, got, want []pkgloki.Stream) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s streams = %+v, want %+v", name, got, want)
	}
	matched := make([]bool, len(got))
	for _, wantStream := range want {
		found := false
		for index, gotStream := range got {
			if matched[index] || !reflect.DeepEqual(gotStream.Labels, wantStream.Labels) {
				continue
			}
			if !reflect.DeepEqual(gotStream.Entries, wantStream.Entries) {
				t.Fatalf("%s stream %v entries = %+v, want %+v", name, wantStream.Labels, gotStream.Entries, wantStream.Entries)
			}
			matched[index], found = true, true
			break
		}
		if !found {
			t.Fatalf("%s streams = %+v, want a stream with labels %v", name, got, wantStream.Labels)
		}
	}
}

func assertPushRecordIndexes(t *testing.T, records []PushRecord, want []int) {
	t.Helper()
	if len(records) != len(want) {
		t.Fatalf("record count = %d, want %d", len(records), len(want))
	}
	for index := range want {
		if records[index].InputIndex != want[index] {
			t.Fatalf("record[%d] input index = %d, want %d", index, records[index].InputIndex, want[index])
		}
	}
}

func TestSplitPushRequestKeepsRecordOrderAndRebuildsStreams(t *testing.T) {
	t.Parallel()

	errorRecord := testPushRecord(1, "error line")
	errorRecord.Labels = map[string]string{"appName": "test", "env": "test", "loglevel": "error"}
	records := []PushRecord{testPushRecord(0, "first"), errorRecord, testPushRecord(2, "third")}
	plan, err := PlanPushRequests(records, PushLimits{MaxLineBytes: 64, MaxPushRequestBytes: 4096})
	if err != nil || len(plan.Requests) != 1 {
		t.Fatalf("PlanPushRequests() = %+v, %v; want one request", plan, err)
	}

	first, second, ok := SplitPushRequest(plan.Requests[0])
	if !ok {
		t.Fatal("SplitPushRequest() ok = false for three records")
	}
	assertPushRecordIndexes(t, first.Records, []int{0})
	assertPushRecordIndexes(t, second.Records, []int{1, 2})
	// 分けたrequestのStreamsは、それぞれのRecordsだけを同じlabelごとにまとめ直します。
	infoLabels := map[string]string{"appName": "test", "env": "test", "loglevel": "info"}
	errorLabels := map[string]string{"appName": "test", "env": "test", "loglevel": "error"}
	assertPushStreams(t, "first half", first.Streams, []pkgloki.Stream{
		{Labels: infoLabels, Entries: []pkgloki.Entry{records[0].Entry}},
	})
	assertPushStreams(t, "second half", second.Streams, []pkgloki.Stream{
		{Labels: errorLabels, Entries: []pkgloki.Entry{records[1].Entry}},
		{Labels: infoLabels, Entries: []pkgloki.Entry{records[2].Entry}},
	})

	if _, _, ok := SplitPushRequest(first); ok {
		t.Fatal("SplitPushRequest() ok = true for a single record")
	}
}
