package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/spool"
)

const testSpoolReplayMetric = "heterolog_spool_replay_records_total"

// spanCapturingSenderは、送信に渡されたcontextのspanを記録してから、内側のsenderへ渡します。
type spanCapturingSender struct {
	next Sender
	span pkgobserver.Span
}

func (s *spanCapturingSender) Send(ctx context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	s.span = observer.SpanFromContext(ctx)
	return s.next.Send(ctx, records)
}

// TestRunOnceSpoolFileProcessTracesReplayAndCountsOutcomesは、1回の送り直しをspanで囲んで
// そのcontextを送信へ渡し、送信のerrorをspanに記録し、送り直したrecordを結果ごとに数える
// ことを確認します。
func TestRunOnceSpoolFileProcessTracesReplayAndCountsOutcomes(t *testing.T) {
	t.Parallel()

	path := "processing/mixed"
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords: map[string][]spool.SpoolRecord{path: {
			testRecord("success"), testRecord("retryable"), testRecord("permanent"), testRecord("invalid"),
		}},
	}
	sendErr := errors.New("connection reset after partial response")
	sender := &spanCapturingSender{next: &senderStub{
		report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
			spool.RecordOutcomeStatusRetryable,
			spool.RecordOutcomeStatusPermanent,
			spool.RecordOutcomeStatusInvalid,
		),
		err: sendErr,
	}}
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()
	worker, err := New(store, &activeStoreStub{}, sender,
		WithTargetBackendService("loki"),
		WithObservers(tracer, meter),
	)
	require.NoError(t, err)

	_, err = worker.RunOnceSpoolFileProcess(context.Background())
	require.ErrorIs(t, err, sendErr)

	spans := tracer.Spans(SpanSpoolReplay)
	require.Len(t, spans, 1)
	span := spans[0]
	require.Same(t, span, sender.span)
	require.Nil(t, span.Parent)
	backend, _ := span.Attr("heterolog.backend")
	records, _ := span.Attr("heterolog.records")
	require.Equal(t, "loki", backend)
	require.Equal(t, 4, records)
	require.Equal(t, []error{sendErr}, span.Errors())
	require.Equal(t, 1, span.Ends())

	require.Equal(t, map[string]int64{
		observertest.Key(testSpoolReplayMetric, "backend", "loki", "outcome", "success"):    1,
		observertest.Key(testSpoolReplayMetric, "backend", "loki", "outcome", "retryable"):  1,
		observertest.Key(testSpoolReplayMetric, "backend", "loki", "outcome", "dead"):       1,
		observertest.Key(testSpoolReplayMetric, "backend", "loki", "outcome", "quarantine"): 1,
	}, meter.Counters())
}

// TestRunOnceSpoolFileProcessCountsInvalidReportAsQuarantineは、分類できない送信結果で
// file全体を隔離した場合、送り直したrecordをすべてquarantineとして数えることを確認します。
func TestRunOnceSpoolFileProcessCountsInvalidReportAsQuarantine(t *testing.T) {
	t.Parallel()

	path := "processing/incomplete-report"
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: {testRecord("first"), testRecord("second")}},
	}
	meter := observertest.NewMeter()
	worker := &Worker{
		Store: store,
		Sender: &senderStub{report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
		)},
		TargetBackendService: "opensearch",
		Meter:                meter,
	}

	_, err := worker.RunOnceSpoolFileProcess(context.Background())
	require.ErrorIs(t, err, spool.ErrInvalidExportReport)

	require.Equal(t, map[string]int64{
		observertest.Key(testSpoolReplayMetric, "backend", "opensearch", "outcome", "quarantine"): 2,
	}, meter.Counters())
}
