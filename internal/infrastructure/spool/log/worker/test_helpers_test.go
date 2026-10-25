package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
)

type workerStoreStub struct {
	processingPaths  []string
	listCandidatesFn func(ctx context.Context, afterRelativePath string, limit int) ([]spool.FileCandidate, error)

	claimPath string
	claimOK   bool
	claimErr  error

	readRecords map[string][]spool.SpoolRecord
	readErr     map[string]error

	exists    map[string]bool
	existsErr error

	moveDoneErr       error
	moveDeadErr       error
	moveQuarantineErr error
	removeErr         error

	storeDoneErr       error
	storeDeadErr       error
	storeQuarantineErr error

	calls              []string
	listCandidateCalls []listCandidateCall
	storedDone         []storeRecordsCall
	storedDead         []storeRecordsCall
	storedQuarantine   []storeRecordsCall
	quarantineReasons  []spool.QuarantineReason
}

type listCandidateCall struct {
	after string
	limit int
}

type storeRecordsCall struct {
	path    string
	records []spool.SpoolRecord
}

var _ WorkFileStore = (*workerStoreStub)(nil)

func (s *workerStoreStub) ListProcessingFileCandidatesAfter(
	ctx context.Context,
	afterRelativePath string,
	limit int,
) ([]spool.FileCandidate, error) {
	s.calls = append(s.calls, "list_processing_candidates_after")
	s.listCandidateCalls = append(s.listCandidateCalls, listCandidateCall{after: afterRelativePath, limit: limit})
	if s.listCandidatesFn != nil {
		return s.listCandidatesFn(ctx, afterRelativePath, limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return firstProcessingCandidatesAfter(s.processingPaths, afterRelativePath, limit), nil
}

func (s *workerStoreStub) ClaimOldestActiveFile(ctx context.Context) (string, bool, error) {
	_ = ctx

	s.calls = append(s.calls, "claim_active")
	return s.claimPath, s.claimOK, s.claimErr
}

func (s *workerStoreStub) ReadProcessingRecords(
	ctx context.Context,
	processingPath string,
) ([]spool.SpoolRecord, error) {
	_ = ctx

	s.calls = append(s.calls, "read:"+processingPath)
	if s.readErr != nil && s.readErr[processingPath] != nil {
		return nil, s.readErr[processingPath]
	}

	return cloneSpoolRecords(s.readRecords[processingPath]), nil
}

func (s *workerStoreStub) MoveProcessingToDone(ctx context.Context, processingPath string) error {
	_ = ctx

	s.calls = append(s.calls, "move_done:"+processingPath)
	return s.moveDoneErr
}

func (s *workerStoreStub) MoveProcessingToDead(ctx context.Context, processingPath string) error {
	_ = ctx

	s.calls = append(s.calls, "move_dead:"+processingPath)
	return s.moveDeadErr
}

func (s *workerStoreStub) MoveProcessingToQuarantine(
	ctx context.Context,
	processingPath string,
	reason spool.QuarantineReason,
) error {
	_ = ctx

	s.calls = append(s.calls, "move_quarantine:"+processingPath)
	s.quarantineReasons = append(s.quarantineReasons, reason)
	return s.moveQuarantineErr
}

func (s *workerStoreStub) ProcessingFileExists(
	ctx context.Context,
	processingPath string,
) (bool, error) {
	_ = ctx

	s.calls = append(s.calls, "exists:"+processingPath)
	if s.existsErr != nil {
		return false, s.existsErr
	}
	if s.exists != nil {
		return s.exists[processingPath], nil
	}

	return true, nil
}

func (s *workerStoreStub) RemoveProcessingFile(ctx context.Context, processingPath string) error {
	_ = ctx

	s.calls = append(s.calls, "remove:"+processingPath)
	return s.removeErr
}

func (s *workerStoreStub) StoreProcessingRecordsToDone(
	ctx context.Context,
	processingPath string,
	records []spool.SpoolRecord,
) error {
	_ = ctx

	s.calls = append(s.calls, "store_done:"+processingPath)
	if s.storeDoneErr != nil {
		return s.storeDoneErr
	}
	s.storedDone = append(s.storedDone, storeRecordsCall{
		path:    processingPath,
		records: cloneSpoolRecords(records),
	})
	return nil
}

func (s *workerStoreStub) StoreProcessingRecordsToDead(
	ctx context.Context,
	processingPath string,
	records []spool.SpoolRecord,
) error {
	_ = ctx

	s.calls = append(s.calls, "store_dead:"+processingPath)
	if s.storeDeadErr != nil {
		return s.storeDeadErr
	}
	s.storedDead = append(s.storedDead, storeRecordsCall{
		path:    processingPath,
		records: cloneSpoolRecords(records),
	})
	return nil
}

func (s *workerStoreStub) StoreProcessingRecordsToQuarantine(
	ctx context.Context,
	processingPath string,
	records []spool.SpoolRecord,
) error {
	_ = ctx

	s.calls = append(s.calls, "store_quarantine:"+processingPath)
	if s.storeQuarantineErr != nil {
		return s.storeQuarantineErr
	}
	s.storedQuarantine = append(s.storedQuarantine, storeRecordsCall{
		path:    processingPath,
		records: cloneSpoolRecords(records),
	})
	return nil
}

type activeStoreStub struct {
	err     error
	calls   []string
	records [][]spool.SpoolRecord
}

var _ ActiveSpoolWriter = (*activeStoreStub)(nil)

func (s *activeStoreStub) StoreActive(ctx context.Context, records []spool.SpoolRecord) error {
	_ = ctx

	s.calls = append(s.calls, "store_active")
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, cloneSpoolRecords(records))
	return nil
}

type senderStub struct {
	report spool.ExportReport
	err    error

	calls   int
	records []spool.SpoolRecord
}

var _ Sender = (*senderStub)(nil)

func (s *senderStub) Send(
	ctx context.Context,
	records []spool.SpoolRecord,
) (spool.ExportReport, error) {
	_ = ctx

	s.calls++
	s.records = cloneSpoolRecords(records)
	return s.report, s.err
}

func mustExportReport(
	t testing.TB,
	outcomes ...spool.RecordOutcome,
) spool.ExportReport {
	t.Helper()

	report := make(spool.ExportReport, len(outcomes))
	for inputIndex, outcome := range outcomes {
		outcome.InputIndex = inputIndex
		report[inputIndex] = outcome
	}
	if err := report.Validate(len(outcomes)); err != nil {
		t.Fatalf("Validate export report returned error: %v", err)
	}
	return report
}

func mustExportReportForStatuses(
	t testing.TB,
	statuses ...spool.RecordOutcomeStatus,
) spool.ExportReport {
	t.Helper()

	attemptedAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	outcomes := make([]spool.RecordOutcome, len(statuses))
	for inputIndex, status := range statuses {
		outcomes[inputIndex].Status = status
		if status == spool.RecordOutcomeStatusSuccess ||
			status == spool.RecordOutcomeStatusPermanent {
			outcomes[inputIndex].AttemptedAt = attemptedAt
		}
		if status != spool.RecordOutcomeStatusSuccess {
			outcomes[inputIndex].Err = fmt.Errorf("test %s outcome", status)
		}
	}
	return mustExportReport(t, outcomes...)
}

func firstProcessingCandidatesAfter(paths []string, afterRelativePath string, limit int) []spool.FileCandidate {
	if limit <= 0 {
		return nil
	}
	candidates := make([]spool.FileCandidate, 0, len(paths))
	for _, path := range paths {
		relativePath := strings.TrimPrefix(path, "processing/")
		if relativePath > afterRelativePath {
			candidates = append(candidates, spool.FileCandidate{Path: path, RelativePath: relativePath})
		}
	}
	slices.SortFunc(candidates, func(a, b spool.FileCandidate) int {
		return strings.Compare(a.RelativePath, b.RelativePath)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func cloneSpoolRecords(records []spool.SpoolRecord) []spool.SpoolRecord {
	if len(records) == 0 {
		return nil
	}

	out := make([]spool.SpoolRecord, len(records))
	copy(out, records)
	return out
}

func testRecord(id string) spool.SpoolRecord {
	return spool.SpoolRecord{
		RequestID: id,
		Payload:   []byte(`{"id":"` + id + `"}`),
	}
}
