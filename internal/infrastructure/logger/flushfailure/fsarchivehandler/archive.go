package fsarchivehandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
)

const (
	// archiveRecordFailureCategoryRetryableは、内容を変更せず再送できる失敗を表すfailure_categoryの値です。
	// RetryableRecordsに対応し、retryable系列のJSONLへ保存します。
	archiveRecordFailureCategoryRetryable = "retryable"
	// archiveRecordFailureCategoryDeadは、レコードの構造に問題はないものの、自動再送の対象外と確定した失敗を表します。
	// DeadRecordsに対応し、permanent系列のJSONLのfailure_categoryへ保存します。
	archiveRecordFailureCategoryDead = "dead"
	// archiveRecordFailureCategoryQuarantineは、レコード・backend向けの表現・送信結果の確認や修復が必要な失敗を表します。
	// QuarantineRecordsに対応し、deadと同じpermanent系列へ保存しますが、failure_categoryで区別します。
	archiveRecordFailureCategoryQuarantine = "quarantine"
)

// appendJSONLWriterFuncはJSONLの追記を行い、基底writerへ実際に書けたbyte数を返します。
// errorを返す場合もpartial write分を返す契約です。
type appendJSONLWriterFunc func(io.Writer, ...any) (uint64, error)

type countingArchiveWriter struct {
	writer  io.Writer
	written uint64
}

func (w *countingArchiveWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n < len(p) && err == nil {
		err = io.ErrShortWrite
	}
	if n > 0 {
		delta := uint64(n)
		if math.MaxUint64-w.written < delta {
			w.written = math.MaxUint64
		} else {
			w.written += delta
		}
	}
	return n, err
}

func writeArchiveJSONL(writer io.Writer, records ...any) (uint64, error) {
	counting := &countingArchiveWriter{writer: writer}
	enc := json.NewEncoder(counting)
	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			return counting.written, err
		}
	}
	return counting.written, nil
}

// archiveFailureRecordは、failure archiveのJSONLへ一行ずつ保存するbackend送信失敗レコードです。
// 全行がこのレコード形式のため、行の種類を表す識別項目は持ちません。
type archiveFailureRecord struct {
	FailureCategory string `json:"failure_category"`

	Backend  string `json:"backend"`
	FailedAt string `json:"failed_at,omitempty"`

	RequestID       string `json:"request_id,omitempty"`
	TraceID         string `json:"trace_id,omitempty"`
	AppName         string `json:"app_name,omitempty"`
	HostName        string `json:"host_name,omitempty"`
	Environment     string `json:"environment,omitempty"`
	RuntimeType     string `json:"runtime_type,omitempty"`
	ExecutionUnitID string `json:"execution_unit_id,omitempty"`
	Version         string `json:"version,omitempty"`
	InputCount      int    `json:"input_count,omitempty"`

	Operation     string                    `json:"operation,omitempty"`
	StatusCode    int                       `json:"status_code,omitempty"`
	FailureCode   loggerfailure.FailureCode `json:"failure_code,omitempty"`
	FailureReason string                    `json:"failure_reason,omitempty"`
	Payload       map[string]any            `json:"payload"`
}

// archiveLockedは、呼び出し時点のretryable/permanent active fileへfailure recordを
// 追記します。deadとquarantineはどちらも自動再送対象ではないため同じpermanent物理fileへ
// 保存しますが、JSON rowのfailure_categoryで区別できるようにします。
// 呼び出し元はFsstore.Muを取得していなければなりません。
func (f *FsArchiveHandler) archiveLocked(ctx context.Context, r loggerfailure.SendResult) error {
	bg := contextWithoutCancel(ctx)
	var errs []error

	if len(r.RetryableRecords) > 0 {
		if err := f.archiveRetryableRecordsLocked(bg, r); err != nil {
			errs = append(errs, err)
		}
	}

	if len(r.DeadRecords) > 0 || len(r.QuarantineRecords) > 0 {
		if err := f.archiveTerminalRecordsLocked(bg, r); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (f *FsArchiveHandler) archiveRetryableRecordsLocked(ctx context.Context, r loggerfailure.SendResult) error {
	_, span := f.Tracer.StartSpan(ctx, SpanRetryableArchiveWrite)
	defer span.End()

	records := make([]any, 0, len(r.RetryableRecords))
	for _, failed := range r.RetryableRecords {
		records = append(records, buildArchiveFailureRecord(archiveRecordFailureCategoryRetryable, r, failed))
	}

	if err := f.appendJSONLLocked(failurearchive.FailureCategoryRetryable, records...); err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (f *FsArchiveHandler) archiveTerminalRecordsLocked(ctx context.Context, r loggerfailure.SendResult) error {
	_, span := f.Tracer.StartSpan(ctx, SpanPermanentArchiveWrite)
	defer span.End()

	records := make([]any, 0, len(r.DeadRecords)+len(r.QuarantineRecords))
	for _, failed := range r.DeadRecords {
		records = append(records, buildArchiveFailureRecord(archiveRecordFailureCategoryDead, r, failed))
	}
	for _, failed := range r.QuarantineRecords {
		records = append(records, buildArchiveFailureRecord(archiveRecordFailureCategoryQuarantine, r, failed))
	}

	if err := f.appendJSONLLocked(failurearchive.FailureCategoryPermanent, records...); err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

// appendJSONLLockedは、呼び出し時点のactive file handleへrecordsを追記します。
// 呼び出し元はFsstore.Muを取得していなければなりません。
func (f *FsArchiveHandler) appendJSONLLocked(
	failureCategory failurearchive.FailureCategory,
	records ...any,
) error {
	var file *os.File
	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		file = f.Fsstore.FileRetryable
	case failurearchive.FailureCategoryPermanent:
		file = f.Fsstore.FilePermanent
	default:
		return fmt.Errorf("unsupported failure archive category: %q", failureCategory)
	}
	if file == nil {
		return fmt.Errorf("%s failure archive active file is nil", failureCategory)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect failure archive before append: %w", err)
	}

	writer := f.appendJSONLWriter
	if writer == nil {
		writer = writeArchiveJSONL
	}
	written, err := writer(file, records...)
	if tracker := f.directorySizeTracker(failureCategory); tracker != nil && written > 0 {
		tracker.RecordGrowth(written)
	}

	if err != nil {
		// The request failed as a whole. Restore its starting boundary, preserving
		// all earlier successful requests, including when an encoder failed after
		// writing complete records from this request.
		state := f.directoryForFailureCategory(failureCategory)
		state.appendRollback = &archiveAppendRollback{file: file, offset: info.Size()}
		return errors.Join(err, f.recoverArchiveAppendLocked(state))
	}
	return nil
}

func buildArchiveFailureRecord(
	failureCategory string,
	r loggerfailure.SendResult,
	failed loggerfailure.FailedRecord,
) archiveFailureRecord {
	record := archiveFailureRecord{
		FailureCategory: failureCategory,
		Backend:         r.Meta.Backend,
		RequestID:       r.Meta.RequestID,
		TraceID:         r.Meta.TraceID,
		AppName:         r.Meta.AppName,
		HostName:        r.Meta.HostName,
		Environment:     r.Meta.Environment,
		RuntimeType:     r.Meta.RuntimeType,
		ExecutionUnitID: r.Meta.ExecutionUnitID,
		Version:         r.Meta.Version,
		InputCount:      r.Meta.InputCount,
		Operation:       failed.Operation,
		StatusCode:      failed.StatusCode,
		FailureCode:     failed.FailureCode,
		FailureReason:   failed.FailureReason,
		Payload:         failed.Payload,
	}

	failedAt := failed.FailedAt
	if failedAt.IsZero() {
		failedAt = r.Meta.AttemptedAt
	}
	if !failedAt.IsZero() {
		record.FailedAt = failedAt.Format(timeLayout)
	}

	return record
}

const timeLayout = "2006-01-02T15:04:05.999999999Z07:00"
