package worker

import "errors"

type ProcessingPathBackoffOp string

const (
	ProcessingPathBackoffOpStoreRetryableRecordsToActive ProcessingPathBackoffOp = "store_retryable_records_to_active"

	ProcessingPathBackoffOpStoreProcessingRecordsToDead       ProcessingPathBackoffOp = "store_processing_records_to_dead"
	ProcessingPathBackoffOpStoreProcessingRecordsToQuarantine ProcessingPathBackoffOp = "store_processing_records_to_quarantine"
	ProcessingPathBackoffOpStoreProcessingRecordsToDone       ProcessingPathBackoffOp = "store_processing_records_to_done"

	ProcessingPathBackoffOpRemoveProcessingFileAfterRetryable ProcessingPathBackoffOp = "remove_processing_file_after_retryable"
	ProcessingPathBackoffOpRemoveProcessingFileAfterSplit     ProcessingPathBackoffOp = "remove_processing_file_after_split"

	ProcessingPathBackoffOpMoveInvalidProcessingToQuarantine ProcessingPathBackoffOp = "move_invalid_processing_to_quarantine"
	ProcessingPathBackoffOpReadProcessingRecords             ProcessingPathBackoffOp = "read_processing_records"
	ProcessingPathBackoffOpReconcileProcessingTerminal       ProcessingPathBackoffOp = "reconcile_processing_terminal"
)

var (
	ErrInvalidSpoolFile = errors.New("invalid spool file")

	ErrUnknownProcessingFileTransitionRetryOp = errors.New("unknown processing file transition retry operation")

	ErrProcessingFileTransitionRetryPathNotFound = errors.New("processing file transition retry path not found")
)
