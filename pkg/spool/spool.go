// Package spool defines the records that the spool keeps for log entries whose
// delivery failed, and the per-record results that the replay workers collect
// when they resend a batch of those records to the backend.
package spool

import (
	"encoding/json"
	"errors"
	"time"
)

type State string

const (
	StateActive     State = "active"
	StateProcessing State = "processing"
	StateDone       State = "done"
	// StateDead stores records whose representation can be trusted and whose
	// delivery result is confirmed terminal. Investigation starts with the
	// backend, credentials, destination configuration, or time policy rather
	// than with spool data integrity.
	StateDead State = "dead"
	// StateQuarantine isolates records or files that cannot be trusted for
	// unchanged automatic replay. They require data inspection, repair,
	// conversion, or a manual decision before being reintroduced.
	StateQuarantine State = "quarantine"
)

func (s State) IsValid() bool {
	switch s {
	case StateActive, StateProcessing, StateDone, StateDead, StateQuarantine:
		return true
	default:
		return false
	}
}

// QuarantineReason is the reason why a whole processing file was quarantined
// without splitting it into records. The value is also saved in the name of
// the quarantined file, so operators can identify the point of failure to
// investigate first even when the original file cannot be decoded.
type QuarantineReason string

const (
	// QuarantineReasonInvalidSpoolFile means that the spool file itself could
	// not be restored as records, for example because the NDJSON is corrupt,
	// the file is empty, or a required field is missing.
	QuarantineReasonInvalidSpoolFile QuarantineReason = "invalid-spool-file"
	// QuarantineReasonInvalidExportReport means that the per-record results
	// after sending to the backend had omissions, duplicates or similar
	// problems, so the original file could not be distributed safely.
	QuarantineReasonInvalidExportReport QuarantineReason = "invalid-export-report"
	// QuarantineReasonInvalidTransitionState means that the retry information
	// for the state transition after sending was corrupt, so the position to
	// resume from could not be determined safely.
	QuarantineReasonInvalidTransitionState QuarantineReason = "invalid-transition-state"
)

func (r QuarantineReason) IsValid() bool {
	switch r {
	case QuarantineReasonInvalidSpoolFile,
		QuarantineReasonInvalidExportReport,
		QuarantineReasonInvalidTransitionState:
		return true
	default:
		return false
	}
}

// invalidSpoolFileDetail は、ErrInvalidSpoolFile の下位分類として扱う error。
// これにより、例えば ErrMalformedSpoolJSON で wrap された error でも、
//
//	errors.Is(err, ErrInvalidSpoolFile)
//
// が true になる。
type invalidSpoolFileDetail string

func (e invalidSpoolFileDetail) Error() string {
	return string(e)
}

func (e invalidSpoolFileDetail) Is(target error) bool {
	return target == ErrInvalidSpoolFile
}

// spoolOperationDetail は、ErrSpoolOperation の下位分類として扱う error。
// これにより、例えば ErrReadProcessingSpoolFile で wrap された error でも、
//
//	errors.Is(err, ErrSpoolOperation)
//
// が true になる。
type spoolOperationDetail string

func (e spoolOperationDetail) Error() string {
	return string(e)
}

func (e spoolOperationDetail) Is(target error) bool {
	return target == ErrSpoolOperation
}

var (
	// ErrInvalidSpoolFile reports that the spool file itself is in a format
	// that cannot be processed.
	//
	// When a replay worker detects this error, it may move that processing
	// file to quarantine.
	//
	// Examples:
	// - the file is corrupt as NDJSON
	// - a line cannot be decoded as JSON
	// - the file cannot be restored as a SpoolRecord
	// - a required item is missing
	ErrInvalidSpoolFile = errors.New("invalid spool file")

	// ErrEmptySpoolFile reports that the spool file is empty.
	//
	// An empty file is not a regular target for a resend, so as a rule it is
	// quarantined in the same way as ErrInvalidSpoolFile.
	ErrEmptySpoolFile error = invalidSpoolFileDetail("empty spool file")

	// ErrMalformedSpoolJSON reports that the JSON or NDJSON in the spool file is
	// corrupt.
	//
	// Examples:
	// - a line cannot be decoded as JSON
	// - a line in the middle of the NDJSON is corrupt
	ErrMalformedSpoolJSON error = invalidSpoolFileDetail("malformed spool json")

	// ErrInvalidSpoolRecord reports that a record could be read as JSON but is
	// invalid as a SpoolRecord.
	//
	// Examples:
	// - OccurredAt is zero
	// - Payload is empty
	// - Payload is invalid as JSON
	ErrInvalidSpoolRecord error = invalidSpoolFileDetail("invalid spool record")

	// ErrInvalidSpoolState reports an unknown or invalid spool state.
	//
	// Examples:
	// - a state other than "active", "processing", "done", "dead" and "quarantine"
	ErrInvalidSpoolState = errors.New("invalid spool state")

	// ErrSpoolOperation is the parent error of failed operations on the spool.
	//
	// It classifies errors in which an operation failed, such as I/O, path
	// validation, state moves, split writes and decoding. A decode failure
	// caused by an invalid spool file also matches ErrInvalidSpoolFile.
	ErrSpoolOperation = errors.New("spool operation failed")

	// ErrListProcessingSpoolFiles reports a failure to list the spool files
	// under processing.
	ErrListProcessingSpoolFiles error = spoolOperationDetail("list processing spool files")

	// ErrValidateProcessingSpoolFilePath reports a failure to validate a
	// processing spool file path.
	ErrValidateProcessingSpoolFilePath error = spoolOperationDetail("validate processing spool file path")

	// ErrReadProcessingSpoolFile reports a failure to read a processing spool
	// file.
	//
	// It classifies an I/O error and does not mean that the spool file has an
	// invalid format.
	ErrReadProcessingSpoolFile error = spoolOperationDetail("read processing spool file")

	// ErrDecodeProcessingSpoolFile reports a failure to decode a processing
	// spool file.
	//
	// When its underlying errors include an error of the ErrInvalidSpoolFile
	// kind, the replay worker may move that file to quarantine.
	ErrDecodeProcessingSpoolFile error = spoolOperationDetail("decode processing spool file")

	// ErrMoveProcessingSpoolFileToDone reports a failure to move a processing
	// spool file to done.
	ErrMoveProcessingSpoolFileToDone error = spoolOperationDetail("move processing spool file to done")

	// ErrMoveProcessingSpoolFileToDead reports a failure to move a processing
	// spool file to dead.
	ErrMoveProcessingSpoolFileToDead error = spoolOperationDetail("move processing spool file to dead")

	// ErrMoveProcessingSpoolFileToQuarantine reports a failure to move a
	// processing spool file to quarantine.
	ErrMoveProcessingSpoolFileToQuarantine error = spoolOperationDetail("move processing spool file to quarantine")

	// ErrRemoveProcessingSpoolFile reports a failure to remove a processing
	// spool file.
	ErrRemoveProcessingSpoolFile error = spoolOperationDetail("remove processing spool file")

	// ErrStoreProcessingRecordsToDone reports a failure to re-save some records
	// of a processing file to done.
	ErrStoreProcessingRecordsToDone error = spoolOperationDetail("store processing records to done")

	// ErrStoreProcessingRecordsToDead reports a failure to re-save some records
	// of a processing file to dead.
	ErrStoreProcessingRecordsToDead error = spoolOperationDetail("store processing records to dead")

	// ErrStoreProcessingRecordsToQuarantine reports a failure to re-save some
	// records of a processing file to quarantine.
	ErrStoreProcessingRecordsToQuarantine error = spoolOperationDetail("store processing records to quarantine")

	// ErrInvalidProcessingSplitDestinationState reports a state that is
	// invalid as the destination for saving a split of a processing file.
	//
	// Some states are valid as a State but are not allowed as the destination
	// of a processing split, so this error is separate from
	// ErrInvalidSpoolState.
	ErrInvalidProcessingSplitDestinationState error = spoolOperationDetail("invalid processing split destination state")

	// ErrResolveDestinationSpoolStateDir reports a failure to resolve the state
	// dir of a split destination.
	ErrResolveDestinationSpoolStateDir error = spoolOperationDetail("resolve destination spool state dir")

	// ErrBuildSpoolNDJSON reports that an NDJSON body could not be built from
	// SpoolRecord values.
	ErrBuildSpoolNDJSON error = spoolOperationDetail("build spool ndjson")

	// ErrWriteSplitSpoolFile reports a failure to write a spool file after a
	// split.
	ErrWriteSplitSpoolFile error = spoolOperationDetail("write split spool file")
)

// SpoolRecord is one record saved in the spool. It is the common storage
// format for records waiting for a resend, records set aside temporarily,
// records set aside in dead or quarantine, and so on.
type SpoolRecord struct {
	OccurredAt  time.Time // time the delivery failed, or was attempted when the failure time is unknown
	FinalizedAt time.Time // time the record was moved to done, dead or quarantine

	FirstResendAttemptedAt time.Time
	LastResendAttemptedAt  time.Time
	ResendAttemptCount     int

	// NotBefore is the earliest time at which this record may be resent to the
	// backend. When the backend specifies a wait with Retry-After, the replay
	// worker saves it in the records that it returns to waiting for a resend. The
	// zero value means no restriction, and existing spool files without this
	// field are also resent as before. A value whose deadline has passed does
	// not restrict resends, so it is not changed when the record is saved to
	// done, dead or quarantine either. A value further away than the maximum
	// resend wait from the time it is loaded is shortened to that maximum from
	// that time.
	NotBefore time.Time `json:",omitzero"`

	RequestID string          // identifier for correlation and tracing; not needed for the resend itself
	Payload   json.RawMessage // body used for the resend to the backend

	// InitialFailureOperation through InitialFailureReason hold the failure
	// with which the record was first saved to the spool. LastFailureReason is
	// updated to the latest failure reason obtained from a resend. All of them
	// are for investigation and are not used to build the resend payload.
	InitialFailureOperation  string `json:",omitempty"`
	InitialFailureStatusCode int    `json:",omitempty"`
	InitialFailureCode       string `json:",omitempty"`
	InitialFailureReason     string `json:",omitempty"`
	LastFailureReason        string `json:",omitempty"`
}
