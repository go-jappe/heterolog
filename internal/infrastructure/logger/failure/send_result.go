// Package failure defines the backend-neutral result passed from logger sends
// to durable failure handlers.
package failure

// SendResult contains terminal send evidence and the records that require
// durable handling. Each failed record is assigned exactly one handling
// disposition at the point where its backend result becomes known:
//
//   - RetryableRecords can be sent again without changing the stored record.
//   - DeadRecords are structurally trustworthy records with a confirmed
//     terminal delivery result.
//   - QuarantineRecords must not be replayed automatically without inspecting
//     or repairing the record, its backend representation, or its result.
//
// FailureCode and FailureReason preserve why the record received that
// disposition. Durable handlers consume the completed classification and do
// not reinterpret backend-specific failure codes.
type SendResult struct {
	Meta              Meta
	TerminalError     error
	RetryableRecords  []FailedRecord
	DeadRecords       []FailedRecord
	QuarantineRecords []FailedRecord
}

// HasFailedRecords reports whether there is payload data to handle durably.
func (r SendResult) HasFailedRecords() bool {
	return len(r.RetryableRecords) > 0 || len(r.DeadRecords) > 0 || len(r.QuarantineRecords) > 0
}

// HasSendFailure reports terminal send failure or failed-record evidence.
func (r SendResult) HasSendFailure() bool {
	return r.TerminalError != nil || r.HasFailedRecords()
}

// FailedCount returns the number of records requiring durable handling,
// including unconfirmed delivery and local rejection.
func (r SendResult) FailedCount() int {
	return len(r.RetryableRecords) + len(r.DeadRecords) + len(r.QuarantineRecords)
}

// SuccessCount returns confirmed successful inputs. A terminal send error means
// unlisted inputs cannot be considered successful.
func (r SendResult) SuccessCount() int {
	if r.TerminalError != nil || r.Meta.InputCount <= 0 {
		return 0
	}

	failed := r.FailedCount()
	if failed >= r.Meta.InputCount {
		return 0
	}
	return r.Meta.InputCount - failed
}
