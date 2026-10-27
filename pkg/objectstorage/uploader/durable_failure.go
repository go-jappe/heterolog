package uploader

import "time"

// FailureCategory is the nature of a failure, determined after a single upload.
// The choice between fileArchive and spool is made by the separate durable type
// setting; this category is used only to decide whether spool resends the file
// and to group failures for diagnostics in the archive.
type FailureCategory string

const (
	// FailureCategoryRetryable is a failure for which the compressed log file is
	// kept and resent later.
	FailureCategoryRetryable FailureCategory = "retryable"
	// FailureCategoryPermanent is a failure confirmed not to be resolved by
	// automatically resending the same request. It is distinct from quarantine,
	// which indicates that the compressed log file itself is corrupted.
	FailureCategoryPermanent FailureCategory = "permanent"
)

// IsValid reports whether category is a defined upload failure category. It
// reports false for the empty string, which means no failure category.
func (category FailureCategory) IsValid() bool {
	switch category {
	case FailureCategoryRetryable, FailureCategoryPermanent:
		return true
	default:
		return false
	}
}

// DurableUploadFailureDecision describes the nature of a compressed log file's
// failure and when to resend it. Reason is a stable string for diagnostics,
// Code is the HTTP status if one was obtained, and RetryAfter is the wait
// before the next attempt, as returned by the service.
type DurableUploadFailureDecision struct {
	FailureCategory FailureCategory
	Reason          string
	Code            int
	RetryAfter      time.Duration
}
