package uploader

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// DestinationUploadError reports that an upload to one destination failed.
// UploadReport.Err returns this error for each failed destination, joined with
// errors.Join. errors.As extracts the failed destination, and errors.Is and
// errors.As identify the cause.
type DestinationUploadError struct {
	// Destination is the ID of the destination that failed.
	Destination objectstorage.DestinationID
	// Err is the cause of the upload failure.
	Err error
}

// Error returns a description that includes the destination ID and the cause.
func (e *DestinationUploadError) Error() string {
	return fmt.Sprintf("object storage destination %q failed: %v", e.Destination, e.Err)
}

// Unwrap returns the cause of the upload failure.
func (e *DestinationUploadError) Unwrap() error { return e.Err }

var (
	// ErrUnrequestedDestinationResult reports that the results include one for a
	// destination that was not requested.
	ErrUnrequestedDestinationResult = errors.New("upload report contains an unrequested destination")
	// ErrDuplicateDestinationResult reports that there is more than one result for
	// the same destination.
	ErrDuplicateDestinationResult = errors.New("upload report classifies a destination more than once")
	// ErrMissingDestinationResult reports that there is no result for a requested
	// destination.
	ErrMissingDestinationResult = errors.New("upload report does not classify a requested destination")
)

// UploadReportError reports one case where the upload results do not correspond
// one-to-one with the requested destinations. errors.Is matches both
// ErrInvalidUploadReport and Err, which identifies the kind of problem.
type UploadReportError struct {
	// Destination is the destination ID of the result that does not correspond,
	// of the missing result, or that is requested more than once.
	Destination objectstorage.DestinationID
	// Index is the position in UploadReport.Results when Err is
	// ErrUnrequestedDestinationResult. It is 0 otherwise.
	Index int
	// Err is ErrDuplicateDestinationID, ErrUnrequestedDestinationResult,
	// ErrDuplicateDestinationResult or ErrMissingDestinationResult.
	Err error
}

// Error returns a description that includes the destination ID and, depending
// on the kind of problem, the position of the result.
func (e *UploadReportError) Error() string {
	switch e.Err {
	case ErrUnrequestedDestinationResult:
		return fmt.Sprintf(
			"%v: result[%d] contains unrequested destination %q",
			ErrInvalidUploadReport, e.Index, e.Destination,
		)
	case ErrDuplicateDestinationID:
		return fmt.Sprintf("%v: destination %q is requested more than once", ErrInvalidUploadReport, e.Destination)
	case ErrDuplicateDestinationResult:
		return fmt.Sprintf("%v: destination %q is classified more than once", ErrInvalidUploadReport, e.Destination)
	case ErrMissingDestinationResult:
		return fmt.Sprintf("%v: destination %q is not classified", ErrInvalidUploadReport, e.Destination)
	default:
		return fmt.Sprintf("%v: destination %q: %v", ErrInvalidUploadReport, e.Destination, e.Err)
	}
}

// Unwrap returns ErrInvalidUploadReport and Err, which identifies the kind of
// problem.
func (e *UploadReportError) Unwrap() []error { return []error{ErrInvalidUploadReport, e.Err} }
