package validation

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrUnsupportedUploadTarget reports that no provider supports the upload
	// target. An UnsupportedUploadTargetError matches this sentinel with
	// errors.Is.
	ErrUnsupportedUploadTarget = errors.New("unsupported object storage upload target")
	// ErrUploadTargetRequired reports that an upload target is empty or only
	// white space.
	ErrUploadTargetRequired = errors.New("object storage upload target is required")
	// ErrNonCanonicalUploadTarget reports that an upload target is not written
	// in lowercase without leading and trailing white space.
	ErrNonCanonicalUploadTarget = errors.New("object storage upload target must use canonical form")
	// ErrDuplicateUploadTarget reports that the same upload target is
	// specified more than once.
	ErrDuplicateUploadTarget = errors.New("duplicate object storage upload target")
	// ErrUploadTargetsRequired reports that an enabled upload destination
	// specifies no upload target. An empty target in the list is an
	// *UploadTargetError with ErrUploadTargetRequired.
	ErrUploadTargetsRequired = errors.New("object storage upload targets are required")
	// ErrInvalidS3Config reports that the S3 destination settings of an upload
	// destination that selects S3 do not meet the validation rules of the S3
	// uploader. The reason for the rejection can be identified with errors.Is
	// or errors.As as an error of the S3 uploader, such as ErrRegionRequired.
	ErrInvalidS3Config = errors.New("s3 config is invalid")
)

// UnsupportedUploadTargetError reports that the configuration at Scope
// specifies an upload target that no provider supports. With errors.Is, it
// matches ErrUnsupportedUploadTarget.
type UnsupportedUploadTargetError struct {
	// Scope is the location of the setting that specified the target, such as
	// "logger file default". When it is empty, Error does not show a scope.
	Scope string
	// Target is the upload target that no provider supports.
	Target objectstorage.UploadTarget
}

// Error returns a description that includes Scope and the unsupported target.
func (e *UnsupportedUploadTargetError) Error() string {
	if e.Scope == "" {
		return fmt.Sprintf("unsupported object storage upload target: %q", e.Target)
	}
	return fmt.Sprintf("%s unsupported object storage upload target: %q", e.Scope, e.Target)
}

// Is returns true when target is ErrUnsupportedUploadTarget.
func (e *UnsupportedUploadTargetError) Is(target error) bool {
	return target == ErrUnsupportedUploadTarget
}

// UploadTargetError reports that the Target at position Index in a list of
// upload targets violates the rule for its notation or for uniqueness. Err is
// the kind of rule, which errors.Is can identify.
type UploadTargetError struct {
	// Index is the position of the invalid target in the list.
	Index int
	// Target is the upload target as it was specified.
	Target objectstorage.UploadTarget
	// Canonical is, for ErrNonCanonicalUploadTarget, Target with leading and
	// trailing white space removed and converted to lowercase. Otherwise it is
	// empty.
	Canonical objectstorage.UploadTarget
	// Err is one of ErrUploadTargetRequired, ErrNonCanonicalUploadTarget and
	// ErrDuplicateUploadTarget.
	Err error
}

// Error returns a description that includes the position and the value of the
// target, according to the kind of rule.
func (e *UploadTargetError) Error() string {
	switch e.Err {
	case ErrUploadTargetRequired:
		return fmt.Sprintf("object storage upload target is required at index %d", e.Index)
	case ErrNonCanonicalUploadTarget:
		return fmt.Sprintf(
			"object storage upload target %q at index %d must use canonical form %q",
			e.Target,
			e.Index,
			e.Canonical,
		)
	case ErrDuplicateUploadTarget:
		return fmt.Sprintf("duplicate object storage upload target %q at index %d", e.Target, e.Index)
	default:
		return fmt.Sprintf("object storage upload target %q at index %d is invalid: %v", e.Target, e.Index, e.Err)
	}
}

// Unwrap returns Err, which represents the kind of rule.
func (e *UploadTargetError) Unwrap() error { return e.Err }
