package durable

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// DurableTypeError is an invalid durable store selection. It is the same type
// as failurestore.DurableTypeError, which ValidateFailureStoreDurableTypes and
// ValidateDeliveryFailureStoreDurableTypes return.
type DurableTypeError = failurestore.DurableTypeError

var (
	// ErrDurableTypesRequired is the same sentinel as
	// failurestore.ErrDurableTypesRequired.
	ErrDurableTypesRequired = failurestore.ErrDurableTypesRequired
	// ErrDurableTypeNoneNotAlone is the same sentinel as
	// failurestore.ErrDurableTypeNoneNotAlone.
	ErrDurableTypeNoneNotAlone = failurestore.ErrDurableTypeNoneNotAlone
	// ErrUnsupportedDurableType is the same sentinel as
	// failurestore.ErrUnsupportedDurableType.
	ErrUnsupportedDurableType = failurestore.ErrUnsupportedDurableType
	// ErrDuplicateDurableType is the same sentinel as
	// failurestore.ErrDuplicateDurableType.
	ErrDuplicateDurableType = failurestore.ErrDuplicateDurableType
)

var (
	// ErrInvalidFailureHandlerMaxConcurrent reports that the number of
	// concurrent executions of the failure handler is 0 or less.
	ErrInvalidFailureHandlerMaxConcurrent = errors.New("failure handler max concurrent must be positive")
	// ErrNegativeSpoolActiveBacklogWarnAfterDays reports that the number of
	// days after which the backlog warning of the active spool starts is
	// negative.
	ErrNegativeSpoolActiveBacklogWarnAfterDays = errors.New(
		"spool active backlog warn-after days must not be negative",
	)
)

var (
	// ErrSpillTypesRequired reports that no direct evacuation destination is
	// selected.
	ErrSpillTypesRequired = errors.New("direct degrade spill types are required")
	// ErrSpillTypeNoneNotAlone reports that SpillTypeNone was not specified
	// alone, for example together with another spill type.
	ErrSpillTypeNoneNotAlone = errors.New(`direct degrade spill type "none" must be specified alone`)
	// ErrSpillTypeNoneWithDegrade reports that SpillTypeNone was selected
	// although direct evacuation is enabled. SpillTypeNone can be specified
	// only when direct evacuation is disabled.
	ErrSpillTypeNoneWithDegrade = errors.New(
		`direct degrade spill type "none" requires degrade to be disabled`,
	)
	// ErrUnsupportedSpillType reports that a reserved value that is not
	// implemented, or an unknown value, was selected as a spill type.
	ErrUnsupportedSpillType = errors.New("unsupported direct degrade spill type")
	// ErrDuplicateSpillType reports that the same spill type was selected more
	// than once.
	ErrDuplicateSpillType = errors.New("duplicate direct degrade spill type")
)

// SpillTypeError reports that the selection of direct evacuation destinations
// is invalid. Err is the kind of problem, which errors.Is can identify.
type SpillTypeError struct {
	// Type is the invalid spill type. It is empty for ErrSpillTypesRequired.
	Type SpillType
	// Err is one of ErrSpillTypesRequired, ErrSpillTypeNoneNotAlone,
	// ErrSpillTypeNoneWithDegrade, ErrUnsupportedSpillType and
	// ErrDuplicateSpillType.
	Err error
}

// Error returns a description that depends on the kind of problem and
// includes Type.
func (e *SpillTypeError) Error() string {
	switch e.Err {
	case ErrSpillTypesRequired:
		return "direct degrade spill types are required"
	case ErrSpillTypeNoneNotAlone:
		return fmt.Sprintf("direct degrade spill type %q must be specified alone", e.Type)
	case ErrSpillTypeNoneWithDegrade:
		return fmt.Sprintf("direct degrade spill type %q requires degrade to be disabled", e.Type)
	case ErrUnsupportedSpillType:
		return fmt.Sprintf("unsupported direct degrade spill type: %q", e.Type)
	case ErrDuplicateSpillType:
		return fmt.Sprintf("duplicate direct degrade spill type: %q", e.Type)
	default:
		return fmt.Sprintf("invalid direct degrade spill type %q: %v", e.Type, e.Err)
	}
}

// Unwrap returns Err, which is the kind of problem.
func (e *SpillTypeError) Unwrap() error { return e.Err }

// validationErrorは、従来のmessageを保ったまま、errors.Isで種類を判別できる検証errorです。
type validationError struct {
	kind    error
	message string
}

func (e *validationError) Error() string { return e.message }

func (e *validationError) Unwrap() error { return e.kind }

func newValidationError(kind error, format string, args ...any) error {
	return &validationError{kind: kind, message: fmt.Sprintf(format, args...)}
}
