package failurestore

import (
	"errors"
	"fmt"
)

var (
	// ErrDurableTypesRequired reports that no durable store is selected.
	ErrDurableTypesRequired = errors.New("durable types are required")
	// ErrDurableTypeNoneNotAlone reports that DurableTypeNone is not specified
	// alone, for example together with another durable type.
	ErrDurableTypeNoneNotAlone = errors.New(`durable type "none" must be specified alone`)
	// ErrUnsupportedDurableType reports that a durable type that the scope
	// does not support is selected.
	ErrUnsupportedDurableType = errors.New("unsupported durable type")
	// ErrDuplicateDurableType reports that the same durable type is selected
	// more than once.
	ErrDuplicateDurableType = errors.New("duplicate durable type")
)

// DurableTypeError reports that the durable store selection of Scope is
// invalid. Err is the kind of problem, which errors.Is can identify.
type DurableTypeError struct {
	// Scope is the part of the configuration that holds the durable store
	// selection, such as "delivery failure store".
	Scope string
	// Type is the invalid durable type. It is empty for
	// ErrDurableTypesRequired.
	Type DurableType
	// Err is one of ErrDurableTypesRequired, ErrDurableTypeNoneNotAlone,
	// ErrUnsupportedDurableType and ErrDuplicateDurableType.
	Err error
}

// Error returns a description that includes Scope and Type, according to the
// kind of problem.
func (e *DurableTypeError) Error() string {
	switch e.Err {
	case ErrDurableTypesRequired:
		return fmt.Sprintf("%s durable types are required", e.Scope)
	case ErrDurableTypeNoneNotAlone:
		return fmt.Sprintf("%s durable type %q must be specified alone", e.Scope, e.Type)
	case ErrUnsupportedDurableType:
		return fmt.Sprintf("unsupported %s durable type: %q", e.Scope, e.Type)
	case ErrDuplicateDurableType:
		return fmt.Sprintf("duplicate %s durable type: %q", e.Scope, e.Type)
	default:
		return fmt.Sprintf("invalid %s durable type %q: %v", e.Scope, e.Type, e.Err)
	}
}

// Unwrap returns Err, which represents the kind of problem.
func (e *DurableTypeError) Unwrap() error { return e.Err }
