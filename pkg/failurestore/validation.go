package failurestore

import (
	"slices"
)

// ValidateDurableTypes validates a selection among the durable stores that
// scope supports. none is accepted only when it is specified alone; otherwise
// each value in supported can be selected once. An invalid selection is
// reported as a *DurableTypeError.
func ValidateDurableTypes(
	scope string,
	types []DurableType,
	supported ...DurableType,
) error {
	if len(types) == 0 {
		return &DurableTypeError{Scope: scope, Err: ErrDurableTypesRequired}
	}
	seen := make(map[DurableType]struct{}, len(types))
	for _, durableType := range types {
		if durableType == DurableTypeNone {
			if len(types) != 1 {
				return &DurableTypeError{Scope: scope, Type: durableType, Err: ErrDurableTypeNoneNotAlone}
			}
		} else if !slices.Contains(supported, durableType) {
			return &DurableTypeError{Scope: scope, Type: durableType, Err: ErrUnsupportedDurableType}
		}
		if _, duplicated := seen[durableType]; duplicated {
			return &DurableTypeError{Scope: scope, Type: durableType, Err: ErrDuplicateDurableType}
		}
		seen[durableType] = struct{}{}
	}
	return nil
}
