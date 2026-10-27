package durable

import (
	"slices"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// ValidateFailureHandlerMaxConcurrent validates the number of child operations
// that the failure handler can run concurrently. Replacing 0 or less with an
// implicit default would hide a missing setting, so it accepts only positive
// values. For a value of 0 or less, it returns an error that wraps
// ErrInvalidFailureHandlerMaxConcurrent.
func ValidateFailureHandlerMaxConcurrent(maxConcurrent int) error {
	if maxConcurrent <= 0 {
		return newValidationError(
			ErrInvalidFailureHandlerMaxConcurrent,
			"failure handler max concurrent must be positive: %d", maxConcurrent,
		)
	}
	return nil
}

// ValidateSpoolActiveBacklogWarnAfterDays validates the number of days after
// which the backlog warning of the active spool starts. It accepts 0 as an
// explicit value that disables the warning, and makes a negative number a
// configuration error. This value is not used to decide whether data may be
// saved to the spool. For a negative number, it returns an error that wraps
// ErrNegativeSpoolActiveBacklogWarnAfterDays.
func ValidateSpoolActiveBacklogWarnAfterDays(warnAfterDays int) error {
	if warnAfterDays < 0 {
		return newValidationError(
			ErrNegativeSpoolActiveBacklogWarnAfterDays,
			"spool active backlog warn-after days must not be negative: %d", warnAfterDays,
		)
	}
	return nil
}

// ValidateFailureStoreDurableTypes validates the selection of stores for
// regular failure handling. It accepts none, an explicit value that uses
// neither the spool nor the file archive, only when it is specified alone. As
// actual stores, fileArchive and spool can each be selected once. An invalid
// selection is a *DurableTypeError whose Scope is "failure store".
func ValidateFailureStoreDurableTypes(types []DurableType) error {
	return failurestore.ValidateDurableTypes(
		"failure store",
		types,
		DurableTypeFileArchive,
		DurableTypeSpool,
	)
}

// ValidateDeliveryFailureStoreDurableTypes validates the storage methods
// selected for the Delivery failure store. none means no store, and
// fileArchive is the only actual store that can be selected. An invalid
// selection is a *DurableTypeError whose Scope is "delivery failure store".
func ValidateDeliveryFailureStoreDurableTypes(types []DurableType) error {
	return failurestore.ValidateDurableTypes(
		"delivery failure store",
		types,
		DurableTypeFileArchive,
	)
}

// ValidateDirectDegradeSpillTypes validates the direct evacuation destinations
// used without a failure store. Only stdout, which is currently implemented,
// can be selected, and only once; an empty selection, reserved values, unknown
// values and duplicates are configuration errors. An invalid selection is a
// *SpillTypeError.
func ValidateDirectDegradeSpillTypes(types []SpillType) error {
	if len(types) == 0 {
		return &SpillTypeError{Err: ErrSpillTypesRequired}
	}
	// noneの併用は、個々の値の対応可否や重複より先に報告します。
	if len(types) > 1 && slices.Contains(types, SpillTypeNone) {
		return &SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone}
	}

	seen := make(map[SpillType]struct{}, len(types))
	for _, spillType := range types {
		if spillType == SpillTypeNone {
			return &SpillTypeError{Type: spillType, Err: ErrSpillTypeNoneWithDegrade}
		}
		if spillType != SpillType(SpillDestinationStdout) {
			return &SpillTypeError{Type: spillType, Err: ErrUnsupportedSpillType}
		}
		if _, duplicated := seen[spillType]; duplicated {
			return &SpillTypeError{Type: spillType, Err: ErrDuplicateSpillType}
		}
		seen[spillType] = struct{}{}
	}
	return nil
}
