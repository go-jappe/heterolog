package fsstore

import (
	"errors"
	"fmt"
)

const (
	OverloadPolicyDrop     OverloadPolicy   = "drop"
	OverloadPolicyDegrade  OverloadPolicy   = "degrade"
	SpillDestinationStdout SpillDestination = "stdout"
)

var (
	// ErrUnsupportedOverloadPolicy reports that the policy for a lack of free
	// space when writing files is neither drop nor degrade. errors.Is identifies
	// it in the errors that ValidateFileOverloadConfig returns.
	ErrUnsupportedOverloadPolicy = errors.New("unsupported file overload policy")
	// ErrUnsupportedSpillDestination reports that the evacuation destination of
	// file writes that chose degrade is not stdout, the implemented destination.
	// errors.Is identifies it in the errors that ValidateFileOverloadConfig
	// returns.
	ErrUnsupportedSpillDestination = errors.New("unsupported file overload secondary destination")
)

// ValidateFileOverloadConfig validates how normal and async file writes handle
// a lack of free space. An unset policy means drop. For degrade, only stdout is
// supported as the evacuation destination, and for drop the evacuation
// destination is not used. An invalid policy is an error that errors.Is matches
// with ErrUnsupportedOverloadPolicy, and an invalid evacuation destination for
// degrade is an error that errors.Is matches with
// ErrUnsupportedSpillDestination.
func ValidateFileOverloadConfig(policy OverloadPolicy, spill SpillDestination) error {
	switch policy {
	case "", OverloadPolicyDrop:
		return nil
	case OverloadPolicyDegrade:
		if spill != SpillDestinationStdout {
			return &overloadConfigError{
				kind:    ErrUnsupportedSpillDestination,
				message: fmt.Sprintf("file overload secondary destination %q is unsupported; degrade requires stdout", spill),
			}
		}
		return nil
	default:
		return &overloadConfigError{
			kind:    ErrUnsupportedOverloadPolicy,
			message: fmt.Sprintf("file overload policy %q is invalid; use drop or degrade", policy),
		}
	}
}

// overloadConfigErrorは、従来の表示文を保ったまま、不正な設定の種類kindをerrors.Isで
// 判別できるようにするerrorです。
type overloadConfigError struct {
	kind    error
	message string
}

func (e *overloadConfigError) Error() string { return e.message }

func (e *overloadConfigError) Unwrap() error { return e.kind }
