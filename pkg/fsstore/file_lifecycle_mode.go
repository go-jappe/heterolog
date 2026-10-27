package fsstore

import (
	"errors"
	"fmt"
)

var (
	// ErrUnsupportedFileLifecycleMode reports a lifecycle mode that
	// ValidateFileLifecycleMode does not accept.
	ErrUnsupportedFileLifecycleMode = errors.New("unsupported file lifecycle mode")
	// ErrUnsupportedDeliveryFileLifecycleMode reports a lifecycle mode that
	// ValidateDeliveryFileLifecycleMode does not accept.
	ErrUnsupportedDeliveryFileLifecycleMode = errors.New("unsupported delivery file lifecycle mode")
)

// ValidateFileLifecycleMode validates the lifecycle mode that the normal
// FileLogger and the failure archive use.
//
// An empty value is treated as inline mode, so that the normal FileLogger can
// be used with minimal configuration. For the Delivery FileLogger an empty
// value disables the lifecycle, so the default behavior of the same empty
// value depends on the Logger contract that uses it. Resolving aliases in
// environment variable values and applying defaults are the responsibility of
// the config loader.
func ValidateFileLifecycleMode(mode FileLifecycleMode) error {
	switch mode {
	case "", FileLifecycleModeDisable, FileLifecycleModeInline, FileLifecycleModeRunner:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFileLifecycleMode, mode)
	}
}

// ValidateDeliveryFileLifecycleMode validates a lifecycle mode against the
// modes that the Delivery FileLogger implements. The Delivery writer has no
// inline lifecycle, so only runner or disable is accepted. An empty value is
// allowed as the safe default behavior that does not start the lifecycle. For
// the normal FileLogger an empty value means inline, so an empty value does not
// mean the same thing for the two Loggers.
func ValidateDeliveryFileLifecycleMode(mode FileLifecycleMode) error {
	switch mode {
	case "", FileLifecycleModeDisable, FileLifecycleModeRunner:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedDeliveryFileLifecycleMode, mode)
	}
}
