package logger

import (
	"errors"
	"fmt"
)

// ErrInvalidNotificationTarget reports that a backend selected as an internal
// log notification target (OnErrorConfig, OnWarnConfig, OnInfoConfig or
// OnDebugConfig) has settings from which its notification writer cannot be
// created, for example an unknown TimeZone or an empty LogLineDateFormat. For a
// target other than the file logger, the constructors return it as a
// *NotificationTargetError before creating any resource, instead of dropping
// notifications later.
var ErrInvalidNotificationTarget = errors.New("invalid internal log notification target")

// NotificationTargetError reports the logger (Source) and the notification target
// (Backend) whose settings are invalid, and the cause in Err. errors.Is matches
// ErrInvalidNotificationTarget and the cause.
type NotificationTargetError struct {
	Source  string
	Backend LogOutputDestination
	Err     error
}

func (e *NotificationTargetError) Error() string {
	return fmt.Sprintf("%s internal log notification target %s: %v", e.Source, e.Backend, e.Err)
}

// Unwrap returns ErrInvalidNotificationTarget and the cause.
func (e *NotificationTargetError) Unwrap() []error {
	return []error{ErrInvalidNotificationTarget, e.Err}
}
