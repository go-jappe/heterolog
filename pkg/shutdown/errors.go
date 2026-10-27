// Package shutdown defines the result of an explicit forced shutdown.
package shutdown

import "errors"

// ErrForcedClose reports that the guarantee of completing accepted work has
// been abandoned. It does not mean that in-flight OS I/O or the release of
// resources has completed, or that the process has exited.
var ErrForcedClose = errors.New("shutdown forced before accepted work completed")
