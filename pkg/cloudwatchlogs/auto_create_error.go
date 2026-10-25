package cloudwatchlogs

import "fmt"

// AutoCreateLogStreamError means PutLogEvents was explicitly rejected because
// its destination was missing, and the subsequent CreateLogStream failed.
// No second PutLogEvents was started: uncertainty about stream creation does
// not imply uncertainty about acceptance of the log events.
// Both the original rejection and creation failure remain available to callers.
type AutoCreateLogStreamError struct {
	PutError    error
	CreateError error
}

func (e *AutoCreateLogStreamError) Error() string {
	return fmt.Sprintf("failed to create cloudwatch logs stream after put failure: %v (put: %v)", e.CreateError, e.PutError)
}

func (e *AutoCreateLogStreamError) Unwrap() []error {
	return []error{e.PutError, e.CreateError}
}
