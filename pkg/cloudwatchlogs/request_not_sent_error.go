package cloudwatchlogs

import "fmt"

// RequestNotSentError means credential retrieval or request signing failed
// before the named HTTP operation was dispatched. Err retains the original
// failure. It does not describe an interrupted HTTP request.
type RequestNotSentError struct {
	Operation string
	Err       error
	// PreviousPutError is the confirmed ResourceNotFound refusal when automatic
	// stream creation succeeded but signing the second PutLogEvents failed.
	// It is nil if no earlier PutLogEvents was dispatched by this call.
	PreviousPutError error
}

func (e *RequestNotSentError) Error() string {
	message := fmt.Sprintf("cloudwatch logs %s was not sent: %v", e.Operation, e.Err)
	if e.PreviousPutError != nil {
		message += fmt.Sprintf(" (previous put: %v)", e.PreviousPutError)
	}
	return message
}

func (e *RequestNotSentError) Unwrap() []error {
	causes := make([]error, 0, 2)
	if e.PreviousPutError != nil {
		causes = append(causes, e.PreviousPutError)
	}
	if e.Err != nil {
		causes = append(causes, e.Err)
	}
	return causes
}
