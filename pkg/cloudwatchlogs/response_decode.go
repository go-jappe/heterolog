package cloudwatchlogs

import (
	"errors"
	"fmt"
)

// ErrNilPutLogEventsOutput reports that there is no PutLogEventsOutput to
// establish the result of a submission. Validate returns it for a nil output,
// such as one from a Client that returned neither an output nor an error, and
// HTTPClient.PutLogEvents returns it inside a ResponseDecodeError when the
// response body is JSON null. It is not treated as evidence of acceptance.
var ErrNilPutLogEventsOutput = errors.New("cloudwatch logs client returned nil PutLogEventsOutput without error")

// ResponseDecodeError means a successful HTTP response could not establish the
// result of the request. Its cause is retained for diagnostics, but a truncated
// response must not be mistaken for a transport failure before acceptance.
type ResponseDecodeError struct {
	Operation string
	Err       error
}

func (e *ResponseDecodeError) Error() string {
	return fmt.Sprintf("failed to decode cloudwatch logs %s response: %v", e.Operation, e.Err)
}

func (e *ResponseDecodeError) Unwrap() error { return e.Err }

// Validate checks that rejection ranges can refer to the submitted events.
// The old/expired exclusive-end ranges may overlap each other, but must not
// overlap the too-new inclusive-start range. Validate before RejectedEvents;
// clamping invalid indexes is not evidence that an event was accepted.
func (o *PutLogEventsOutput) Validate(eventCount int) error {
	if o == nil {
		return ErrNilPutLogEventsOutput
	}
	if eventCount < 0 {
		return fmt.Errorf("cloudwatch logs event count must not be negative")
	}
	info := o.RejectedLogEventsInfo
	if info == nil {
		return nil
	}
	for _, index := range []struct {
		name  string
		value *int
	}{
		{"expiredLogEventEndIndex", info.ExpiredLogEventEndIndex},
		{"tooOldLogEventEndIndex", info.TooOldLogEventEndIndex},
		{"tooNewLogEventStartIndex", info.TooNewLogEventStartIndex},
	} {
		if index.value != nil && (*index.value < 0 || *index.value > eventCount) {
			return fmt.Errorf("cloudwatch logs response %s=%d is outside event range [0,%d]", index.name, *index.value, eventCount)
		}
	}
	if start := info.TooNewLogEventStartIndex; start != nil {
		if *start >= eventCount {
			return fmt.Errorf("cloudwatch logs response tooNewLogEventStartIndex=%d does not refer to an event", *start)
		}
		for _, end := range []*int{info.TooOldLogEventEndIndex, info.ExpiredLogEventEndIndex} {
			if end != nil && *end > *start {
				return fmt.Errorf("cloudwatch logs response rejects the same event as both too new and too old/expired")
			}
		}
	}
	return nil
}
