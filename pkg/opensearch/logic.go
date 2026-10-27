package opensearch

import (
	"encoding/json"
	"fmt"
	"time"
)

type HTTPStatusError struct {
	Code   int
	Status string
	Body   []byte
	After  time.Duration
}

// ResponseDecodeError reports that reading or parsing the response body that
// OpenSearch returned with a success status, or checking that the response
// corresponds to the request, failed, so the result of the write could not be
// confirmed. Operation is "index" or "bulk", and Err holds the specific cause.
type ResponseDecodeError struct {
	Operation string
	Err       error
}

func (e *ResponseDecodeError) Error() string {
	return fmt.Sprintf("failed to decode opensearch %s response: %v", e.Operation, e.Err)
}

func (e *ResponseDecodeError) Unwrap() error {
	return e.Err
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("opensearch http status error: code=%d status=%s", e.Code, e.Status)
}

func (e *HTTPStatusError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.Code
}

func (e *HTTPStatusError) RetryAfter() time.Duration {
	if e == nil {
		return 0
	}
	return e.After
}

type ErrorCause struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type ErrorDetail struct {
	Type      ErrorType    `json:"type"`
	Reason    string       `json:"reason"`
	CausedBy  *ErrorDetail `json:"caused_by,omitempty"`
	RootCause []ErrorCause `json:"root_cause,omitempty"`
}

type ErrorResponse struct {
	Error  *ErrorDetail `json:"error"`
	Status int          `json:"status"`
}

func (e *HTTPStatusError) OpenSearchError() *ErrorDetail {
	var er ErrorResponse
	if err := json.Unmarshal(e.Body, &er); err != nil {
		return nil
	}
	return er.Error
}
