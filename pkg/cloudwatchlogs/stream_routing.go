package cloudwatchlogs

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidStreamRouting reports a StreamRouting value that is not supported.
// The error returned is an *InvalidStreamRoutingError.
var ErrInvalidStreamRouting = errors.New("invalid cloudwatch logs stream routing granularity")

// InvalidStreamRoutingError reports the unsupported StreamRouting value in its
// Granularity field. errors.Is matches ErrInvalidStreamRouting.
type InvalidStreamRoutingError struct {
	// Granularity is the value that was given.
	Granularity StreamRouting
}

func (e *InvalidStreamRoutingError) Error() string {
	return fmt.Sprintf(
		"%v %q: must be one of none, monthly, daily, hourly",
		ErrInvalidStreamRouting,
		e.Granularity,
	)
}

func (e *InvalidStreamRoutingError) Unwrap() error { return ErrInvalidStreamRouting }

// StreamRouting selects the time partitioning of CloudWatch Logs stream names.
// It is a routing policy of heterolog, not an option of the CloudWatch Logs API:
// a partitioned stream name is the configured base name followed by a time
// path.
type StreamRouting string

const (
	// StreamRoutingNone uses the configured base stream name as it is.
	StreamRoutingNone StreamRouting = "none"
	// StreamRoutingMonthly appends a YYYY/MM suffix to the base stream name.
	StreamRoutingMonthly StreamRouting = "monthly"
	// StreamRoutingDaily appends a YYYY/MM/DD suffix to the base stream name.
	StreamRoutingDaily StreamRouting = "daily"
	// StreamRoutingHourly appends a YYYY/MM/DD/HH suffix to the base stream name.
	StreamRoutingHourly StreamRouting = "hourly"
)

// Normalize returns r in its canonical form: in lower case, without surrounding
// spaces. The zero value and a blank value become StreamRoutingNone.
func (r StreamRouting) Normalize() StreamRouting {
	normalized := StreamRouting(strings.ToLower(strings.TrimSpace(string(r))))
	if normalized == "" {
		return StreamRoutingNone
	}
	return normalized
}

// Validate reports whether r is a supported value after Normalize; the zero
// value is valid and means StreamRoutingNone. It returns an
// *InvalidStreamRoutingError for any other value.
func (r StreamRouting) Validate() error {
	switch r.Normalize() {
	case StreamRoutingNone, StreamRoutingMonthly, StreamRoutingDaily, StreamRoutingHourly:
		return nil
	default:
		return &InvalidStreamRoutingError{Granularity: r}
	}
}
