package clock

import "time"

// Clock is a time source that returns the current time as a time.Time. The
// implementation of Clock decides the time zone of the returned value. The
// code that uses a Clock decides which implementation and default to use when
// that code is constructed, and uses the value that Now returns as is during
// processing.
type Clock interface {
	Now() time.Time
}

// UTCClock returns the current time in UTC.
type UTCClock struct{}

func NewUTCClock() Clock {
	return UTCClock{}
}

func (UTCClock) Now() time.Time {
	return time.Now().UTC()
}

// FixedClock is a Clock that keeps returning the time specified when it was
// constructed.
type FixedClock struct {
	at time.Time
}

func NewFixedClock(at time.Time) Clock {
	return FixedClock{at: at}
}

func (c FixedClock) Now() time.Time {
	return c.at
}
