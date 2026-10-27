package logger

import "errors"

// ErrLogDropped reports that, because the storage was overloaded, the drop
// overload policy was applied and the log was discarded without being stored.
// Even when a log is discarded, the drop count metric and a span event are
// recorded.
//
// The synchronous API returns an error that wraps this error when it drops a
// log, for example a log that failed to be sent to its destination and is
// dropped because the failure store is overloaded. Check it with
// errors.Is(err, ErrLogDropped). For such a log, the original cause at the
// destination can also be identified from the same error with errors.Is or
// errors.As. When degrade evacuates the log successfully, the log has not
// been discarded, so this error is not returned. The asynchronous API is best
// effort, so it does not return errors to the caller. With the Delivery API,
// check DeliveryReport.HasDrop instead of an error.
var ErrLogDropped = errors.New("log was dropped by overload policy")
