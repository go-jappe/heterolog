package fsstore

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidUsageCacheTTL reports that the interval for measuring the estimated
// filesystem usage again is zero or less.
var ErrInvalidUsageCacheTTL = errors.New("filesystem usage cache TTL must be greater than zero")

// ValidateUsageCacheTTL validates the interval for measuring the estimated
// filesystem usage again. If the interval is zero or less, it returns an error
// that wraps ErrInvalidUsageCacheTTL.
func ValidateUsageCacheTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("%w, got %s", ErrInvalidUsageCacheTTL, ttl)
	}
	return nil
}
