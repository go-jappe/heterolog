package fsstore

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidUsageCacheTTLは、filesystem使用量の概算値を再観測する間隔が0以下であることを表します。
var ErrInvalidUsageCacheTTL = errors.New("filesystem usage cache TTL must be greater than zero")

// ValidateUsageCacheTTLは、filesystem使用量の概算値を再観測する間隔を検証します。
// 0以下ならErrInvalidUsageCacheTTLをwrapしたerrorを返します。
func ValidateUsageCacheTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("%w, got %s", ErrInvalidUsageCacheTTL, ttl)
	}
	return nil
}
