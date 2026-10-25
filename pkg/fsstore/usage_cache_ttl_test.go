package fsstore

import (
	"errors"
	"testing"
	"time"
)

func TestValidateUsageCacheTTLRejectsNonPositiveValue(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		t.Run(ttl.String(), func(t *testing.T) {
			if err := ValidateUsageCacheTTL(ttl); !errors.Is(err, ErrInvalidUsageCacheTTL) {
				t.Fatalf("ValidateUsageCacheTTL(%s) error = %v, want %v", ttl, err, ErrInvalidUsageCacheTTL)
			}
		})
	}
}

func TestValidateUsageCacheTTLAcceptsPositiveValue(t *testing.T) {
	for _, ttl := range []time.Duration{time.Nanosecond, time.Second} {
		t.Run(ttl.String(), func(t *testing.T) {
			if err := ValidateUsageCacheTTL(ttl); err != nil {
				t.Fatalf("ValidateUsageCacheTTL(%s) error = %v", ttl, err)
			}
		})
	}
}
