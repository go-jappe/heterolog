package objectstoragespool

import (
	"context"
	"testing"
)

func BenchmarkStoreRepeatedEnsureLayout(b *testing.B) {
	store := newTestStore(b)
	if err := store.EnsureLayout(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.EnsureLayout(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreIdleRecoverAndClaim(b *testing.B) {
	store := newTestStore(b)
	if err := store.EnsureLayout(context.Background()); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if recovered, err := store.RecoverProcessing(ctx); err != nil || recovered != 0 {
			b.Fatalf("idle recovery = %d, error = %v", recovered, err)
		}
		if _, claimed, err := store.ClaimOldestActive(ctx); err != nil || claimed {
			b.Fatalf("idle claim = %t, error = %v", claimed, err)
		}
	}
}
