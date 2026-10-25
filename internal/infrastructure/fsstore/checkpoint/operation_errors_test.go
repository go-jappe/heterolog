package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestIsPendingSaveOnly(t *testing.T) {
	pending := &PendingSaveError{Err: errors.New("disk unavailable")}
	conflict := &SavedTargetConflictError{
		Key: CheckpointKey{Generation: "20260920-1", Compressed: true}, DestinationID: "primary",
	}
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "success"},
		{name: "pending with storage cause", err: pending, want: true},
		{name: "wrapped pending", err: fmt.Errorf("save cursor: %w", pending), want: true},
		{name: "multiple pending saves", err: errors.Join(pending, fmt.Errorf("save progress: %w", pending)), want: true},
		{name: "upload failure", err: errors.New("upload unavailable")},
		{name: "joined upload failure", err: errors.Join(pending, errors.New("upload unavailable"))},
		{name: "nested cancellation", err: fmt.Errorf("continue: %w", errors.Join(pending, context.Canceled))},
		{name: "joined conflict", err: errors.Join(pending, conflict)},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsPendingSaveOnly(test.err))
		})
	}
}

func TestSavedTargetConflictDestination(t *testing.T) {
	key := CheckpointKey{Generation: "20260920-1", Compressed: true}
	conflict := &SavedTargetConflictError{Key: key, DestinationID: "primary"}
	var absent *SavedTargetConflictError
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "success"},
		{name: "nil conflict", err: absent},
		{name: "matching conflict", err: conflict, want: true},
		{name: "wrapped conflict", err: fmt.Errorf("advance completion cursor: %w", conflict), want: true},
		{name: "conflict and pending save", err: errors.Join(conflict, &PendingSaveError{Err: errors.New("disk unavailable")}), want: true},
		{name: "older generation", err: &SavedTargetConflictError{Key: CheckpointKey{Generation: "20260919-1", Compressed: true}, DestinationID: "primary"}},
		{name: "uncompressed file", err: &SavedTargetConflictError{Key: CheckpointKey{Generation: key.Generation}, DestinationID: "primary"}},
		{name: "unknown destination", err: &SavedTargetConflictError{Key: key, DestinationID: "unknown"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			destination, found := SavedTargetConflictDestination(test.err, key, []objectstorage.DestinationID{"primary", "secondary"})
			require.Equal(t, test.want, found)
			if test.want {
				require.Equal(t, objectstorage.DestinationID("primary"), destination)
			} else {
				require.Empty(t, destination)
			}
		})
	}
}
