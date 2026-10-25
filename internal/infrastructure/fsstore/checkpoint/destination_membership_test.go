package checkpoint

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestSortedDestinationIDsReturnsIndependentOrderedCopy(t *testing.T) {
	t.Parallel()
	input := []objectstorage.DestinationID{"second", "first"}
	result := sortedDestinationIDs(input)
	require.Equal(t, []objectstorage.DestinationID{"first", "second"}, result)
	require.Equal(t, []objectstorage.DestinationID{"second", "first"}, input)
	result[0] = "changed"
	require.Equal(t, []objectstorage.DestinationID{"second", "first"}, input)
	require.Empty(t, sortedDestinationIDs(nil))
	require.Empty(t, sortedDestinationIDs([]objectstorage.DestinationID{}))
}

func TestValidateDestinationID(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		id    objectstorage.DestinationID
		valid bool
	}{
		{name: "empty"},
		{name: "whitespace", id: " \t\u3000"},
		{name: "nonempty", id: "primary", valid: true},
		{name: "no normalization", id: " primary ", valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateDestinationID(test.id)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidDestinationID)
			}
		})
	}
}

func TestValidateDestinationIDsPreservesOrderAndRejectsInvalidJSONValues(t *testing.T) {
	t.Parallel()
	input := []objectstorage.DestinationID{"second", "first"}
	require.NoError(t, validateDestinationIDs(input))
	require.Equal(t, []objectstorage.DestinationID{"second", "first"}, input)
	require.NoError(t, validateDestinationIDs(nil))
	require.NoError(t, validateDestinationIDs([]objectstorage.DestinationID{}))
	for _, invalid := range [][]objectstorage.DestinationID{
		{"first", ""}, {"first", " \t\u3000"}, {"first", "first"},
	} {
		original := slices.Clone(invalid)
		require.ErrorIs(t, validateDestinationIDs(invalid), ErrInvalidDestinationID)
		require.Equal(t, original, invalid)
	}
}

func TestCheckpointPendingDestinationsFollowCurrentActiveOrder(t *testing.T) {
	t.Parallel()
	checkpoint := Checkpoint{
		Key:                     CheckpointKey{Generation: "20260915-1", Compressed: true},
		CompletedDestinationIDs: []objectstorage.DestinationID{"uploaded", "inactive", "excluded"},
	}
	active := []objectstorage.DestinationID{"second", "uploaded", "excluded", "new", "first", "second", "first", "excluded"}
	original := cloneCheckpoint(checkpoint)
	originalActive := slices.Clone(active)
	pending := checkpoint.PendingDestinationIDs(active)
	assertDestinationIDs(t, pending, "second", "new", "first")
	require.Empty(t, checkpoint.PendingDestinationIDs(nil))
	require.Empty(t, checkpoint.PendingDestinationIDs([]objectstorage.DestinationID{}))
	pending[0] = "changed"
	require.Equal(t, original, checkpoint)
	require.Equal(t, originalActive, active)
}

func TestCheckpointCompleteUsesOnlyCurrentNonemptyActiveDestinations(t *testing.T) {
	t.Parallel()
	checkpoint := Checkpoint{
		CompletedDestinationIDs: []objectstorage.DestinationID{"primary", "inactive", "excluded"},
	}
	for _, test := range []struct {
		name   string
		active []objectstorage.DestinationID
		want   bool
	}{
		{name: "nil"},
		{name: "empty", active: []objectstorage.DestinationID{}},
		{name: "uploaded", active: []objectstorage.DestinationID{"primary"}, want: true},
		{name: "duplicate uploaded", active: []objectstorage.DestinationID{"primary", "primary"}, want: true},
		{name: "reactivated uploaded", active: []objectstorage.DestinationID{"inactive", "primary"}, want: true},
		{name: "new active", active: []objectstorage.DestinationID{"new"}},
		{name: "partially uploaded", active: []objectstorage.DestinationID{"primary", "secondary"}},
		{name: "all initially outside upload range", active: []objectstorage.DestinationID{"excluded"}, want: true},
		{name: "uploaded and excluded", active: []objectstorage.DestinationID{"excluded", "primary"}, want: true},
		{name: "pending and excluded", active: []objectstorage.DestinationID{"excluded", "secondary"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, checkpoint.Complete(test.active))
		})
	}
	require.True(t, checkpoint.DestinationComplete("primary"))
	require.True(t, checkpoint.DestinationComplete("inactive"))
	require.False(t, checkpoint.DestinationComplete("new"))
	require.True(t, checkpoint.DestinationComplete("excluded"))
	require.False(t, (Checkpoint{}).Complete([]objectstorage.DestinationID{"primary"}))
}
