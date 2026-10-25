package processing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRemoveCompletedFileRequiresCompletionProof(t *testing.T) {
	for _, test := range []struct {
		name                                             string
		compressed, active, complete, unnamed, attempted bool
	}{
		{name: "uncompressed retained", active: true, complete: true},
		{name: "inactive retained", compressed: true, complete: true},
		{name: "incomplete retained", compressed: true, active: true},
		{name: "all active completed", compressed: true, active: true, complete: true, attempted: true},
		{name: "unnamed upload completed", compressed: true, complete: true, unnamed: true, attempted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := CompletionRequest{Path: "app_20261011_1.log.gz", Compressed: test.compressed, Complete: test.complete, UnnamedUploaderActive: test.unnamed}
			if test.active {
				request.DestinationIDs = []objectstorage.DestinationID{"primary"}
			}
			syncFailure := errors.New("directory sync failed after removal")
			calls := 0
			result, err := RemoveCompletedFile(context.Background(), request, func(path string) (fsstore.FileRemovalResult, error) {
				calls++
				return fsstore.FileRemovalResult{File: fsstore.FileInfo{Path: path}}, syncFailure
			})
			require.Equal(t, test.attempted, result.Attempted)
			if test.attempted {
				require.Equal(t, 1, calls)
				require.NoError(t, result.Removal.Err, "削除の成功と後続の同期失敗を分ける")
				require.ErrorIs(t, err, syncFailure)
			} else {
				require.Zero(t, calls)
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoveCompletedFileCancellationDoesNotStartRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := RemoveCompletedFile(ctx, CompletionRequest{
		Path: "app_20261011_1.log.gz", Compressed: true, Complete: true,
		DestinationIDs: []objectstorage.DestinationID{"primary"},
	}, func(string) (fsstore.FileRemovalResult, error) {
		t.Fatal("中止済みの処理から削除を開始しない")
		return fsstore.FileRemovalResult{}, nil
	})
	require.False(t, result.Attempted)
	require.ErrorIs(t, err, context.Canceled)
}
