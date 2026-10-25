package checkpointtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestWaitForContentsAndRestartLoadsCompletionsAndPendingUploadProgress(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	const sourceID = "test_source"
	original, err := checkpointstore.NewFilesystemStore(directory, sourceID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, original.Close()) })
	ctx := context.Background()
	key := checkpointstore.CheckpointKey{Generation: "20260915-10", Compressed: true}
	_, err = original.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	progress := checkpointstore.UploadProgress{
		Key:           checkpointstore.CheckpointKey{Generation: "20260915-11", Compressed: true},
		DestinationID: "secondary", UploadResult: checkpointstore.UploadResultRetryableFailure,
	}
	require.NoError(t, original.SaveUploadProgress(ctx, progress))
	path := filepath.Join(directory, sourceID+".log-file-uploads.json")
	persisted, err := os.ReadFile(path)
	require.NoError(t, err)
	// 数値の世代順で最後の完了位置を求め、未圧縮・圧縮を同じcursorとして比較します。
	expected := []checkpointstore.Checkpoint{
		{Key: key, CompletedDestinationIDs: []objectstorage.DestinationID{"primary"}},
		{Key: checkpointstore.CheckpointKey{Generation: "20260915-9"}, CompletedDestinationIDs: []objectstorage.DestinationID{"primary"}},
		{Key: checkpointstore.CheckpointKey{Generation: key.Generation}, CompletedDestinationIDs: []objectstorage.DestinationID{"primary"}},
	}
	restarted := WaitForContentsAndRestart(t, path, sourceID, original, expected)
	select {
	case <-original.Done():
	default:
		t.Fatal("original store must finish before restart")
	}
	for _, compressed := range []bool{false, true} {
		loaded, err := restarted.Load(ctx, checkpointstore.CheckpointKey{Generation: key.Generation, Compressed: compressed})
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{"primary"}, loaded.CompletedDestinationIDs)
	}
	loaded, err := restarted.Load(ctx, progress.Key)
	require.NoError(t, err)
	require.Empty(t, loaded.CompletedDestinationIDs)
	loadedProgress, found, err := restarted.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, progress, loadedProgress)
	afterRestart, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, persisted, afterRestart, "restart must validate the saved document without repairing it")
}

func TestWaitForContentsAndRestartWaitsForLatestBackgroundPersistence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		directory := t.TempDir()
		const sourceID = "test_source"
		original, err := checkpointstore.NewFilesystemStore(directory, sourceID)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, original.Close()) })
		path := filepath.Join(directory, sourceID+".log-file-uploads.json")
		restore := BlockPublication(t, path)
		for _, key := range []checkpointstore.CheckpointKey{
			{Generation: "20260915-9", Compressed: true},
			{Generation: "20260915-10"},
		} {
			_, err := original.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{"primary"})
			require.True(t, checkpointstore.IsPendingSaveOnly(err))
		}
		restore()
		expected := []checkpointstore.Checkpoint{{
			Key:                     checkpointstore.CheckpointKey{Generation: "20260915-10", Compressed: true},
			CompletedDestinationIDs: []objectstorage.DestinationID{"primary"},
		}}
		restarted := WaitForContentsAndRestart(t, path, sourceID, original, expected)
		loaded, err := restarted.Load(context.Background(), expected[0].Key)
		require.NoError(t, err)
		require.Equal(t, expected[0], loaded)
	})
}

func TestWaitForContentsAndRestartAcceptsNoCompletions(t *testing.T) {
	for _, test := range []struct {
		name         string
		destinations []objectstorage.DestinationID
	}{
		{name: "no destinations"},
		{name: "registered empty cursors", destinations: []objectstorage.DestinationID{"primary", "secondary"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			const sourceID = "test_source"
			original, err := checkpointstore.NewFilesystemStore(directory, sourceID)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, original.Close()) })
			require.NoError(t, original.RegisterDestinations(context.Background(), test.destinations, nil))
			path := filepath.Join(directory, sourceID+".log-file-uploads.json")
			restarted := WaitForContentsAndRestart(t, path, sourceID, original, nil)
			loaded, err := restarted.Load(context.Background(), checkpointstore.CheckpointKey{Generation: "20260915-1", Compressed: true})
			require.NoError(t, err)
			require.Empty(t, loaded.CompletedDestinationIDs)
		})
	}
}

func TestReloadMatchingContentsRejectsUnreloadableDocuments(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
	}{
		{"missing source", `{"cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}]}`},
		{"wrong source", `{"source":"other","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}]}`},
		{"duplicate destination", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}]}`},
		{"null extra generation", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":null}]}`},
		{"missing extra generation", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary"}]}`},
		{"invalid upload progress", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}],"upload_progress":[{}]}`},
		{"invalid upload result", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}],"upload_progress":[{"key":{"generation":"20260915-11","compressed":true},"destination_id":"secondary","upload_result":"unattempted"}]}`},
		{"already completed upload progress", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}],"upload_progress":[{"key":{"generation":"20260915-10","compressed":false},"destination_id":"primary","upload_result":"success"}]}`},
		{"unknown field", `{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"},{"destination_id":"secondary","generation":""}],"unexpected":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path, wanted := closedCheckpointFixture(t)
			// 停止済みStoreのファイルを変更し、Closeの保存で不正内容が直らない条件を作ります。
			require.NoError(t, os.WriteFile(path, []byte(test.payload), 0o600))
			restarted, err := reloadMatchingContents(path, "test_source", wanted)
			require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
			require.Nil(t, restarted)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, test.payload, string(after))
		})
	}
}

func TestReloadMatchingContentsRejectsMissingRegistrationOrCursor(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"registration file", checkpointstore.ErrRegisteredDestinationsMissing},
		{"registered cursor", checkpointstore.ErrRegisteredCheckpointMissing},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path, wanted := closedCheckpointFixture(t)
			if test.name == "registration file" {
				require.NoError(t, os.Remove(filepath.Join(filepath.Dir(path), "test_source.destinations.json")))
			} else {
				require.NoError(t, os.WriteFile(path, []byte(`{"source":"test_source","cursors":[{"destination_id":"primary","generation":"20260915-10"}]}`), 0o600))
			}
			restarted, err := reloadMatchingContents(path, "test_source", wanted)
			require.ErrorIs(t, err, test.err)
			require.Nil(t, restarted)
		})
	}
}

func TestReloadMatchingContentsRejectsUnexpectedCompletion(t *testing.T) {
	for _, test := range []struct {
		name   string
		wanted map[objectstorage.DestinationID]string
	}{
		{"older cursor", map[objectstorage.DestinationID]string{"primary": "20260915-9"}},
		{"newer cursor", map[objectstorage.DestinationID]string{"primary": "20260915-11"}},
		{"extra completion", map[objectstorage.DestinationID]string{"primary": "20260915-10", "secondary": "20260915-10"}},
		{"wrong destination", map[objectstorage.DestinationID]string{"secondary": "20260915-10"}},
		{"no completion", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path, _ := closedCheckpointFixture(t)
			restarted, err := reloadMatchingContents(path, "test_source", test.wanted)
			var mismatch *completionMismatchError
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, test.wanted, mismatch.wanted)
			require.Nil(t, restarted)
		})
	}
}

func TestReloadMatchingContentsDoesNotInitializeMissingDocument(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	restarted, err := reloadMatchingContents(filepath.Join(directory, "test_source.log-file-uploads.json"), "test_source", nil)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, restarted)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func closedCheckpointFixture(t *testing.T) (string, map[objectstorage.DestinationID]string) {
	t.Helper()
	directory := t.TempDir()
	original, err := checkpointstore.NewFilesystemStore(directory, "test_source")
	require.NoError(t, err)
	require.NoError(t, original.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"primary", "secondary"}, nil))
	_, err = original.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20260915-10", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	require.NoError(t, original.Close())
	return filepath.Join(directory, "test_source.log-file-uploads.json"), map[objectstorage.DestinationID]string{"primary": "20260915-10"}
}
