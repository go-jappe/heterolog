// Package checkpointtest provides filesystem fixtures for checkpoint integration tests.
package checkpointtest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

// BlockPublicationはcheckpointのパスを空でないディレクトリにして、保存を失敗させます。
// Storeのメモリは変更しません。返した関数で障害を解除でき、テスト終了時にも解除します。
func BlockPublication(t *testing.T, checkpointPath string) func() {
	t.Helper()
	if _, err := os.Stat(checkpointPath); err == nil {
		require.NoError(t, os.Rename(checkpointPath, checkpointPath+".previous"))
	} else {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	require.NoError(t, os.Mkdir(checkpointPath, 0o700))
	marker := filepath.Join(checkpointPath, "prevent-replacement")
	require.NoError(t, os.WriteFile(marker, []byte("preserve"), 0o600))
	restore := sync.OnceFunc(func() {
		require.NoError(t, os.Remove(marker))
		require.NoError(t, os.Remove(checkpointPath))
	})
	t.Cleanup(restore)
	return restore
}

// WaitForContentsAndRestartは、期待する完了位置の保存を待ち、元のStoreを閉じて再起動します。
// expectedの各keyは、その宛先の最後の完了位置を表します。圧縮前後は区別しません。
// nilなら進行済みのcursorがないことを要求します。登録だけ済んだ空cursorは比較から除きます。
// 呼び出し元はStoreへの新規操作と利用側の再試行を先に止め、sourceIDを保存内容と独立に指定します。
// polling中はファイルを読むだけにし、最後に実際のloaderで文書全体と登録情報を検証します。
func WaitForContentsAndRestart(t *testing.T, path, sourceID string, original *checkpointstore.FilesystemStore, expected []checkpointstore.Checkpoint) *checkpointstore.FilesystemStore {
	t.Helper()
	wanted := completionGenerations(expected)
	require.Eventually(t, func() bool {
		payload, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		var document struct {
			Source  string                              `json:"source"`
			Cursors []checkpointstore.DestinationCursor `json:"cursors"`
		}
		if json.Unmarshal(payload, &document) != nil || document.Source != sourceID || document.Cursors == nil {
			return false
		}
		return cursorsMatch(document.Cursors, wanted)
	}, 12*time.Second, 10*time.Millisecond)
	require.NoError(t, original.Close())
	restarted, err := reloadMatchingContents(path, sourceID, wanted)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	return restarted
}

// completionMismatchErrorは、再起動したStoreのcursorが期待する完了位置wantedと一致しないことを表します。
type completionMismatchError struct {
	cursors []checkpointstore.DestinationCursor
	wanted  map[objectstorage.DestinationID]string
}

func (e *completionMismatchError) Error() string {
	return fmt.Sprintf("reloaded checkpoint cursors %+v do not match expected completions %v", e.cursors, e.wanted)
}

// reloadMatchingContentsは停止済みStoreの保存内容を、欠落復旧による補正なしに検証します。
// 期待するcursorは文書から作らず、呼び出し元の期待値と実loaderの結果を照合します。
func reloadMatchingContents(path, sourceID string, wanted map[objectstorage.DestinationID]string) (*checkpointstore.FilesystemStore, error) {
	if filepath.Base(path) != sourceID+".log-file-uploads.json" {
		return nil, fmt.Errorf("checkpoint path %q does not match source %q", path, sourceID)
	}
	// 初期化で空documentが作られ、保存済みと誤判定されることを防ぎます。
	if _, err := os.ReadFile(path); err != nil {
		return nil, err
	}
	restarted, err := checkpointstore.NewFilesystemStore(filepath.Dir(path), sourceID, checkpointstore.WithMissingCheckpointRecovery(false))
	if err != nil {
		return nil, err
	}
	cursors, err := restarted.DestinationCursors(context.Background())
	if err == nil && !cursorsMatch(cursors, wanted) {
		err = &completionMismatchError{cursors: cursors, wanted: wanted}
	}
	if err != nil {
		_ = restarted.Close()
		return nil, err
	}
	return restarted, nil
}

func completionGenerations(expected []checkpointstore.Checkpoint) map[objectstorage.DestinationID]string {
	wanted := make(map[objectstorage.DestinationID]string)
	for _, checkpoint := range expected {
		for _, destination := range checkpoint.CompletedDestinationIDs {
			if previous, exists := wanted[destination]; !exists || checkpointstore.CompareGenerations(checkpoint.Key.Generation, previous) > 0 {
				wanted[destination] = checkpoint.Key.Generation
			}
		}
	}
	return wanted
}

func cursorsMatch(cursors []checkpointstore.DestinationCursor, wanted map[objectstorage.DestinationID]string) bool {
	actual := make(map[objectstorage.DestinationID]string)
	seen := make(map[objectstorage.DestinationID]struct{}, len(cursors))
	for _, cursor := range cursors {
		if _, duplicate := seen[cursor.DestinationID]; duplicate {
			return false
		}
		seen[cursor.DestinationID] = struct{}{}
		if cursor.Generation != "" {
			actual[cursor.DestinationID] = cursor.Generation
		}
	}
	if len(actual) != len(wanted) {
		return false
	}
	for destination, generation := range wanted {
		if actual[destination] != generation {
			return false
		}
	}
	return true
}
