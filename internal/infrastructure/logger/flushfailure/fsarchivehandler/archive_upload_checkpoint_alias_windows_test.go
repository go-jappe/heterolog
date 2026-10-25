package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestWindowsFailureArchiveCheckpointAliasesKeepPartialUploadSuccess(t *testing.T) {
	for _, aliasKind := range []string{"case", "extended-case"} {
		t.Run(aliasKind, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
			failureCategory := failurearchive.FailureCategoryRetryable
			required := []objectstorage.DestinationID{"primary", "secondary"}
			initial := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(failureCategory, &checkpointDestinationRecordingFailureArchiveUploader{destinationIDs: required})
			})
			handler := initial.handler
			store := handler.compressedArchiveUploadCheckpointStore(failureCategory)
			path := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260914_1.jsonl.gz")
			require.NoError(t, os.WriteFile(path, []byte("published archive"), 0o600))
			alias := filepath.Join(strings.ToUpper(filepath.Dir(path)), filepath.Base(path))
			if aliasKind == "extended-case" {
				if strings.HasPrefix(alias, `\\`) {
					alias = `\\?\UNC\` + alias[2:]
				} else {
					alias = `\\?\` + alias
				}
			}
			require.NotEqual(t, path, alias)
			require.FileExists(t, alias)

			key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
			_, err := store.Load(ctx, key)
			require.NoError(t, err)
			checkpoint, err := store.AdvanceCompletionCursor(ctx, key, required[:1])
			require.NoError(t, err)
			require.False(t, checkpoint.Complete(required))

			// 再起動後の別表記でも、保存済みprimaryの成功を引き継ぎます。
			remote := &checkpointDestinationRecordingFailureArchiveUploader{destinationIDs: required}
			restarted := restartTestArchiveHandler(t, initial, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(failureCategory, remote)
			})
			handler = restarted.handler
			outcome := handler.uploadCheckpointedArchiveGroup(
				ctx, []string{alias}, failureCategory, handler.Fsstore.FileNameRetryable,
			)
			require.False(t, outcome.retryRequired)
			require.True(t, outcome.completion.Attempted)
			require.Equal(t, []objectstorage.DestinationID{"secondary"}, remote.requested)
			require.NoFileExists(t, path)
			require.NoFileExists(t, alias)

			// 別表記で完了した世代も同じ世代として全宛先の完了を記録し、途中経過を残しません。
			store = handler.compressedArchiveUploadCheckpointStore(failureCategory)
			checkpoint, err = store.Load(ctx, key)
			require.NoError(t, err)
			require.True(t, checkpoint.Complete(required))
			for _, destination := range required {
				_, found, err := store.LoadUploadProgress(ctx, key, destination)
				require.NoError(t, err)
				require.False(t, found, "completed destination %s must not keep upload progress", destination)
			}
		})
	}
}
