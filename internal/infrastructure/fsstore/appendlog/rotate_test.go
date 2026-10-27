package appendlog

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestRotateWithRetryKeepsOldPathPendingWhenClosingActiveFileFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.log")
	newPath := filepath.Join(dir, "new.log")
	file, err := OpenFile(oldPath)
	require.NoError(t, err)
	_, err = file.WriteString("before\n")
	require.NoError(t, err)
	// os.File.Closeがerrorを返した後のhandleは、再度のClose・Stat・Writeがすべて失敗します。
	// 閉じ済みhandleでその状態を再現し、rotationのCloseをos.ErrClosedで失敗させます。
	require.NoError(t, file.Close())

	guard := DirectoryGuardForPath(dir)
	writer := NewDirectoryGuardToken()
	require.NoError(t, guard.TransitionActive(writer, func() (string, error) { return oldPath, nil }))
	t.Cleanup(func() { guard.RemoveToken(writer) })
	store := &FileStore{File: file, ActiveFilePath: oldPath, Tracer: observer.NopTracer{}}
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// Closeの再試行を許す設定でも、失敗したCloseは一度だけ報告します。
	retry := NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteRetries: 2, FileOperationRetryWaitTime: time.Nanosecond})
	var closeReports []*logging.LogEntry
	logFunc := func(_ context.Context, err error, entry *logging.LogEntry) {
		if errors.Is(err, os.ErrClosed) {
			closeReports = append(closeReports, entry)
		}
	}
	entryFactory := func(_ context.Context, message string, err error) *logging.LogEntry {
		return &logging.LogEntry{Message: message, Err: err}
	}
	// FileLoggerのopenと同じく、active pathの切替をopen成功と同じguard境界で確定します。
	replacementAvailable := false
	openReplacement := func(context.Context) error {
		return guard.TransitionActive(writer, func() (string, error) {
			if !replacementAvailable {
				return "", errors.New("replacement is temporarily unavailable")
			}
			replacement, err := OpenFile(newPath)
			if err != nil {
				return "", err
			}
			store.File = replacement
			store.ActiveFilePath = newPath
			return newPath, nil
		})
	}

	path, err := store.RotateWithRetry(ctx, logFunc, entryFactory, openReplacement, retry)
	require.ErrorIs(t, err, os.ErrClosed, "the close failure must be reported to the caller")
	require.Equal(t, oldPath, path)
	require.Len(t, closeReports, 1, "a failed Close cannot succeed on retry and is reported once")
	require.Equal(t, fmt.Sprintf(ErrorMesFailedFileClose, oldPath), closeReports[0].Message)
	require.Nil(t, store.File, "a handle whose Close failed must not remain the active file")
	require.True(t, store.RotationPending(), "the old path must stay pending so the open can resume")
	require.Equal(t, oldPath, store.ActiveFilePath)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, map[string]struct{}{pathidentity.AbsoluteOrCleanPath(oldPath): {}}, protected,
			"the old path must stay protected until the replacement opens")
	})
	content, err := os.ReadFile(oldPath)
	require.NoError(t, err)
	require.Equal(t, "before\n", string(content))

	// 障害が解消した後の呼び出しは、旧handleを再びCloseせずに新fileのopenから再開します。
	replacementAvailable = true
	path, err = store.RotateWithRetry(ctx, logFunc, entryFactory, openReplacement, retry)
	require.NoError(t, err)
	require.Equal(t, oldPath, path, "the resumed rotation still reports the old path for archiving")
	require.Len(t, closeReports, 1, "the resumed rotation must not close the old handle again")
	require.False(t, store.RotationPending())
	require.Equal(t, newPath, store.ActiveFilePath)
	require.Equal(t, newPath, store.File.Name())
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, map[string]struct{}{pathidentity.AbsoluteOrCleanPath(newPath): {}}, protected)
	})
	_, err = store.WriteStringWithRetry(ctx, "rotate-test", "after", nil, nil, nil, "write", nil, retry)
	require.NoError(t, err, "writes must continue in the replacement after a close failure")
	content, err = os.ReadFile(oldPath)
	require.NoError(t, err)
	require.Equal(t, "before\n", string(content))
	content, err = os.ReadFile(newPath)
	require.NoError(t, err)
	require.Equal(t, "after\n", string(content))
}

func TestRotateWithRetryResumesOpeningWithoutClosingOldHandleAgain(t *testing.T) {
	for _, samePath := range []bool{false, true} {
		name := "new_path"
		if samePath {
			name = "same_path"
		}
		t.Run(name, func(t *testing.T) {
			oldPath := filepath.Join(t.TempDir(), "old.log")
			file, err := OpenFile(oldPath)
			require.NoError(t, err)
			_, err = file.WriteString("before\n")
			require.NoError(t, err)
			store := &FileStore{File: file, ActiveFilePath: oldPath}
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			retry := NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteRetries: 0, FileOperationRetryWaitTime: time.Nanosecond})
			newPath := filepath.Join(filepath.Dir(oldPath), "new.log")
			if samePath {
				newPath = oldPath
			}
			unavailable := errors.New("temporarily unavailable")
			for range 2 {
				path, err := store.RotateWithRetry(context.Background(), nil, nil, func(context.Context) error { return unavailable }, retry)
				require.ErrorIs(t, err, unavailable)
				require.Equal(t, oldPath, path)
				require.Equal(t, oldPath, store.ActiveFilePath)
				require.True(t, store.RotationPending())
				_, err = file.Stat()
				require.True(t, testutil.IsClosedFileError(err), "old handle must stay closed: Stat error = %v", err)
				content, err := os.ReadFile(oldPath)
				require.NoError(t, err)
				require.Equal(t, "before\n", string(content))
			}
			path, err := store.RotateWithRetry(context.Background(), nil, nil, func(context.Context) error {
				replacement, err := OpenFile(newPath)
				if err != nil {
					return err
				}
				store.File = replacement
				store.ActiveFilePath = newPath
				return nil
			}, retry)
			require.NoError(t, err)
			require.Equal(t, oldPath, path)
			require.False(t, store.RotationPending())
			_, err = store.File.WriteString("after\n")
			require.NoError(t, err)
			oldContent, err := os.ReadFile(oldPath)
			require.NoError(t, err)
			newContent, err := os.ReadFile(newPath)
			require.NoError(t, err)
			if samePath {
				require.Equal(t, "before\nafter\n", string(newContent))
			} else {
				require.Equal(t, "before\n", string(oldContent))
				require.Equal(t, "after\n", string(newContent))
			}
		})
	}
}

func TestCloseCancelsPendingRotation(t *testing.T) {
	file, err := OpenFile(filepath.Join(t.TempDir(), "old.log"))
	require.NoError(t, err)
	store := &FileStore{File: file, ActiveFilePath: file.Name()}
	retry := NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteRetries: 0, FileOperationRetryWaitTime: time.Nanosecond})
	_, err = store.RotateWithRetry(context.Background(), nil, nil, func(context.Context) error { return errors.New("unavailable") }, retry)
	require.Error(t, err)
	require.NoError(t, store.Close())
	require.False(t, store.RotationPending())
	_, err = store.RotateWithRetry(context.Background(), nil, nil, func(context.Context) error { t.Fatal("must not reopen after explicit Close"); return nil }, retry)
	require.Error(t, err)
}
