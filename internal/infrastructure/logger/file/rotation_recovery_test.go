package file

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFileLoggersRecoverAfterRotationOpenFailure(t *testing.T) {
	for _, kind := range []string{"file", "delivery", "file_inline"} {
		for _, rotation := range []string{"daily", "size"} {
			t.Run(kind+"/"+rotation, func(t *testing.T) {
				writer := newRotationRecoveryWriter(t, kind)
				oldPath, nextPath, oldContent := failRotationOpen(t, writer, rotation)
				require.NoError(t, os.Remove(nextPath))
				require.NoError(t, writer.write("after recovery"))
				require.NoError(t, writer.write("following record"))
				require.Equal(t, nextPath, writer.store.ActiveFilePath)
				require.Equal(t, nextPath, writer.store.File.Name())
				writer.guard.WithProtectedPaths(func(paths map[string]struct{}) {
					require.Contains(t, paths, nextPath)
				})
				// Closeは、inlineのarchiveがあればそれを待ちます。archiveの内容は旧世代だけの
				// ままでなければならず、新しいactive世代はrawのままでなければなりません。
				require.NoError(t, writer.close())
				if kind == "file_inline" {
					require.NoFileExists(t, oldPath)
					compressed, err := os.ReadFile(oldPath + ".gz")
					require.NoError(t, err)
					reader, err := gzip.NewReader(bytes.NewReader(compressed))
					require.NoError(t, err)
					content, err := io.ReadAll(reader)
					require.NoError(t, err)
					require.NoError(t, reader.Close())
					require.Equal(t, oldContent, content)
				} else {
					content, err := os.ReadFile(oldPath)
					require.NoError(t, err)
					require.Equal(t, oldContent, content)
				}
				content, err := os.ReadFile(nextPath)
				require.NoError(t, err)
				require.Equal(t, 1, bytes.Count(content, []byte("after recovery")))
				require.Equal(t, 1, bytes.Count(content, []byte("following record")))
				require.NotContains(t, string(content), "failed record")
				require.NoFileExists(t, nextPath+".gz")
				require.ErrorIs(t, writer.write("after close"), pkglogger.ErrLoggerClosed)
			})
		}
	}
}

// rotation前の旧fileのCloseが失敗した書き込みはerrorを返します。旧handleは再利用せず、
// 次の書き込みで新しいfileを開いてrotationを完了し、旧fileは通常のrotation後と同様に扱います。
func TestFileLoggersRecoverAfterRotationCloseFailure(t *testing.T) {
	for _, kind := range []string{"file", "delivery", "file_inline"} {
		t.Run(kind, func(t *testing.T) {
			writer := newRotationRecoveryWriter(t, kind)
			at, err := time.Parse(pkgfsstore.FileDateFormat, writer.store.FileName.Date)
			require.NoError(t, err)
			at = at.Add(12 * time.Hour)
			writer.setTime(at)
			require.NoError(t, writer.write("before rotation failure"))
			oldPath := writer.store.ActiveFilePath
			oldName := writer.store.FileName
			oldContent, err := os.ReadFile(oldPath)
			require.NoError(t, err)

			// Closeが一度失敗したhandleは再びCloseできないため、先に閉じておくことで
			// 次のrotationでの旧fileのCloseを実際のerrorで失敗させます。
			writer.store.Mu.Lock()
			closeErr := writer.store.File.Close()
			writer.store.Mu.Unlock()
			require.NoError(t, closeErr)
			next := at.AddDate(0, 0, 1)
			writer.setTime(next)
			require.ErrorIs(t, writer.write("failed record"), os.ErrClosed)
			writer.store.Mu.Lock()
			pending := writer.store.RotationPending()
			writer.store.Mu.Unlock()
			require.True(t, pending, "close failure must leave the rotation pending instead of reusing the old handle")
			writer.guard.WithProtectedPaths(func(paths map[string]struct{}) {
				require.Contains(t, paths, oldPath)
			})

			// 次の書き込みは新しいfileを開いて成功し、以降の書き込みも続けられます。
			require.NoError(t, writer.write("after recovery"))
			require.NoError(t, writer.write("following record"))
			nextPath := filepath.Join(oldName.Dir, fmt.Sprintf("%s%s_%s_1.log", oldName.Prefix, oldName.Target, next.Format(pkgfsstore.FileDateFormat)))
			require.Equal(t, nextPath, writer.store.ActiveFilePath)
			writer.guard.WithProtectedPaths(func(paths map[string]struct{}) {
				require.Contains(t, paths, nextPath)
			})
			// Closeはinlineの圧縮を待ちます。旧fileには失敗した記録を含めず、元の内容だけを残します。
			require.NoError(t, writer.close())
			if kind == "file_inline" {
				require.NoFileExists(t, oldPath)
				compressed, err := os.ReadFile(oldPath + ".gz")
				require.NoError(t, err)
				reader, err := gzip.NewReader(bytes.NewReader(compressed))
				require.NoError(t, err)
				content, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				require.Equal(t, oldContent, content)
			} else {
				content, err := os.ReadFile(oldPath)
				require.NoError(t, err)
				require.Equal(t, oldContent, content)
			}
			content, err := os.ReadFile(nextPath)
			require.NoError(t, err)
			require.Equal(t, 1, bytes.Count(content, []byte("after recovery")))
			require.Equal(t, 1, bytes.Count(content, []byte("following record")))
			require.NotContains(t, string(content), "failed record")
			require.NoFileExists(t, nextPath+".gz")
		})
	}
}

func TestFileLoggersCloseDuringPendingRotationReleasesProtection(t *testing.T) {
	for _, kind := range []string{"file", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			writer := newRotationRecoveryWriter(t, kind)
			oldPath, nextPath, oldContent := failRotationOpen(t, writer, "daily")
			require.NoError(t, writer.close())
			writer.guard.WithProtectedPaths(func(paths map[string]struct{}) {
				require.NotContains(t, paths, oldPath)
			})
			require.NoError(t, os.Remove(nextPath))
			require.ErrorIs(t, writer.write("after close"), pkglogger.ErrLoggerClosed)
			require.NoFileExists(t, nextPath)
			content, err := os.ReadFile(oldPath)
			require.NoError(t, err)
			require.Equal(t, oldContent, content)
		})
	}
}

func TestFileLoggerPendingRotationReopensSamePathWithoutArchiving(t *testing.T) {
	ctx := context.Background()
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "same-path-recovery")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	period, err := time.Parse(pkgfsstore.FileDateFormat, logger.Fsstore.FileName.Date)
	require.NoError(t, err)
	logger.Clock = clock.NewFixedClock(period.Add(12 * time.Hour))
	entry := &logging.LogEntry{OccurredAt: period, Level: pkglogger.LogLevelInfo, Message: "before same-path rotation"}
	require.NoError(t, logger.HandleLogEntry(ctx, entry))
	want := logger.LogFormatter.Format(entry) + "\n"
	oldPath := logger.Fsstore.ActiveFilePath
	oldName := logger.Fsstore.FileName
	// 既存のrotationの入口を通して、同じ世代のreopenを強制します。
	// 不正なdirectoryは、次に公開の入口から書き込む前にfixtureから取り除きます。
	notDirectory := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(notDirectory, []byte("block child open"), 0o600))
	logger.Fsstore.FileName.Dir = notDirectory
	require.Error(t, rotateFileLoggerForTest(ctx, logger))
	logger.Fsstore.FileName = oldName
	entry.Message = "after same-path recovery"
	require.NoError(t, logger.HandleLogEntry(ctx, entry))
	want += logger.LogFormatter.Format(entry) + "\n"
	require.Equal(t, oldPath, logger.Fsstore.ActiveFilePath)
	logger.lifecycleGuard.WithProtectedPaths(func(paths map[string]struct{}) {
		require.Contains(t, paths, oldPath)
	})
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
	content, err := os.ReadFile(oldPath)
	require.NoError(t, err)
	require.Equal(t, want, string(content))
	require.NoFileExists(t, oldPath+".gz")
}

type rotationRecoveryWriter struct {
	store       *appendlog.FileStore
	guard       *appendlog.DirectoryGuard
	maxFileSize uint64
	setTime     func(time.Time)
	write       func(string) error
	close       func() error
	// waitBackgroundは、それまでの書き込みについてloggerが受け付けた、
	// inline cleanupなどのbackground処理を待ちます。
	waitBackground func()
}

// inline cleanupは、writeが戻った後にclockを読むことがあります。test全体で同じClock
// instanceを使い続け、その時刻の変更を同期します。
type rotationRecoveryClock struct {
	mu sync.RWMutex
	at time.Time
}

func (c *rotationRecoveryClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.at
}

func (c *rotationRecoveryClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

func newRotationRecoveryWriter(t *testing.T, kind string) rotationRecoveryWriter {
	t.Helper()
	ctx := context.Background()
	testClock := &rotationRecoveryClock{at: time.Now().UTC()}
	entry := func(message string) *logging.LogEntry {
		return &logging.LogEntry{OccurredAt: time.Now().UTC(), Level: pkglogger.LogLevelInfo, Message: message}
	}
	var writer rotationRecoveryWriter
	if kind == "delivery" {
		cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "rotation-recovery")
		cfg.FsstoreConfig.Rotation.EnableRotate = true
		logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger.Clock = testClock
		writer = rotationRecoveryWriter{
			store: &logger.Fsstore, guard: logger.lifecycleGuard,
			maxFileSize: cfg.FsstoreConfig.Rotation.MaxFileSize,
			setTime:     testClock.set,
			close:       logger.Close,
			// delivery file loggerはbackground処理を開始しません。
			waitBackground: func() {},
			write: func(message string) error {
				report, err := logger.HandleLogEntry(ctx, entry(message))
				if err == nil {
					requireDeliverySucceeded(t, report, err)
				} else if errors.Is(err, pkglogger.ErrLoggerClosed) {
					requireDeliveryFileClosedRejection(t, report)
				} else {
					result := requireSingleDeliveryTarget(t, report)
					require.Equal(t, pkglogger.DeliveryFailed, result.Status)
					require.ErrorIs(t, result.Err, err)
				}
				return err
			},
		}
	} else {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "rotation-recovery")
		cfg.FsstoreConfig.Lifecycle.EnableRotate = true
		cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
		if kind == "file_inline" {
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
			cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
		}
		logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger.Clock = testClock
		writer = rotationRecoveryWriter{
			store: &logger.Fsstore, guard: logger.lifecycleGuard,
			maxFileSize:    cfg.FsstoreConfig.Lifecycle.MaxFileSize,
			setTime:        testClock.set,
			close:          logger.Close,
			write:          func(message string) error { return logger.HandleLogEntry(ctx, entry(message)) },
			waitBackground: func() { waitBackgroundForTest(t, logger) },
		}
	}
	t.Cleanup(func() { require.NoError(t, writer.close()) })
	return writer
}

// 次のfilenameの位置にdirectoryを置き、権限の細工やOS固有のhookを使わずに実際のopen errorを
// 起こします。そのdirectoryを削除すると、filesystemは元に戻ります。
func failRotationOpen(t *testing.T, writer rotationRecoveryWriter, rotation string) (oldPath, nextPath string, oldContent []byte) {
	t.Helper()
	at, err := time.Parse(pkgfsstore.FileDateFormat, writer.store.FileName.Date)
	require.NoError(t, err)
	at = at.Add(12 * time.Hour)
	writer.setTime(at)
	require.NoError(t, writer.write("before rotation failure"))
	oldPath = writer.store.ActiveFilePath
	oldHandle := writer.store.File
	oldName := writer.store.FileName
	index := oldName.Index + 1
	if rotation == "daily" {
		at = at.AddDate(0, 0, 1)
		index = 1
	} else {
		info, err := oldHandle.Stat()
		require.NoError(t, err)
		padding := make([]byte, 32*1024)
		for remaining := int64(writer.maxFileSize) - info.Size(); remaining > 0; {
			n, err := oldHandle.Write(padding[:min(remaining, int64(len(padding)))])
			require.NoError(t, err)
			require.Positive(t, n)
			remaining -= int64(n)
		}
	}
	oldContent, err = os.ReadFile(oldPath)
	require.NoError(t, err)
	nextPath = filepath.Join(oldName.Dir, fmt.Sprintf("%s%s_%s_%d.log", oldName.Prefix, oldName.Target, at.Format(pkgfsstore.FileDateFormat), index))
	require.NoError(t, os.Mkdir(nextPath, 0o700))
	writer.setTime(at)
	for range 2 {
		require.Error(t, writer.write("failed record"))
		// writeが開始するinline cleanupはdirectoryを走査し、そのentryを保全する間、次の名前に
		// あるdirectoryを保護します。以下の確認が、失敗したrotation自体が残したものだけを
		// 見るように、そのcleanupを待ちます。
		writer.waitBackground()
		require.Equal(t, oldPath, writer.store.ActiveFilePath)
		require.Equal(t, oldName, writer.store.FileName)
		_, err := oldHandle.Stat()
		require.True(t, testutil.IsClosedFileError(err), "archive can only start after the old handle has closed: Stat error = %v", err)
		writer.guard.WithProtectedPaths(func(paths map[string]struct{}) {
			require.Contains(t, paths, oldPath, "failed rotation must retain the old generation's protection")
			require.NotContains(t, paths, nextPath)
		})
		content, err := os.ReadFile(oldPath)
		require.NoError(t, err)
		require.Equal(t, oldContent, content)
		require.NoFileExists(t, oldPath+".gz", "failed rotation must not start archiving the old generation")
	}
	return oldPath, nextPath, oldContent
}
