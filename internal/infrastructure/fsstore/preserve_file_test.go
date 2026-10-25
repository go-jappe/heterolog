package fsstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestPreserveRegularFileReusesOnlyVerifiedSameNameAndBytes(t *testing.T) {
	root := resolvedTempDirForTest(t)
	source := filepath.Join(t.TempDir(), "app_20261003_1.log.custom")
	body := bytes.Repeat([]byte{0x00, 0xff, 0x81, 0x37}, 96*1024)
	require.NoError(t, os.WriteFile(source, body, 0o600))
	first, err := PreserveRegularFile(nil, root, source)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, PreservedDirectoryName), filepath.Dir(first))
	info, err := os.Stat(first)
	require.NoError(t, err)
	second, err := PreserveRegularFile(context.Background(), root, source)
	require.NoError(t, err)
	require.Equal(t, first, second)
	secondInfo, err := os.Stat(second)
	require.NoError(t, err)
	require.True(t, os.SameFile(info, secondInfo), "retry must not replace the accepted copy")
	changed := []byte("different bytes from the same basename")
	require.NoError(t, os.WriteFile(source, changed, 0o600))
	third, err := PreserveRegularFile(context.Background(), root, source)
	require.NoError(t, err)
	require.NotEqual(t, first, third)
	otherName := filepath.Join(t.TempDir(), "other_20261003_1.log.custom")
	require.NoError(t, os.WriteFile(otherName, body, 0o600))
	fourth, err := PreserveRegularFile(context.Background(), root, otherName)
	require.NoError(t, err)
	require.NotEqual(t, first, fourth, "basename is part of the identity")
	for path, want := range map[string][]byte{first: body, third: changed, fourth: body, source: changed} {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	entries, err := os.ReadDir(filepath.Join(root, PreservedDirectoryName))
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestPreserveRegularFileStopsCanceledOrCorruptCopyBeforePublication(t *testing.T) {
	for _, action := range []string{"cancel", "corrupt"} {
		t.Run(action, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			directory := filepath.Join(root, PreservedDirectoryName)
			require.NoError(t, EnsureDirectoryNoSymlinks(directory, 0o750))
			otherTemporary := filepath.Join(directory, ".preserve-other.tmp")
			require.NoError(t, os.WriteFile(otherTemporary, []byte("other task"), 0o600))
			source := filepath.Join(t.TempDir(), "app_20261003_1.log.custom")
			body := bytes.Repeat([]byte("\x00\xffsource"), 96*1024)
			require.NoError(t, os.WriteFile(source, body, 0o600))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := false
			observed := &preserveObservedContext{Context: ctx, observe: func() {
				if injected {
					return
				}
				entries, err := os.ReadDir(directory)
				require.NoError(t, err)
				for _, entry := range entries {
					path := filepath.Join(directory, entry.Name())
					if path == otherTemporary {
						continue
					}
					file, err := os.Open(path)
					require.NoError(t, err)
					info, err := file.Stat()
					require.NoError(t, err)
					require.NoError(t, file.Close())
					if action == "cancel" && info.Size() > 0 && info.Size() < int64(len(body)) {
						injected = true
						cancel()
						return
					}
					if action == "corrupt" && info.Size() == int64(len(body)) {
						file, err := os.OpenFile(path, os.O_WRONLY, 0)
						require.NoError(t, err)
						_, err = file.WriteAt([]byte("!"), info.Size()-1)
						require.NoError(t, err)
						require.NoError(t, file.Close())
						require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
						injected = true
						return
					}
				}
			}}
			path, err := PreserveRegularFile(observed, root, source)
			require.True(t, injected)
			require.Empty(t, path)
			if action == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, ErrCopyDiffersFromSource)
			}
			entries, err := os.ReadDir(directory)
			require.NoError(t, err)
			require.Len(t, entries, 1, "only this call's unpublished temporary may be removed")
			require.Equal(t, filepath.Base(otherTemporary), entries[0].Name())
			got, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, body, got)
		})
	}
}

func TestPreserveRegularFileRetriesDirectorySyncWithoutReplacingPublishedCopy(t *testing.T) {
	for _, syncRoot := range []bool{false, true} {
		name := "preserved directory"
		if syncRoot {
			name = "root directory"
		}
		t.Run(name, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			source := filepath.Join(t.TempDir(), "app_20261003_1.log.gz")
			body := []byte("preserved bytes awaiting directory sync")
			require.NoError(t, os.WriteFile(source, body, 0o600))
			failure := errors.New("directory sync failed")
			failedDirectory := filepath.Join(root, PreservedDirectoryName)
			if syncRoot {
				failedDirectory = root
			}
			path, err := preserveRegularFile(context.Background(), root, source, func(directory string) error {
				if directory == failedDirectory {
					return failure
				}
				return SyncDirectory(directory)
			})
			require.ErrorIs(t, err, failure)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			require.NotEmpty(t, path, "publication is reported even when durability remains unconfirmed")
			info, err := os.Stat(path)
			require.NoError(t, err)
			var synced []string
			retried, err := preserveRegularFile(context.Background(), root, source, func(directory string) error {
				synced = append(synced, directory)
				return SyncDirectory(directory)
			})
			require.NoError(t, err)
			require.Equal(t, path, retried)
			require.Contains(t, synced, failedDirectory)
			retriedInfo, err := os.Stat(retried)
			require.NoError(t, err)
			require.True(t, os.SameFile(info, retriedInfo))
			entries, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			require.Len(t, entries, 1)
			for _, existing := range []string{source, path} {
				got, err := os.ReadFile(existing)
				require.NoError(t, err)
				require.Equal(t, body, got)
			}
		})
	}
}

func TestPreserveRegularFileRejectsLinksAndCorruptExistingCopy(t *testing.T) {
	for _, kind := range []string{"source link", "root link", "preserved directory link", "existing copy link", "corrupt existing copy"} {
		t.Run(kind, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			source := filepath.Join(t.TempDir(), "app_20261003_1.log.gz")
			require.NoError(t, os.WriteFile(source, []byte("source"), 0o600))
			outside := t.TempDir()
			marker := filepath.Join(outside, "marker")
			require.NoError(t, os.WriteFile(marker, []byte("outside"), 0o600))
			switch kind {
			case "source link":
				link := filepath.Join(t.TempDir(), filepath.Base(source))
				testutil.Symlink(t, source, link)
				source = link
			case "root link":
				root = filepath.Join(resolvedTempDirForTest(t), "linked-root")
				testutil.Symlink(t, outside, root)
			case "preserved directory link":
				testutil.Symlink(t, outside, filepath.Join(root, PreservedDirectoryName))
			default:
				path, err := PreserveRegularFile(context.Background(), root, source)
				require.NoError(t, err)
				if kind == "existing copy link" {
					require.NoError(t, os.Remove(path))
					testutil.Symlink(t, marker, path)
				} else {
					require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))
				}
			}
			path, err := PreserveRegularFile(context.Background(), root, source)
			require.Error(t, err)
			require.Empty(t, path)
			got, err := os.ReadFile(marker)
			require.NoError(t, err)
			require.Equal(t, "outside", string(got))
			got, err = os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, "source", string(got))
		})
	}
}

func TestPreserveRegularFileConcurrentCopiesPublishOneVerifiedFile(t *testing.T) {
	root := resolvedTempDirForTest(t)
	source := filepath.Join(t.TempDir(), "app_20261003_1.log.gz")
	require.NoError(t, os.WriteFile(source, []byte("same concurrent source"), 0o600))
	require.NoError(t, EnsureDirectoryNoSymlinks(filepath.Join(root, PreservedDirectoryName), 0o750))
	type result struct {
		path string
		err  error
	}
	results := make(chan result, 8)
	var tasks sync.WaitGroup
	for range cap(results) {
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			path, err := PreserveRegularFile(context.Background(), root, source)
			results <- result{path, err}
		}()
	}
	tasks.Wait()
	close(results)
	var first string
	for got := range results {
		require.NoError(t, got.err)
		if first == "" {
			first = got.path
		}
		require.Equal(t, first, got.path)
	}
	entries, err := os.ReadDir(filepath.Join(root, PreservedDirectoryName))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	got, err := os.ReadFile(first)
	require.NoError(t, err)
	require.Equal(t, "same concurrent source", string(got))
}

type preserveObservedContext struct {
	context.Context
	observe func()
}

func (ctx *preserveObservedContext) Err() error {
	ctx.observe()
	return ctx.Context.Err()
}
