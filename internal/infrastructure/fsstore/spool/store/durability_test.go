package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestSpoolFileSyncPrecedesPublicationAndDirectorySync(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	body := []byte("complete spool bytes\n")
	store := &SpoolStore{}
	var events []string
	var published string
	path, err := store.writeUniqueFilePath(directory, "01000", spooldir.SpoolExt, body,
		func(file *os.File) error {
			events = append(events, "file sync")
			require.True(t, strings.HasSuffix(file.Name(), tmpFileSuffix))
			published = strings.TrimSuffix(file.Name(), tmpFileSuffix)
			_, err := os.Stat(published)
			require.ErrorIs(t, err, os.ErrNotExist, "a worker must not see the file before its contents are synced")
			got, err := os.ReadFile(file.Name())
			require.NoError(t, err)
			require.Equal(t, body, got)
			// 権限もfileの同期より前に確定するため、他者が未公開の一時fileを読むことは
			// ありません。
			assertPrivateSpoolFile(t, file.Name())
			return file.Sync()
		},
		func(path string) error {
			events = append(events, "directory sync")
			require.Equal(t, directory, path)
			require.NotEmpty(t, published)
			got, err := os.ReadFile(published)
			require.NoError(t, err, "sync the published name, not only the temporary name")
			require.Equal(t, body, got)
			assertNoTmpFiles(t, directory)
			return infrafsstore.SyncDirectory(path)
		})
	require.NoError(t, err)
	require.Equal(t, published, path)
	require.Equal(t, []string{"file sync", "directory sync"}, events)
}

func TestSpoolFileSyncFailureDoesNotPublishOrReportSuccess(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	wantErr := errors.New("file sync failed")
	path, err := (&SpoolStore{}).writeUniqueFilePath(directory, "02000", spooldir.SpoolExt, []byte("retry me\n"),
		func(*os.File) error { return wantErr },
		func(string) error {
			t.Fatal("publication continued after file sync failed")
			return nil
		})
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	require.Empty(t, path)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, entries, "remove only the unpublished temporary file")
}

func TestSpoolDirectorySyncFailurePreservesPublishedFile(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	body := []byte("preserve published spool bytes\n")
	wantErr := errors.New("directory sync failed")
	store := &SpoolStore{}
	path, err := store.writeUniqueFilePath(directory, "03000", spooldir.SpoolExt, body,
		(*os.File).Sync,
		func(string) error { return wantErr })
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	require.NotEmpty(t, path, "the already published file remains recoverable")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, got)
	assertNoTmpFiles(t, directory)
	before, err := os.Stat(path)
	require.NoError(t, err)

	// 呼び出し元は、確認できなかった保存を再試行することがあります。先に公開した
	// copyは残します。at-least-once配信では重複を許しますが、そのcopyを上書きしては
	// いけません。
	retried, err := store.writeUniqueFilePath(directory, "03000", spooldir.SpoolExt, body,
		(*os.File).Sync, infrafsstore.SyncDirectory)
	require.NoError(t, err)
	require.NotEqual(t, path, retried)
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	for _, retained := range []string{path, retried} {
		got, err := os.ReadFile(retained)
		require.NoError(t, err)
		require.Equal(t, body, got)
	}
}

func TestStoreByStateReturnsDirectorySyncFailureAndPreservesRecords(t *testing.T) {
	for _, state := range []pkgspool.State{
		pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone,
		pkgspool.StateDead, pkgspool.StateQuarantine,
	} {
		t.Run(string(state), func(t *testing.T) {
			now := time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC)
			store := newTestSpoolStore(t, fixedClock{now: now}, testSpoolStoreConfig{
				spoolPath: t.TempDir(), target: "durable", dir: "record",
			})
			require.NoError(t, store.EnsureCurrentTimePartitionDirectory(context.Background(), state))
			directory, err := store.timePartitionDirectory(state, now)
			require.NoError(t, err)
			restore := testutil.DenyDirectorySync(t, directory)
			records := []pkgspool.SpoolRecord{{
				OccurredAt: now, RequestID: "retained", Payload: json.RawMessage(`{"message":"keep complete bytes"}`),
			}}
			err = store.StoreByState(context.Background(), state, records)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			restore()
			files := listSpoolFiles(t, directory)
			require.Len(t, files, 1)
			assertSpoolRecords(t, readSpoolRecords(t, files[0]), records)
			assertNoTmpFiles(t, store.RootDir.String())
		})
	}
}

func TestStoreByStateDoesNotAcceptUnsyncedNewTimePartition(t *testing.T) {
	now := time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC)
	cfg := testSpoolStoreConfig{
		spoolPath: t.TempDir(), target: "durable", dir: "record",
	}
	store := newTestSpoolStore(t, fixedClock{now: now}, cfg)
	stateDirectory := store.RootDir.ActiveDir().Path()
	require.NoError(t, os.MkdirAll(stateDirectory, 0o755))
	restore := testutil.DenyDirectorySync(t, stateDirectory)
	records := []pkgspool.SpoolRecord{{
		OccurredAt: now, Payload: json.RawMessage(`{"message":"retry after directory setup failure"}`),
	}}
	err := store.StoreByState(context.Background(), pkgspool.StateActive, records)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	// 作成済みのdirectoryがあっても、別のStoreがこの失敗を回避できてはいけません。
	otherStore := newTestSpoolStore(t, fixedClock{now: now}, cfg)
	err = otherStore.StoreByState(context.Background(), pkgspool.StateActive, records)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	restore()
	require.Empty(t, listSpoolFiles(t, stateDirectory), "do not publish records before the time partition's parent is synced")
	_, err = os.Stat(filepath.Join(stateDirectory, "2026"))
	require.NoError(t, err, "directory creation can succeed even when its parent sync fails")
	require.NoError(t, otherStore.StoreByState(context.Background(), pkgspool.StateActive, records))
	files := listSpoolFiles(t, stateDirectory)
	require.Len(t, files, 1)
	assertSpoolRecords(t, readSpoolRecords(t, files[0]), records)
}
