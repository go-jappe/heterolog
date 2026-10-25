package objectstoragespool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

// macOSのFAT32・ExFATでは、拡張属性を保存する「._」の付随fileが圧縮ログや宛先directoryの隣に
// 作られます。APFSは付随fileを作らないため、同じ名前のfileを置いて再現します。

func TestStoreStateScanIgnoresAppleDoubleCompanionsOfCompressedLogs(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	ctx := context.Background()
	require.NoError(t, store.EnsureLayout(ctx))
	directory, err := store.Root().StateDir(pkgspool.StateActive)
	require.NoError(t, err)
	validPath := filepath.Join(directory, "app_20261004_1.log.gz")
	companionPath := filepath.Join(directory, "._app_20261004_1.log.gz")
	require.NoError(t, os.WriteFile(validPath, []byte("valid"), payloadMode))
	require.NoError(t, os.WriteFile(companionPath, []byte("extended attributes"), payloadMode))

	// 付随fileは送信待ちのfileとして列挙せず、不正なentryとして退避もしません。
	files, err := store.ListFiles(ctx, pkgspool.StateActive)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, validPath, files[0].Path)
	require.FileExists(t, companionPath)
	warnings, err := stores.TakePreservationWarnings(ctx)
	require.NoError(t, err)
	require.Empty(t, warnings)
}

func TestDestinationStoresExistingIgnoresAppleDoubleCompanionsOfDestinations(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	ctx := context.Background()
	require.NoError(t, store.EnsureLayout(ctx))
	destination := store.Root().Path()
	companion := filepath.Join(filepath.Dir(destination), "._"+filepath.Base(destination))
	require.NoError(t, os.WriteFile(companion, []byte("extended attributes"), payloadMode))

	// 宛先directoryの付随fileを、実際のdirectoryではない宛先として通知しません。
	observed, err := stores.Existing(ctx)
	require.NoError(t, err)
	require.Len(t, observed, 1)
	require.Equal(t, destination, observed[0].Root().Path())
}
