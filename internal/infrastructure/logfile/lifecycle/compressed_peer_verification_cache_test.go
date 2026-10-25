package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestExistingCompressedPeerCacheDiscardsObsoleteVerification(t *testing.T) {
	for _, scenario := range []string{"missing", "directory", "changed size", "changed mtime", "changed valid file"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			codec := &fileSeriesConcurrencyCodec{runnerTestCompressionCodec: &runnerTestCompressionCodec{}}
			manager, source := newCompressionConcurrencyManager(t, codec, true)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
			peer := source + ".br"
			key := pathidentity.ComparisonKey(peer)
			valid, err := manager.existingCompressedFileIsValid(source, peer)
			require.NoError(t, err)
			require.True(t, valid)
			previous := manager.verifiedCompressedPeers[key]
			_, verifyCalls := codec.calls()
			require.Equal(t, 1, verifyCalls)

			switch scenario {
			case "missing", "directory":
				require.NoError(t, os.Remove(peer))
				if scenario == "directory" {
					require.NoError(t, os.Mkdir(peer, 0o700))
				}
			case "changed size":
				require.NoError(t, os.WriteFile(peer, []byte("corrupt"), 0o600))
			case "changed mtime":
				// サイズが同じでも、更新日時が変われば以前の検証成功は使いません。
				require.NoError(t, os.WriteFile(peer, bytes.Repeat([]byte("?"), int(previous.size)), 0o600))
				modified := time.Unix(0, previous.modTimeUnixNano).Add(time.Hour)
				require.NoError(t, os.Chtimes(peer, modified, modified))
			case "changed valid file":
				require.NoError(t, os.WriteFile(peer, []byte(runnerTestCompressionMagic+"new larger verified payload"), 0o600))
			}

			// ファイル走査による回収を挟まず、検査自体が古い記録を破棄することを確認します。
			valid, err = manager.existingCompressedFileIsValid(source, peer)
			switch scenario {
			case "missing":
				require.NoError(t, err)
			case "directory":
				require.ErrorIs(t, err, appendlog.ErrCompressedPeerNotRegular)
			case "changed valid file":
				require.NoError(t, err)
				require.True(t, valid)
				_, verifyCalls = codec.calls()
				require.Equal(t, 2, verifyCalls)
				// 変更後のfileで成功した検証を記録し直し、同じfileを再びdecodeしません。
				valid, err = manager.existingCompressedFileIsValid(source, peer)
				require.NoError(t, err)
				require.True(t, valid)
				_, verifyCalls = codec.calls()
				require.Equal(t, 2, verifyCalls)
				return
			default:
				require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
			}
			require.False(t, valid)
			require.NotContains(t, manager.verifiedCompressedPeers, key)
		})
	}
}

func TestExistingCompressedPeerCacheUsesInspectionResultForInvalidation(t *testing.T) {
	t.Parallel()
	codec := &fileSeriesConcurrencyCodec{runnerTestCompressionCodec: &runnerTestCompressionCodec{}}
	manager, source := newCompressionConcurrencyManager(t, codec, true)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	peer := source + ".br"
	key := pathidentity.ComparisonKey(peer)
	valid, err := manager.existingCompressedFileIsValid(source, peer)
	require.NoError(t, err)
	require.True(t, valid)
	previous := manager.verifiedCompressedPeers[key]

	// Lstatのエラー分類に従って判断します。この状況をOSが不存在と報告すれば破棄し、
	// その他の検査エラーなら保持します。退避した元ファイルの内容・更新日時は変えません。
	directory := filepath.Dir(peer)
	parked := directory + "-parked"
	require.NoError(t, os.Rename(directory, parked))
	require.NoError(t, os.WriteFile(directory, []byte("not a directory"), 0o600))
	_, inspectionErr := os.Lstat(peer)
	require.Error(t, inspectionErr)
	reportedAbsent := errors.Is(inspectionErr, os.ErrNotExist)
	valid, err = manager.existingCompressedFileIsValid(source, peer)
	require.False(t, valid)
	if reportedAbsent {
		require.NoError(t, err)
		require.NotContains(t, manager.verifiedCompressedPeers, key)
	} else {
		require.Error(t, err)
		require.Equal(t, previous, manager.verifiedCompressedPeers[key])
	}

	require.NoError(t, os.Remove(directory))
	require.NoError(t, os.Rename(parked, directory))
	valid, err = manager.existingCompressedFileIsValid(source, peer)
	require.NoError(t, err)
	require.True(t, valid)
	_, verifyCalls := codec.calls()
	if reportedAbsent {
		require.Equal(t, 2, verifyCalls)
	} else {
		require.Equal(t, 1, verifyCalls)
	}
}

func TestVerifiedCompressedPeerCacheSurvivesSourceRetirementFailure(t *testing.T) {
	t.Parallel()
	codec := &fileSeriesConcurrencyCodec{runnerTestCompressionCodec: &runnerTestCompressionCodec{}}
	manager, source := newCompressionConcurrencyManager(t, codec, true)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	peer := source + ".br"
	retirementErr := errors.New("source rename failed")
	retirementCalls := 0
	retireSource := manager.removeSourceAfterCompression
	setLogFileProcessingRecoveryInterval(t, manager, 20*time.Millisecond)
	manager.removeSourceAfterCompression = func(raw, compressed string) error {
		require.Equal(t, source, raw)
		require.Equal(t, peer, compressed)
		retirementCalls++
		if retirementCalls < 3 {
			return retirementErr
		}
		return retireSource(raw, compressed)
	}

	_, err := manager.RunOnce(context.Background())
	require.ErrorIs(t, err, retirementErr)
	// 再試行は独立した回復処理に任せ、検証済みpeerを再利用して完了まで待ちます。
	require.NoError(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)))
	require.NoFileExists(t, source)
	require.FileExists(t, peer)
	require.Contains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(peer))
	require.Equal(t, 3, retirementCalls)
	compressCalls, verifyCalls := codec.calls()
	require.Zero(t, compressCalls)
	require.Equal(t, 1, verifyCalls, "an unchanged valid peer is not decoded again after source retirement fails")
}

func TestRemoveInvalidCompressedPeerClearsVerificationWithoutScan(t *testing.T) {
	t.Parallel()
	codec := &fileSeriesConcurrencyCodec{runnerTestCompressionCodec: &runnerTestCompressionCodec{}}
	manager, source := newCompressionConcurrencyManager(t, codec, true)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	peer := source + ".br"
	key := pathidentity.ComparisonKey(peer)
	manager.verifiedCompressedPeers[key] = compressedPeerFingerprint{size: 1}
	require.NoError(t, manager.removeInvalidCompressedPeer(source, peer))
	require.NoFileExists(t, peer)
	require.FileExists(t, source)
	require.NotContains(t, manager.verifiedCompressedPeers, key)
}
