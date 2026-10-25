package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestFileSeriesManagerLeavesCompressionSourceSymlinkUntouched(t *testing.T) {
	for _, hasCompressedPeer := range []bool{false, true} {
		name := "without-compressed-peer"
		if hasCompressedPeer {
			name = "with-valid-compressed-peer"
		}
		t.Run(name, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "source-symlink"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))
			date := currentLogDate()
			sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
			peerPath := sourcePath + ".gz"
			latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
			externalPath := filepath.Join(resolvedTempDirForTest(t), "external.log")
			const payload = "must not be compressed or retired through a symlink"
			require.NoError(t, os.WriteFile(externalPath, []byte(payload), 0o600))
			testutil.Symlink(t, externalPath, sourcePath)
			require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))
			var peerContent []byte
			if hasCompressedPeer {
				writeGzipFile(t, peerPath, []byte(payload))
				var err error
				peerContent, err = os.ReadFile(peerPath)
				require.NoError(t, err)
			}

			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
			})
			// 圧縮済みの同名ファイルがあっても、元ログのリンクを圧縮完了として
			// 退避・削除しません。最新世代の判定は従来どおりファイル名が正本です。
			report, err := manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.Empty(t, report.Compressed)
			sourceInfo, err := os.Lstat(sourcePath)
			require.NoError(t, err)
			require.NotZero(t, sourceInfo.Mode()&os.ModeSymlink)
			body, err := os.ReadFile(externalPath)
			require.NoError(t, err)
			require.Equal(t, payload, string(body))
			_, err = os.Lstat(logpattern.CompressionSourceDeletePendingPath(sourcePath))
			require.ErrorIs(t, err, os.ErrNotExist)
			require.FileExists(t, latestPath)
			if hasCompressedPeer {
				body, err := os.ReadFile(peerPath)
				require.NoError(t, err)
				require.Equal(t, peerContent, body)
			} else {
				_, err := os.Lstat(peerPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}
