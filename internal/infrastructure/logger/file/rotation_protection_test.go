package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestPathsToProtectForRotatedFileOnlyIncludesCompressedFilesInInlineMode(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{
		pkgfsstore.FileLifecycleModeInline,
		pkgfsstore.FileLifecycleModeRunner,
		pkgfsstore.FileLifecycleModeDisable,
	} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "rotation-protection", "rotation-protection")
			cfg.LifecycleMode = mode
			cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
			cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
			logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

			fileName := logger.Fsstore.FileName
			rawPath := filepath.Join(fileName.Dir, fileName.Prefix+fileName.Target+"_20260909_1.log")
			zstdPath := rawPath + ".zst"
			require.NoError(t, os.WriteFile(zstdPath, []byte("existing zstd"), 0o600))

			paths := logger.pathsToProtectForRotatedFile(rawPath)
			if mode == pkgfsstore.FileLifecycleModeInline {
				require.ElementsMatch(t, []string{rawPath, zstdPath}, paths)
			} else {
				// inline taskを起動しないmodeは、既存の圧縮fileがあっても
				// その一覧を取得せず、writer切り替え中のrawだけを保護します。
				require.Equal(t, []string{rawPath}, paths)
			}
		})
	}
}
