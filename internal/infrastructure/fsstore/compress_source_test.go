package fsstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestOpenFilesForCompressionRejectsNonRegularSourceBeforeCreatingOutput(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"symlink", "dangling_symlink", "symlink_loop", "directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source.log")
			target := filepath.Join(t.TempDir(), "external.log")
			require.NoError(t, os.WriteFile(target, []byte("preserve external source"), 0o600))
			linkTarget := target
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(source, 0o700))
			case "dangling_symlink":
				linkTarget += ".missing"
			case "symlink_loop":
				linkTarget = source
			}
			if kind != "directory" {
				testutil.Symlink(t, linkTarget, source)
			}
			before, err := os.Lstat(source)
			require.NoError(t, err)
			for _, existingOutput := range []bool{false, true} {
				output := filepath.Join(dir, "output.gz")
				if existingOutput {
					require.NoError(t, os.WriteFile(output, []byte("preserve existing output"), 0o600))
				}
				in, out, err := OpenFilesForCompression(source, output)
				// 退行が起きた場合もWindowsでfixtureを削除できるよう、閉じておきます。
				if in != nil {
					require.NoError(t, in.Close())
				}
				if out != nil {
					require.NoError(t, out.Close())
				}
				require.Error(t, err, "non-regular source must be rejected before opening output")
				require.Nil(t, in)
				require.Nil(t, out)
				if existingOutput {
					body, readErr := os.ReadFile(output)
					require.NoError(t, readErr)
					require.Equal(t, "preserve existing output", string(body))
				} else {
					_, statErr := os.Lstat(output)
					require.ErrorIs(t, statErr, os.ErrNotExist)
				}
			}
			after, err := os.Lstat(source)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after))
			require.Equal(t, before.Mode(), after.Mode())
			if kind != "directory" {
				gotTarget, err := os.Readlink(source)
				require.NoError(t, err)
				require.Equal(t, linkTarget, gotTarget)
			}
			body, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, "preserve external source", string(body))
		})
	}
}
