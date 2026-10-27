package fsstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestOpenFilesForCompressionRestrictsOutputBeforeWriting(t *testing.T) {
	for _, sourceMode := range []os.FileMode{0o600, 0o640} {
		for _, outputMode := range []os.FileMode{0, 0o644, 0o600} {
			t.Run(fmt.Sprintf("source_%o/output_%o", sourceMode, outputMode), func(t *testing.T) {
				directory := t.TempDir()
				source, output := filepath.Join(directory, "source.log"), filepath.Join(directory, "output.tmp")
				require.NoError(t, os.WriteFile(source, []byte("original private log"), 0o600))
				require.NoError(t, os.Chmod(source, sourceMode))
				var expectedMode os.FileMode
				if outputMode == 0 {
					// processのumaskを変更せず、並行する試験にも影響を与えずに観測します。
					// 作成時に加わる追加の制限は残らなければなりません。
					control := filepath.Join(directory, "creation-mode-control")
					require.NoError(t, os.WriteFile(control, nil, sourceMode))
					info, err := os.Stat(control)
					require.NoError(t, err)
					expectedMode = sourceMode & info.Mode().Perm()
				} else {
					require.NoError(t, os.WriteFile(output, []byte("stale temporary contents"), 0o600))
					require.NoError(t, os.Chmod(output, outputMode))
					expectedMode = sourceMode & outputMode
				}
				in, out, err := OpenFilesForCompression(source, output)
				require.NoError(t, err)
				t.Cleanup(func() { _ = in.Close(); _ = out.Close() })
				// codecはまだどちらのhandleも受け取っていません。古く権限の広い一時fileでも、
				// 最初の1 byteより前に制限済みでなければなりません。
				testutil.RequirePrivatePermissions(t, output, expectedMode)
				info, err := out.Stat()
				require.NoError(t, err)
				require.Zero(t, info.Size())
				_, err = out.WriteString("replacement compressed bytes")
				require.NoError(t, err)
				require.NoError(t, out.Sync())
				require.NoError(t, out.Close())
				require.NoError(t, in.Close())
				published := filepath.Join(directory, "published.gz")
				require.NoError(t, os.Rename(output, published))
				testutil.RequirePrivatePermissions(t, published, expectedMode)
				body, err := os.ReadFile(published)
				require.NoError(t, err)
				require.Equal(t, "replacement compressed bytes", string(body))
				body, err = os.ReadFile(source)
				require.NoError(t, err)
				require.Equal(t, "original private log", string(body))
			})
		}
	}
}

func TestOpenFilesForCompressionDoesNotChangeSourceOrLinkedOutputPermissions(t *testing.T) {
	for _, kind := range []string{"source itself", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			source, target := filepath.Join(directory, "source.log"), filepath.Join(directory, "external.log")
			require.NoError(t, os.WriteFile(source, []byte("private source"), 0o600))
			require.NoError(t, os.WriteFile(target, []byte("external contents"), 0o640))
			output := source
			if kind == "symlink" {
				output = filepath.Join(directory, "output.tmp")
				testutil.Symlink(t, target, output)
			}
			before, err := os.Stat(target)
			require.NoError(t, err)
			in, out, err := OpenFilesForCompression(source, output)
			if in != nil {
				_ = in.Close()
			}
			if out != nil {
				_ = out.Close()
			}
			require.Error(t, err)
			require.Nil(t, in)
			require.Nil(t, out)
			body, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, "private source", string(body))
			body, err = os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, "external contents", string(body))
			after, err := os.Stat(target)
			require.NoError(t, err)
			require.Equal(t, before.Mode(), after.Mode())
		})
	}
}
