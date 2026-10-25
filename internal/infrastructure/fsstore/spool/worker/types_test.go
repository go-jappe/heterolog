package worker

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
)

// PathErrorは、表示にpathを加えても元のエラーをerrors.Is・errors.Asで辿れます。
func TestPathErrorKeepsOriginalErrorAsCause(t *testing.T) {
	original := &fs.PathError{Op: "open", Path: "spool/active/a.hlogspool", Err: fs.ErrPermission}
	for _, path := range []string{"", "spool/active/a.hlogspool"} {
		err := error(PathError{Path: path, Err: original})
		require.ErrorIs(t, err, fs.ErrPermission)
		var pathErr *fs.PathError
		require.True(t, errors.As(err, &pathErr), "PathError must expose its original error: %v", err)
		require.Same(t, original, pathErr)
	}
}
