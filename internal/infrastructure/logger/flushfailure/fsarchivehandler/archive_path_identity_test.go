package fsarchivehandler

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"

	"github.com/stretchr/testify/require"
)

func TestFailureArchiveGroupsPathIdentityKeepsPublishedOriginalPath(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows case-insensitive path identity")
	}
	root := t.TempDir()
	raw := filepath.Join(root, "Logs", "App_20260911_1.jsonl")
	published := filepath.Join(root, "LOGS", "App_20260911_1.jsonl.gz")
	groups, err := groupFailureArchiveUploadPaths(logpattern.FileNamePattern{
		Dir: filepath.Dir(raw), Target: "App", Extension: "jsonl",
	}, []string{published})
	require.NoError(t, err)
	require.Len(t, groups, 1)

	// 世代の解決でも、実在file名やremote keyの表記を変更しません。
	require.Equal(t, published, groups[0].path)
}
