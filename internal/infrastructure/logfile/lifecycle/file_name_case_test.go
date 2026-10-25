package lifecycle

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// 未圧縮fileかどうかは、OS名ではなく保存先directoryの規則で判定します。
// 規則を指定して両方をどのOSでも確認し、未指定ならdirectoryの判定を使うことも確認します。
func TestFileSeriesManagerUncompressedExtensionFollowsDirectoryNameCase(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "case-extension"
	directory := filepath.Join(root, target, "logs")
	for _, rule := range []struct {
		name     string
		nameCase pathidentity.NameCase
	}{
		{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
		{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
		{name: "directory rule", nameCase: pathidentity.NameCaseUnspecified},
	} {
		t.Run(rule.name, func(t *testing.T) {
			pattern := logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log", NameCase: rule.nameCase}
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: pattern,
				Config:   testFileSeriesManagerConfig(root, target, 2, 0, 0, false),
			})
			foldsCase := rule.nameCase.ForDirectory(directory) == pathidentity.NameCaseInsensitive
			require.True(t, manager.hasUncompressedExtension(filepath.Join(directory, target+"_20260821_1.log")))
			// 大小文字を区別しないdirectoryでは、`.LOG`も同じ未圧縮fileの名前です。
			require.Equal(t, foldsCase, manager.hasUncompressedExtension(filepath.Join(directory, target+"_20260821_1.LOG")))
			require.False(t, manager.hasUncompressedExtension(filepath.Join(directory, target+"_20260821_1.log.gz")))
			require.False(t, manager.hasUncompressedExtension(filepath.Join(directory, target+"_20260821_1.LOG.GZ")))
			require.False(t, manager.hasUncompressedExtension(filepath.Join(directory, target+"_20260821_1.log.gz.tmp")))
		})
	}
}
