package pathidentity

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ComparisonKey・DirectoryNameCase・Containsは保存先の走査ごとに多数のpathへ使います。
// 既存の親directoryと未作成の子を持つpathで、filesystemの問い合わせを含む1回あたりの費用を測ります。
// 一時directoryのfilesystemで測るため、TMPDIRで対象のfilesystemを選べます。
func BenchmarkComparisonKey(b *testing.B) {
	existing := benchmarkExistingDirectory(b)
	for _, bench := range []struct{ name, path string }{
		{name: "existing parent", path: filepath.Join(existing, "App_20260914_1.log")},
		{name: "missing parents", path: filepath.Join(existing, "Missing", "Logs", "App_20260914_1.log")},
	} {
		b.Run(bench.name, func(b *testing.B) {
			for b.Loop() {
				ComparisonKey(bench.path)
			}
		})
	}
}

func BenchmarkDirectoryNameCase(b *testing.B) {
	existing := benchmarkExistingDirectory(b)
	for b.Loop() {
		DirectoryNameCase(existing)
	}
}

// 除外集合にないpathの照合は、走査で最も多い場合です。
func BenchmarkContainsMiss(b *testing.B) {
	existing := benchmarkExistingDirectory(b)
	paths := make(map[string]struct{})
	for index := range 8 {
		paths[filepath.Join(existing, fmt.Sprintf("App_20260914_%d.log", index+1))] = struct{}{}
	}
	missing := filepath.Join(existing, "App_20260914_9.log")
	for b.Loop() {
		if Contains(paths, missing) {
			b.Fatal("missing path matched")
		}
	}
}

func benchmarkExistingDirectory(b *testing.B) string {
	b.Helper()
	existing := filepath.Join(b.TempDir(), "Logs", "App")
	if err := os.MkdirAll(existing, 0o700); err != nil {
		b.Fatal(err)
	}
	return existing
}
