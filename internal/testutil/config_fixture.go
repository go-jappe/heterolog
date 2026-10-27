package testutil

import (
	"github.com/go-jappe/heterolog/internal/testutil/configfixture"
	"path/filepath"
	"testing"
)

// ConfigFixtureRootは、.env.exampleから隔離されたofflineの設定を作成します。
// 開発者の個人用.env fileを開くことはありません。
func ConfigFixtureRoot(t testing.TB) string {
	t.Helper()
	source := filepath.Join(RepositoryRoot(t), ".env.example")
	root := filepath.Join(t.TempDir(), "config-fixture")
	if err := configfixture.Generate(source, root); err != nil {
		t.Fatal(err)
	}
	return root
}
