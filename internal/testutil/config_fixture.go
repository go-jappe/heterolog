package testutil

import (
	"github.com/go-jappe/heterolog/internal/testutil/configfixture"
	"path/filepath"
	"testing"
)

// ConfigFixtureRoot creates isolated offline settings from .env.example.
// It never opens the developer's private .env file.
func ConfigFixtureRoot(t testing.TB) string {
	t.Helper()
	source := filepath.Join(RepositoryRoot(t), ".env.example")
	root := filepath.Join(t.TempDir(), "config-fixture")
	if err := configfixture.Generate(source, root); err != nil {
		t.Fatal(err)
	}
	return root
}
