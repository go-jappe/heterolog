package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreserveEnvironmentRestoresWindowsDriveEntry(t *testing.T) {
	const key = "=T:"
	original, originallySet := os.LookupEnv(key)
	t.Cleanup(func() {
		var err error
		if originallySet {
			err = os.Setenv(key, original)
		} else {
			err = os.Unsetenv(key)
		}
		if err != nil {
			t.Fatalf("restore original drive environment entry: %v", err)
		}
	})
	want := filepath.Join(t.TempDir(), "drive=value")
	if err := os.Setenv(key, want); err != nil {
		t.Fatalf("set drive environment fixture: %v", err)
	}

	t.Run("clear environment", func(t *testing.T) {
		PreserveEnvironment(t)
		os.Clearenv()
		if _, found := os.LookupEnv(key); found {
			t.Fatal("drive environment fixture was not cleared")
		}
	})
	if got, found := os.LookupEnv(key); !found || got != want {
		t.Fatal("drive environment entry was not restored")
	}
}
