package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreKeepsPrivatePermissionsAfterReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "checkpoint")
	if err := infrafsstore.EnsureDirectory(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := NewFilesystemStore(directory, "private-source")
	if err != nil {
		t.Fatal(err)
	}
	key := CheckpointKey{Generation: "20260911-1", Compressed: true}
	destinations := []objectstorage.DestinationID{"primary", "secondary"}
	if _, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"previous"}); err != nil {
		t.Fatal(err)
	}
	testutil.RequirePrivatePermissions(t, directory, 0o700)
	testutil.RequirePrivatePermissions(t, store.documentPath(), fileMode)
	testutil.RequirePrivatePermissions(t, store.registrationsPath(), fileMode)
	if _, err := store.AdvanceCompletionCursor(ctx, key, destinations[:1]); err != nil {
		t.Fatal(err)
	}
	testutil.RequirePrivatePermissions(t, store.documentPath(), fileMode)
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 2 {
		t.Fatalf("checkpoint entries = %v, error = %v; want the document and initialization marker", entries, err)
	}
	restarted, err := NewFilesystemStore(directory, "private-source")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := restarted.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if pending := checkpoint.PendingDestinationIDs(destinations); len(pending) != 1 || pending[0] != "secondary" {
		t.Fatalf("pending destinations after replacement and restart = %v, want [secondary]", pending)
	}
}
