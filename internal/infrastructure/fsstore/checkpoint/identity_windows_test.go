package checkpoint

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestArchiveCheckpointSourceIDUnifiesWindowsExtendedPaths(t *testing.T) {
	t.Parallel()
	for _, directory := range []string{`C:\Logs\App`, `\\server\share\Logs\App`} {
		pattern := logpattern.FileNamePattern{
			Dir: directory, Prefix: "Error_", Target: "App", Extension: ".Log",
		}
		alias := pattern
		if strings.HasPrefix(directory, `\\`) {
			alias.Dir = `\\?\UNC\` + strings.TrimPrefix(directory, `\\`)
		} else {
			alias.Dir = `\\?\` + directory
		}
		alias.Dir = strings.ToUpper(alias.Dir)
		want := ArchiveCheckpointSourceID("logger_file", "error", pattern)
		if got := ArchiveCheckpointSourceID("logger_file", "error", alias); got != want {
			t.Fatalf("extended alias %q has a different source ID from %q", alias.Dir, directory)
		}
	}
}

func TestArchiveCheckpointSourceIDPreservesWindowsUploadProgressAfterAliasRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "Checkpoint")
	pattern := logpattern.FileNamePattern{
		Dir: filepath.Join(t.TempDir(), "Logs"), Prefix: "Error_", Target: "App", Extension: "Log",
	}
	key := CheckpointKey{Generation: "20260911-1", Compressed: true}
	destinations := []objectstorage.DestinationID{"primary", "secondary"}
	sourceID := ArchiveCheckpointSourceID("logger_file", "error", pattern)
	if err := PrepareDirectory(directory); err != nil {
		t.Fatal(err)
	}
	store, err := NewFilesystemStore(directory, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceCompletionCursor(ctx, key, destinations[:1]); err != nil {
		t.Fatal(err)
	}

	alias := pattern
	alias.Dir = `\\?\` + strings.ToUpper(pattern.Dir)
	alias.Prefix = strings.ToLower(pattern.Prefix)
	alias.Target = strings.ToLower(pattern.Target)
	alias.Extension = strings.ToLower(pattern.Extension)
	aliasID := ArchiveCheckpointSourceID("logger_file", "error", alias)
	restarted, err := NewFilesystemStore(directory, aliasID)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := restarted.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if pending := checkpoint.PendingDestinationIDs(destinations); len(pending) != 1 || pending[0] != "secondary" {
		t.Fatalf("restart lost upload progress: pending destinations = %v", pending)
	}
	if aliasID != sourceID {
		t.Fatalf("source identity changed across Windows alias: got %q, want %q", aliasID, sourceID)
	}
}
