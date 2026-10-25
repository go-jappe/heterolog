package appendlog

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestDirectoryStateCreatesSizeTrackerOnlyWhenRequested(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	key := pathidentity.ComparisonKey(dir)

	directoryStates.mu.Lock()
	state := directoryStates.byPath[key]
	if state == nil || state.guard != guard {
		directoryStates.mu.Unlock()
		t.Fatal("directory state does not contain the acquired guard")
	}
	if state.directorySizeTracker != nil {
		directoryStates.mu.Unlock()
		t.Fatal("directory size tracker was created by a guard-only request")
	}
	directoryStates.mu.Unlock()

	tracker := DirectorySizeTrackerForPath(dir)
	if tracker == nil {
		t.Fatal("DirectorySizeTrackerForPath() returned nil")
	}

	directoryStates.mu.Lock()
	defer directoryStates.mu.Unlock()
	if directoryStates.byPath[key].directorySizeTracker != tracker {
		t.Fatal("directory state does not retain the acquired size tracker")
	}
}

func TestDirectorySizeTrackerForPathNormalizesDirectoryPath(t *testing.T) {
	dir := t.TempDir()
	first := DirectorySizeTrackerForPath(dir)
	second := DirectorySizeTrackerForPath(filepath.Join(dir, "."))
	if first != second {
		t.Fatal("equivalent directory paths returned different size trackers")
	}
}
