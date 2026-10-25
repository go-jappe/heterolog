package directorylease

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestRegistryAcquireAllowsDistinctDirectories(t *testing.T) {
	registry := newRegistry()
	first, err := registry.acquire("logger.file", filepath.Join(t.TempDir(), "first"))
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	defer first.Release()

	second, err := registry.acquire("logger.file", filepath.Join(t.TempDir(), "second"))
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	defer second.Release()

	if first.ID() == 0 || second.ID() == 0 {
		t.Fatalf("lease IDs = %d and %d, want non-zero", first.ID(), second.ID())
	}
	if first.ID() == second.ID() {
		t.Fatalf("lease IDs = %d and %d, want different IDs", first.ID(), second.ID())
	}
	if got := activeLeaseCount(registry); got != 2 {
		t.Fatalf("active lease count = %d, want 2", got)
	}
}

func TestRegistryAcquireRejectsNormalizedExactConflict(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	firstPath := filepath.Join(root, "logs")
	first, err := registry.acquire("logger.file", firstPath)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	defer first.Release()

	// Windowsではpackageの作業directoryと一時directoryが別のdriveにある場合があるため、
	// rootと同じvolumeの一時directoryを作業directoryにして相対pathを作ります。
	workingDirectory := t.TempDir()
	t.Chdir(workingDirectory)
	relativePath, err := filepath.Rel(workingDirectory, filepath.Join(root, "child", "..", "logs"))
	if err != nil {
		t.Fatalf("resolve relative path: %v", err)
	}
	requestedPath := relativePath
	_, err = registry.acquire("logger.file", requestedPath)
	conflict := assertConflict(t, err)
	assertConflictValues(
		t,
		conflict,
		"logger.file",
		2,
		requestedPath,
		"logger.file",
		first.ID(),
		firstPath,
	)

	if got := activeLeaseCount(registry); got != 1 {
		t.Fatalf("active lease count after conflict = %d, want 1", got)
	}
}

func TestRegistryAcquireResolvesSymlinksBeforeConflictCheck(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	target := filepath.Join(root, "target")
	alias := filepath.Join(root, "alias")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, target, alias)

	targetLease, err := registry.acquire("logger.target", target)
	if err != nil {
		t.Fatalf("acquire symlink target: %v", err)
	}
	defer targetLease.Release()

	_, err = registry.acquire("logger.alias", alias)
	conflict := assertConflict(t, err)
	assertConflictValues(t, conflict, "logger.alias", 2, alias, "logger.target", targetLease.ID(), target)
}

func TestRegistryAcquireDeduplicatesSymlinkAliasesWithMissingSuffix(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	target := filepath.Join(root, "target")
	alias := filepath.Join(root, "alias")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, target, alias)
	requested := filepath.Join(alias, "missing", "logs")
	physical := filepath.Join(target, "missing", "logs")
	lease, err := registry.acquire("logger.alias", requested, physical)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	registered := onlyRegistration(t, registry)
	if len(registered.paths) != 1 || registered.paths[0] != canonicalPath(t, physical) {
		t.Fatalf("registered paths = %v, want resolved physical path only", registered.paths)
	}
	if _, err := os.Lstat(filepath.Join(target, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acquire created missing suffix: %v", err)
	}
	_, err = registry.acquire("logger.child", filepath.Join(physical, "child"))
	assertConflict(t, err)
}

func TestRegistryAcquireRejectsDanglingSymlinkWithoutRegistration(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	alias := filepath.Join(root, "alias")
	testutil.Symlink(t, filepath.Join(root, "missing"), alias)
	if _, err := registry.acquire("logger.alias", filepath.Join(alias, "logs")); err == nil {
		t.Fatal("acquire dangling symlink succeeded")
	}
	if got := activeLeaseCount(registry); got != 0 {
		t.Fatalf("active lease count = %d, want 0", got)
	}
	lease, err := registry.acquire("logger.valid", filepath.Join(root, "logs"))
	if err != nil {
		t.Fatalf("valid acquire after resolution failure: %v", err)
	}
	defer lease.Release()
}

func TestRegistryAcquireRejectsParentChildConflict(t *testing.T) {
	tests := []struct {
		name          string
		existingPath  func(string) string
		requestedPath func(string) string
	}{
		{
			name:          "requested child",
			existingPath:  func(root string) string { return root },
			requestedPath: func(root string) string { return filepath.Join(root, "nested") },
		},
		{
			name:          "requested parent",
			existingPath:  func(root string) string { return filepath.Join(root, "nested") },
			requestedPath: func(root string) string { return root },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := newRegistry()
			root := t.TempDir()
			existingPath := tt.existingPath(root)
			lease, err := registry.acquire("logger.first", existingPath)
			if err != nil {
				t.Fatalf("acquire first lease: %v", err)
			}
			defer lease.Release()

			requestedPath := tt.requestedPath(root)
			_, err = registry.acquire("logger.second", requestedPath)
			conflict := assertConflict(t, err)
			assertConflictValues(
				t,
				conflict,
				"logger.second",
				2,
				requestedPath,
				"logger.first",
				lease.ID(),
				existingPath,
			)
		})
	}
}

func TestRegistryAcquireDeduplicatesExactPathsWithinRequest(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	lease, err := registry.acquire(
		"logger.file",
		root,
		filepath.Join(root, "."),
		root+string(filepath.Separator)+".",
	)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer lease.Release()

	registration := onlyRegistration(t, registry)
	if got := len(registration.paths); got != 1 {
		t.Fatalf("registered path count = %d, want 1", got)
	}
}

func TestRegistryAcquireRejectsParentChildWithinSameRequest(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	child := filepath.Join(root, "nested")

	_, err := registry.acquire("logger.file", root, child)
	if !errors.Is(err, ErrDirectoryLeaseConflict) {
		t.Fatalf("error = %v, want ErrDirectoryLeaseConflict", err)
	}
	var overlap *RequestOverlapError
	if !errors.As(err, &overlap) {
		t.Fatalf("error type = %T, want *RequestOverlapError", err)
	}
	var conflict *ConflictError
	if errors.As(err, &conflict) {
		t.Fatalf("self-overlap error type = %T, must not be *ConflictError", err)
	}
	wantRoot := canonicalPath(t, root)
	wantChild := canonicalPath(t, child)
	if overlap.ScopeLabel != "logger.file" ||
		overlap.AttemptID != 1 ||
		overlap.FirstPath != wantRoot ||
		overlap.SecondPath != wantChild {
		t.Fatalf("overlap = %#v, want scope/attempt/paths %q/%d/%q/%q",
			overlap,
			ScopeLabel("logger.file"),
			AcquisitionID(1),
			wantRoot,
			wantChild,
		)
	}
	if got := activeLeaseCount(registry); got != 0 {
		t.Fatalf("active lease count = %d, want 0", got)
	}

	// 正しい取得要求であったため、登録されなくてもattempt IDは消費します。
	lease, err := registry.acquire("logger.file", filepath.Join(root, "sibling"))
	if err != nil {
		t.Fatalf("acquire after self-overlap: %v", err)
	}
	defer lease.Release()
	if lease.ID() != 2 {
		t.Fatalf("lease ID after self-overlap = %d, want 2", lease.ID())
	}
}

func TestRegistryAcquireIsAtomicWhenOnePathConflicts(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	occupied := filepath.Join(root, "occupied")
	first, err := registry.acquire("logger.first", occupied)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	defer first.Release()

	free := filepath.Join(root, "free")
	_, err = registry.acquire("logger.second", free, occupied)
	if !errors.Is(err, ErrDirectoryLeaseConflict) {
		t.Fatalf("acquire conflicting lease error = %v, want ErrDirectoryLeaseConflict", err)
	}

	freeLease, err := registry.acquire("logger.third", free)
	if err != nil {
		t.Fatalf("acquire path from rejected request: %v", err)
	}
	defer freeLease.Release()
	if freeLease.ID() != 3 {
		t.Fatalf("lease ID after rejected attempt = %d, want 3", freeLease.ID())
	}
}

func TestRegistryAcquireReportsOldestOverlappingLeaseDeterministically(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	oldestPath := filepath.Join(root, "error")
	newerPath := filepath.Join(root, "debug")

	oldest, err := registry.acquire("logger.file", oldestPath)
	if err != nil {
		t.Fatalf("acquire oldest lease: %v", err)
	}
	defer oldest.Release()
	newer, err := registry.acquire("async_logger.file", newerPath)
	if err != nil {
		t.Fatalf("acquire newer lease: %v", err)
	}
	defer newer.Release()

	for attempt := AcquisitionID(3); attempt < 35; attempt++ {
		_, err = registry.acquire("delivery_logger.file", root)
		conflict := assertConflict(t, err)
		assertConflictValues(
			t,
			conflict,
			"delivery_logger.file",
			attempt,
			root,
			"logger.file",
			oldest.ID(),
			oldestPath,
		)
	}
}

func TestRegistryFailedAttemptsUseUniqueIDsAndKeepExistingLeaseID(t *testing.T) {
	registry := newRegistry()
	path := t.TempDir()
	existing, err := registry.acquire("logger.file", path)
	if err != nil {
		t.Fatalf("acquire existing lease: %v", err)
	}
	defer existing.Release()

	seenAttempts := make(map[AcquisitionID]struct{})
	for wantAttempt := AcquisitionID(2); wantAttempt <= 10; wantAttempt++ {
		_, err := registry.acquire("logger.file", path)
		conflict := assertConflict(t, err)
		if conflict.RequestedAttemptID != wantAttempt {
			t.Fatalf("requested attempt ID = %d, want %d", conflict.RequestedAttemptID, wantAttempt)
		}
		if conflict.ExistingLeaseID != existing.ID() {
			t.Fatalf("existing lease ID = %d, want %d", conflict.ExistingLeaseID, existing.ID())
		}
		if _, duplicated := seenAttempts[conflict.RequestedAttemptID]; duplicated {
			t.Fatalf("duplicate requested attempt ID %d", conflict.RequestedAttemptID)
		}
		seenAttempts[conflict.RequestedAttemptID] = struct{}{}
	}
}

func TestLeaseReleaseIsIdempotentAndReacquireUsesNewID(t *testing.T) {
	registry := newRegistry()
	path := t.TempDir()
	lease, err := registry.acquire("logger.file", path)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	firstID := lease.ID()

	lease.Release()
	lease.Release()
	var nilLease *Lease
	nilLease.Release()
	if nilLease.ID() != 0 {
		t.Fatalf("nil lease ID = %d, want 0", nilLease.ID())
	}

	replacement, err := registry.acquire("logger.file", path)
	if err != nil {
		t.Fatalf("reacquire released path: %v", err)
	}
	defer replacement.Release()
	if replacement.ID() == firstID {
		t.Fatalf("replacement lease ID = %d, must differ from released lease ID", replacement.ID())
	}

	// 古いLeaseを再度解放しても、pointer tokenが異なる新しいLeaseを解放してはいけません。
	lease.Release()
	if got := activeLeaseCount(registry); got != 1 {
		t.Fatalf("active lease count after stale release = %d, want 1", got)
	}
	_, err = registry.acquire("logger.file", path)
	conflict := assertConflict(t, err)
	if conflict.ExistingLeaseID != replacement.ID() {
		t.Fatalf("existing lease ID = %d, want replacement ID %d", conflict.ExistingLeaseID, replacement.ID())
	}
}

func TestRegistryConcurrentAcquireHasSingleWinnerAndUniqueAttemptIDs(t *testing.T) {
	registry := newRegistry()
	path := t.TempDir()
	const contenders = 32

	type acquireResult struct {
		lease *Lease
		err   error
	}
	start := make(chan struct{})
	results := make(chan acquireResult, contenders)
	var wg sync.WaitGroup
	for index := 0; index < contenders; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := registry.acquire("logger.file", path)
			results <- acquireResult{lease: lease, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var winner *Lease
	conflicts := make([]*ConflictError, 0, contenders-1)
	for result := range results {
		if result.err == nil {
			if winner != nil {
				t.Fatalf("multiple successful leases: %d and %d", winner.ID(), result.lease.ID())
			}
			winner = result.lease
			continue
		}
		conflicts = append(conflicts, assertConflict(t, result.err))
	}
	if winner == nil {
		t.Fatal("no acquire winner")
	}
	defer winner.Release()
	if winner.ID() != 1 {
		t.Fatalf("winner lease ID = %d, want 1", winner.ID())
	}
	if len(conflicts) != contenders-1 {
		t.Fatalf("conflict count = %d, want %d", len(conflicts), contenders-1)
	}

	allAttemptIDs := map[AcquisitionID]struct{}{winner.ID(): {}}
	for _, conflict := range conflicts {
		if conflict.ExistingLeaseID != winner.ID() {
			t.Fatalf("existing lease ID = %d, want winner ID %d", conflict.ExistingLeaseID, winner.ID())
		}
		if _, duplicated := allAttemptIDs[conflict.RequestedAttemptID]; duplicated {
			t.Fatalf("duplicate concurrent attempt ID %d", conflict.RequestedAttemptID)
		}
		allAttemptIDs[conflict.RequestedAttemptID] = struct{}{}
	}
	if len(allAttemptIDs) != contenders {
		t.Fatalf("unique attempt ID count = %d, want %d", len(allAttemptIDs), contenders)
	}
	for id := AcquisitionID(1); id <= contenders; id++ {
		if _, exists := allAttemptIDs[id]; !exists {
			t.Fatalf("attempt ID %d is missing", id)
		}
	}
}

func TestRegistryInvalidInputDoesNotConsumeAcquisitionID(t *testing.T) {
	registry := newRegistry()
	if _, err := registry.acquire("  ", t.TempDir()); !errors.Is(err, ErrScopeLabelRequired) {
		t.Fatalf("empty scope label error = %v, want ErrScopeLabelRequired", err)
	}
	if _, err := registry.acquire("logger.file"); !errors.Is(err, ErrDirectoryPathRequired) {
		t.Fatalf("missing path error = %v, want ErrDirectoryPathRequired", err)
	}
	if _, err := registry.acquire("logger.file", "  "); !errors.Is(err, ErrDirectoryPathRequired) {
		t.Fatalf("empty path error = %v, want ErrDirectoryPathRequired", err)
	}
	if got := activeLeaseCount(registry); got != 0 {
		t.Fatalf("active lease count after invalid input = %d, want 0", got)
	}

	lease, err := registry.acquire(" logger.file ", t.TempDir())
	if err != nil {
		t.Fatalf("acquire after invalid inputs: %v", err)
	}
	defer lease.Release()
	if lease.ID() != 1 {
		t.Fatalf("first valid lease ID = %d, want 1", lease.ID())
	}
	if got := onlyRegistration(t, registry).scopeLabel; got != "logger.file" {
		t.Fatalf("normalized scope label = %q, want %q", got, ScopeLabel("logger.file"))
	}
}

func assertConflict(t *testing.T, err error) *ConflictError {
	t.Helper()
	if !errors.Is(err, ErrDirectoryLeaseConflict) {
		t.Fatalf("error = %v, want ErrDirectoryLeaseConflict", err)
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error type = %T, want *ConflictError", err)
	}
	return conflict
}

func assertConflictValues(
	t *testing.T,
	conflict *ConflictError,
	requestedScopeLabel ScopeLabel,
	requestedAttemptID AcquisitionID,
	requestedPath string,
	existingScopeLabel ScopeLabel,
	existingLeaseID AcquisitionID,
	existingPath string,
) {
	t.Helper()
	wantRequestedPath := canonicalPath(t, requestedPath)
	wantExistingPath := canonicalPath(t, existingPath)
	if conflict.RequestedScopeLabel != requestedScopeLabel ||
		conflict.RequestedAttemptID != requestedAttemptID ||
		conflict.RequestedPath != wantRequestedPath ||
		conflict.ExistingScopeLabel != existingScopeLabel ||
		conflict.ExistingLeaseID != existingLeaseID ||
		conflict.ExistingPath != wantExistingPath {
		t.Fatalf(
			"conflict = %#v, want requested scope/attempt/path %q/%d/%q and existing scope/lease/path %q/%d/%q",
			conflict,
			requestedScopeLabel,
			requestedAttemptID,
			wantRequestedPath,
			existingScopeLabel,
			existingLeaseID,
			wantExistingPath,
		)
	}
}

func canonicalPath(t *testing.T, path string) string {
	t.Helper()
	absolute, err := pathidentity.ResolveDirectory(path)
	if err != nil {
		t.Fatalf("resolve path %q: %v", path, err)
	}
	return filepath.Clean(absolute)
}

func activeLeaseCount(registry *registry) int {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return len(registry.activeRegistrations)
}

func onlyRegistration(t *testing.T, registry *registry) registration {
	t.Helper()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.activeRegistrations) != 1 {
		t.Fatalf("active lease count = %d, want 1", len(registry.activeRegistrations))
	}
	for _, registered := range registry.activeRegistrations {
		return registered
	}
	return registration{}
}
