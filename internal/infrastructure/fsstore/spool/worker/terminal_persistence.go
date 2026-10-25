package worker

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrTerminalSpoolConflict means a prior terminal file cannot be reused for this
// save. Preserve both the existing outcome and the processing source for review.
var ErrTerminalSpoolConflict = errors.New("terminal spool file conflicts with requested records")

// terminalSpoolFileMode is the Unix mode of terminal spool files. It keeps the
// owner-only 0600 used by os.CreateTemp so records are not exposed to others.
const terminalSpoolFileMode = 0o600

// writeSpoolFileAtPath keeps the processing file's relative name in a terminal
// state. Restrict the temporary file before writing records: Unix keeps the
// owner-only mode from os.CreateTemp, and Windows allows only the current user,
// SYSTEM and Administrators. Publish only after syncing the data and
// permissions, then sync the directory before allowing the caller to retire its
// source. A published file survives a directory-sync error; retrying verifies
// and syncs it without overwriting it.
func writeSpoolFileAtPath(rootDirectory, finalPath string, body []byte) error {
	if filepath.Ext(finalPath) != spooldir.SpoolExt {
		return fmt.Errorf("path is not a spool file: %s", finalPath)
	}
	dir := filepath.Dir(finalPath)
	if err := spooldir.EnsureDirectoryNoSymlinks(rootDirectory, dir); err != nil {
		return fmt.Errorf("failed to create destination dir: %w", err)
	}
	if found, err := reuseTerminalSpoolFile(finalPath, body); found || err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(finalPath)+".*"+tmpFileSuffix)
	if err != nil {
		return fmt.Errorf("failed to create temp spool file: %w", err)
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if err := infrafsstore.SetPrivateFilePermissions(f, terminalSpoolFileMode); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to set spool file permissions: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to write spool file body: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("%w: sync terminal spool file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close spool file: %w", err)
	}
	if err := infrafsstore.RenameFileNoReplace(tmpPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			if found, reuseErr := reuseTerminalSpoolFile(finalPath, body); found || reuseErr != nil {
				return reuseErr
			}
		}
		return fmt.Errorf("failed to publish terminal spool file: %w", err)
	}
	return infrafsstore.SyncDirectory(dir)
}

func reuseTerminalSpoolFile(path string, requestedBody []byte) (bool, error) {
	file, before, err := infrafsstore.OpenRegularFile(path, os.O_RDWR)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("inspect existing terminal spool file: %w", err)
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil {
		return true, fmt.Errorf("read existing terminal spool file: %w", err)
	}
	if !sameTerminalSpoolRecords(body, requestedBody) {
		return true, fmt.Errorf("%w: %s", ErrTerminalSpoolConflict, path)
	}
	if err := file.Sync(); err != nil {
		return true, fmt.Errorf("%w: sync existing terminal spool file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	if err := file.Close(); err != nil {
		return true, fmt.Errorf("close existing terminal spool file: %w", err)
	}
	after, err := os.Lstat(path)
	if err != nil {
		return true, err
	}
	if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return true, fmt.Errorf("%w: file changed while verifying %s", ErrTerminalSpoolConflict, path)
	}
	return true, infrafsstore.SyncDirectory(filepath.Dir(path))
}

func sameTerminalSpoolRecords(storedBody, requestedBody []byte) bool {
	stored, storedErr := decodeSpoolRecords(storedBody)
	requested, requestedErr := decodeSpoolRecords(requestedBody)
	if storedErr != nil || requestedErr != nil || len(stored) != len(requested) {
		return false
	}
	for i := range stored {
		if stored[i].FinalizedAt.IsZero() {
			return false
		}
		// Finalization time belongs to the first published result. A retry may
		// supply a new time, but must match every other field, including attempts
		// and failure reason; do not merge different outcomes at the same path.
		requested[i].FinalizedAt = stored[i].FinalizedAt
	}
	return sameStoredRecords(stored, requested)
}
