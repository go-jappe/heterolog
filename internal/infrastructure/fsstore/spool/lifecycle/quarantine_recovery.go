package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// A split terminal write keeps its processing source's relative path. While
// that source exists, the saved result is evidence needed to suppress replay on
// restart. Preserve it even if retention runs before the recovery worker. A
// partial/conflicting source needs operator inspection and is protected too.
func terminalHasProcessingSource(stateDir, terminalPath string) (bool, error) {
	switch pkgspool.State(filepath.Base(stateDir)) {
	case pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine:
	default:
		return false, nil
	}
	rel, err := filepath.Rel(stateDir, terminalPath)
	if err != nil {
		return false, err
	}
	root := filepath.Dir(stateDir)
	parentRel := filepath.Join(string(pkgspool.StateProcessing), filepath.Dir(rel))
	if _, err := pathidentity.ResolveAndValidateManagedDirectory(root, parentRel); err != nil {
		return false, fmt.Errorf("verify pending terminal source directory: %w", err)
	}
	_, err = os.Lstat(filepath.Join(root, string(pkgspool.StateProcessing), rel))
	if errors.Is(err, os.ErrNotExist) {
		// A previous unlink may have succeeded without its directory sync. The
		// Runner purges before retrying cleanup, so confirm that absence before
		// deleting the terminal evidence which made the unlink safe.
		return false, syncAbsentProcessingParent(root, filepath.Join(root, parentRel))
	}
	if err != nil {
		return false, fmt.Errorf("verify pending terminal source: %w", err)
	}
	return true, nil
}

func syncAbsentProcessingParent(root, directory string) error {
	for {
		info, err := os.Lstat(directory)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("processing parent is not a directory: %s", directory)
			}
			return infrafsstore.SyncDirectory(directory)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if directory == filepath.Clean(root) {
			return nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return err
		}
		directory = parent
	}
}
