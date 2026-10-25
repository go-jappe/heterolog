package fsarchivehandler

import (
	"errors"
	"fmt"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

type archiveAppendRollback struct {
	file   *os.File
	offset int64
}

// recoverArchiveAppendsLocked runs before rotation can close or archive either
// active file. No new append is accepted while a failed request's bytes remain.
func (f *FsArchiveHandler) recoverArchiveAppendsLocked() error {
	return errors.Join(
		f.recoverArchiveAppendLocked(&f.retryableDirectory),
		f.recoverArchiveAppendLocked(&f.permanentDirectory),
	)
}

func (f *FsArchiveHandler) recoverArchiveAppendLocked(state *archiveDirectoryRuntime) error {
	pending := state.appendRollback
	if pending == nil {
		return nil
	}
	truncate := f.truncateArchiveAppend
	if truncate == nil {
		truncate = truncateFailedArchiveAppend
	}
	if err := truncate(pending.file, pending.offset); err != nil {
		return fmt.Errorf("restore failed archive append at %q to byte %d: %w", pending.file.Name(), pending.offset, err)
	}
	state.appendRollback = nil
	if state.directorySizeTracker != nil {
		state.directorySizeTracker.Invalidate()
	}
	return nil
}

func truncateFailedArchiveAppend(active *os.File, offset int64) (err error) {
	info, err := active.Stat()
	if err != nil {
		return err
	}
	// A dedicated non-append handle also permits truncation on Windows. Verify
	// identity before modifying it, so a replaced path cannot damage another file.
	file, openedInfo, err := fsstore.OpenRegularFile(active.Name(), os.O_WRONLY)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if !os.SameFile(info, openedInfo) {
		return errors.New("failure archive path no longer identifies the active file")
	}
	if openedInfo.Size() < offset {
		return errors.New("failure archive shrank before failed append rollback")
	}
	return file.Truncate(offset)
}
