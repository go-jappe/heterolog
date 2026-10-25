package fsstore

import (
	"fmt"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// DirectorySyncError identifies a created directory's parent whose persistence
// is unconfirmed. EnsureDirectory and its variants record the parent for the
// rest of the process and retry its sync before preparing any directory at or
// below it, so a later attempt keeps returning this error, even when the created
// directory already exists, until the sync succeeds.
type DirectorySyncError struct {
	Directory string
	Err       error
}

func (e *DirectorySyncError) Error() string {
	return fmt.Sprintf("%v: sync newly created directory parent %q: %v",
		pkgfsstore.ErrDurabilityUnconfirmed, e.Directory, e.Err)
}

func (e *DirectorySyncError) Unwrap() []error {
	return []error{pkgfsstore.ErrDurabilityUnconfirmed, e.Err}
}
