package fsstore

import "errors"

// ErrDurabilityUnconfirmed reports that syncing the contents of a file or a
// directory could not be completed, so the durability of the stored result
// cannot be confirmed. It does not confirm that data was lost. errors.Is
// detects it in the reports and errors of archive processing. A handoff to a
// destination whose sync is unconfirmed is not treated as a success: the
// source file that the caller owns is kept and the handoff is retried. When
// the parent of a newly created directory could not be synced, the sync is
// retried each time the same process prepares a destination that depends on
// that parent, and until it succeeds this error is returned even if the
// destination has already been created.
var ErrDurabilityUnconfirmed = errors.New("filesystem durability could not be confirmed")
