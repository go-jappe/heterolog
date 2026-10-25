package fsstore

// ConflictRename records an existing file moved aside to keep a single
// canonical file name. The retained file is excluded from automatic replay
// and purge, so RetainedPath needs operator review. It is not the new save path.
type ConflictRename struct {
	OriginalPath string
	RetainedPath string
}
