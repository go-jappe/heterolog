package spool

// FileCandidate is a regular spool file found by listing a state directory.
type FileCandidate struct {
	// Path is the local file system path for operations, which is passed to
	// reads, moves and removals.
	Path string

	// RelativePath is the path relative to the state directory. It uses the
	// OS-specific notation of filepath and is the boundary for resuming a scan
	// that compares the year/month/day/hour/minute directory names and the file
	// name lexicographically, in that order. It does not represent the Path for
	// operations, nor the existence or identity of the file.
	RelativePath string
}
