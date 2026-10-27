package fsstore

import (
	"errors"
	"strings"
)

var (
	// ErrEmptyLogDirectory reports that the name of the subdirectory that stores
	// normal logs is empty or only white space.
	ErrEmptyLogDirectory = errors.New("log directory must not be empty")
	// ErrEmptyDebugLogDirectory reports that a separate debug directory is
	// enabled but no destination is set.
	ErrEmptyDebugLogDirectory = errors.New("debug log directory must not be empty")
)

// ValidateLogDirectory validates that LogDir, the name of the subdirectory that
// stores normal logs, is not empty or only whitespace, and returns
// ErrEmptyLogDirectory if it is. Callers should pass only the settings that
// file output actually uses, after applying defaults and setting inheritance.
// It does not check that the directory exists, create it, or inspect its
// permissions.
func ValidateLogDirectory(directory string) error {
	if strings.TrimSpace(directory) == "" {
		return ErrEmptyLogDirectory
	}
	return nil
}

// ValidateDebugLogDirectory validates that a destination is set when a
// separate debug directory is enabled, and returns ErrEmptyDebugLogDirectory if
// none is set. When the separate directory is disabled, it does not validate
// the destination, even if a previous destination remains.
func ValidateDebugLogDirectory(enabled bool, directory string) error {
	if enabled && strings.TrimSpace(directory) == "" {
		return ErrEmptyDebugLogDirectory
	}
	return nil
}
