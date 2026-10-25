package spool

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/spool"
)

var (
	// ErrInvalidStateDirectoryは、管理stateのpathがdirectoryでないか、symlinkであることを表します。
	ErrInvalidStateDirectory = errors.New("spool state path is not a directory or is a symlink")
	// ErrInvalidRootは、spool rootを実体のdirectoryとして使用できないことを表します。
	ErrInvalidRoot = errors.New("invalid spool root")
	// ErrDirectoryOutsideStateは、作成するdirectoryがspool rootの管理stateの配下にないことを表します。
	ErrDirectoryOutsideState = errors.New("spool write directory is outside managed states")
)

// StateDirectoryErrorは、管理stateのpathを保存先として使用できないことを示します。
type StateDirectoryError struct {
	State spool.State
	Path  string
}

func (e *StateDirectoryError) Error() string {
	return fmt.Sprintf("spool state path %q is not a directory or is a symlink", e.Path)
}

func (e *StateDirectoryError) Unwrap() error {
	return ErrInvalidStateDirectory
}

// RootErrorは、Rootをspool rootとして使用できないことを示します。Linkはrootまたはその祖先が
// linkであることを表します。Errはrootを解決できなかった原因で、Errorはその文言をそのまま返します。
type RootError struct {
	Root string
	Link bool
	Err  error
}

func (e *RootError) Error() string {
	if e.Link || e.Err == nil {
		return fmt.Sprintf("spool root %q or its ancestor is a link", e.Root)
	}
	return e.Err.Error()
}

func (e *RootError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrInvalidRoot}
	}
	return []error{ErrInvalidRoot, e.Err}
}

// DirectoryRangeErrorは、Directoryがspool rootの子でないか、管理stateの配下でないことを示します。
type DirectoryRangeError struct {
	Directory string
	Root      string
	// OutsideRootは、Directoryがrootの子でないことを表します。falseなら、rootの子ですが
	// 管理stateの配下ではありません。
	OutsideRoot bool
}

func (e *DirectoryRangeError) Error() string {
	if e.OutsideRoot {
		return fmt.Sprintf("spool write directory %q must be a child of root %q", e.Directory, e.Root)
	}
	return fmt.Sprintf("spool write directory %q must belong to a managed state", e.Directory)
}

func (e *DirectoryRangeError) Unwrap() error {
	return ErrDirectoryOutsideState
}
