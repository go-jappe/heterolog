package config

import "fmt"

// StorageDirectoryCollisionErrorは、二つの所有単位の保存先directoryが同じか、一方が他方を
// 内包することを表します。Sourceは"logger.file.default"や
// "application.reserved_directory.archive_checkpoint"などの所有単位の名前、Pathは比較に
// 使った解決済みのpathです。複数の衝突はerrors.Joinでまとめて返します。
type StorageDirectoryCollisionError struct {
	LeftSource  string
	LeftPath    string
	RightSource string
	RightPath   string
}

func newStorageDirectoryCollisionError(left, right storageDirectoryClaim) error {
	return &StorageDirectoryCollisionError{
		LeftSource:  left.source,
		LeftPath:    left.path,
		RightSource: right.source,
		RightPath:   right.path,
	}
}

func (e *StorageDirectoryCollisionError) Error() string {
	return fmt.Sprintf(
		"storage directory collision: %s %q overlaps %s %q",
		e.LeftSource,
		e.LeftPath,
		e.RightSource,
		e.RightPath,
	)
}

// StorageDirectoryResolutionErrorは、保存先directoryを、それを使うstoreと同じ規則で
// 解決できないことを表します。Directoryは"logger.file.default"や"archive checkpoint"、
// "logger.loki failure archive retryable"など、解決しようとした保存先の名前です。
type StorageDirectoryResolutionError struct {
	Directory string
	// Errは解決に失敗した原因です。
	Err error
}

func (e *StorageDirectoryResolutionError) Error() string {
	return fmt.Sprintf("resolve %s directory: %v", e.Directory, e.Err)
}

func (e *StorageDirectoryResolutionError) Unwrap() error { return e.Err }

// SharedRoleLogDirectoryErrorは、所有単位Sourceのerrorとdebugのログが、defaultのログとは別の
// Directoryを共有することを表します。DefaultDirectoryはdefaultのログの保存先です。
type SharedRoleLogDirectoryError struct {
	Source           string
	Directory        string
	DefaultDirectory string
}

func (e *SharedRoleLogDirectoryError) Error() string {
	return fmt.Sprintf(
		"%s: error and debug directories %q must not be shared without the default log directory %q",
		e.Source,
		e.Directory,
		e.DefaultDirectory,
	)
}

// LogFileLayoutResolutionErrorは、内部ログ専用のfile出力と通常のfile出力が同じ保存先かを
// 比べるために、Layoutのfile layoutを解決できないことを表します。Layoutは"base"または
// "internal log"です。
type LogFileLayoutResolutionError struct {
	Layout string
	// Errは解決に失敗した原因です。
	Err error
}

func (e *LogFileLayoutResolutionError) Error() string {
	return fmt.Sprintf("resolve %s file layout: %v", e.Layout, e.Err)
}

func (e *LogFileLayoutResolutionError) Unwrap() error { return e.Err }
