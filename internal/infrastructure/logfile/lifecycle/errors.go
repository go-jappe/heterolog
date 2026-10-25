package lifecycle

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

var (
	// ErrRoleDirectoryConflictは、errorとdebugのログが同じdirectoryを使い、defaultだけが
	// 別のdirectoryを使う配置です。lifecycleは物理directoryを一つの単位として扱うため受け付けません。
	ErrRoleDirectoryConflict = errors.New("appendlog lifecycle roles cannot share a directory that another role does not use")
	// ErrInvalidFileNameConfigは、設定のtarget・prefix・file名形式・日付形式がlog file名に
	// 使えない値であることを表します。原因の検証errorもerrors.Is/Asで判定できます。
	ErrInvalidFileNameConfig = errors.New("appendlog lifecycle file name config")
	// ErrIncompleteCheckpointConfigurationは、checkpointの保存先とsource prefixの一方だけを
	// 指定した設定です。ErrInvalidCheckpointConfigurationと併せて返します。
	ErrIncompleteCheckpointConfiguration = errors.New("checkpoint directory and source prefix must both be configured")
	// ErrUploadFailureArchiveLifecycleは、送信失敗archiveの独立した保守周期が失敗したことを表します。
	ErrUploadFailureArchiveLifecycle = errors.New("compressed log file upload failure archive lifecycle failed")
	// ErrUploadFailureSpoolMaintenanceは、送信失敗spoolの再送・整理が失敗したことを表します。
	ErrUploadFailureSpoolMaintenance = errors.New("compressed log file upload failure spool maintenance failed")
	// ErrExistingUploadFailureFileCheckは、送信前に退避済みファイルを確認できなかったことを表します。
	// 元の圧縮ファイルは残し、送信を後続の周期へ保留します。
	ErrExistingUploadFailureFileCheck = errors.New("check existing failure files before upload")
	// ErrUploadProgressSaveは、送信・退避の途中経過をcheckpointへ保存できなかったことを表します。
	ErrUploadProgressSave = errors.New("save upload progress")
	// ErrUploadProgressPreservedは、保存結果の矛盾に備えて元の圧縮ファイルを.preservedへ保全したことを
	// 表す警告です。送信は継続できます。
	ErrUploadProgressPreserved = errors.New("preserved conflicting upload progress")
)

// RoleDirectoryConflictErrorは、ErrRoleDirectoryConflictの詳細です。SharedRolesの二つが
// SharedDirectoryを共有し、OtherRoleだけがOtherDirectoryを使います。
type RoleDirectoryConflictError struct {
	SharedRoles     [2]string
	SharedDirectory string
	OtherRole       string
	OtherDirectory  string
}

func (e *RoleDirectoryConflictError) Error() string {
	return fmt.Sprintf(
		"appendlog lifecycle roles %q and %q cannot share directory %q while role %q uses a different directory %q",
		e.SharedRoles[0], e.SharedRoles[1], e.SharedDirectory, e.OtherRole, e.OtherDirectory,
	)
}

func (e *RoleDirectoryConflictError) Is(target error) bool { return target == ErrRoleDirectoryConflict }

// FailureArchiveUploaderErrorは、failure archiveの分類ごとのobject storage uploaderを
// 作れなかったことを表します。Errは設定の検証や構築の原因です。
type FailureArchiveUploaderError struct {
	FailureCategory failurearchive.FailureCategory
	Err             error
}

func (e *FailureArchiveUploaderError) Error() string {
	return fmt.Sprintf("create %s failure archive uploader: %v", e.FailureCategory, e.Err)
}

func (e *FailureArchiveUploaderError) Unwrap() error { return e.Err }

// UploadFailureNotificationErrorは、送信失敗の退避処理が通知する一件のerror・警告です。
// KindはErrExistingUploadFailureFileCheck、ErrUploadProgressSave、ErrUploadProgressPreservedの
// いずれかで、errors.Isで判定できます。Errは原因です。
type UploadFailureNotificationError struct {
	Kind          error
	SourcePath    string
	DestinationID objectstorage.DestinationID
	Err           error
}

func (e *UploadFailureNotificationError) Error() string {
	return fmt.Sprintf("%v for %q (destination %q): %v", e.Kind, e.SourcePath, e.DestinationID, e.Err)
}

func (e *UploadFailureNotificationError) Unwrap() []error { return []error{e.Kind, e.Err} }

// UploadFailureHandoffErrorは、object storageへの送信に失敗した圧縮ファイルを
// SavedTargetsの退避先へ保存したことを通知するerrorです。Errは送信のerrorです。
type UploadFailureHandoffError struct {
	SourcePath      string
	DestinationID   objectstorage.DestinationID
	FailureCategory uploader.FailureCategory
	SavedTargets    []compressedlogfailure.SavedTargetDetails
	Err             error
}

func (e *UploadFailureHandoffError) Error() string {
	return fmt.Sprintf(
		"object storage upload failed for %q; compressed log file was saved to durable stores %v (upload failure category %q, destination %q): %v",
		e.SourcePath, e.SavedTargets, e.FailureCategory, e.DestinationID, e.Err,
	)
}

func (e *UploadFailureHandoffError) Unwrap() error { return e.Err }

// UploadFailureConflictPreservedErrorは、退避先で同名などの衝突があった既存ファイルを
// RetainedPathへ保全したことを表します。保全したファイルは自動削除・再送しないため、
// 運用者が内容を確認します。
type UploadFailureConflictPreservedError struct {
	OriginalPath  string
	RetainedPath  string
	DestinationID objectstorage.DestinationID
}

func (e *UploadFailureConflictPreservedError) Error() string {
	return fmt.Sprintf(
		"preserved conflicting compressed log file %q as %q (destination %q); operator review required",
		e.OriginalPath, e.RetainedPath, e.DestinationID,
	)
}
