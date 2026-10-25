package compressedlogfailure

import (
	"context"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// SavedTargetDetailsは保存が完了した退避先の通知用情報です。
// 通知側はStringで種類ごとの詳細を表示でき、具体型を判定する必要はありません。
type SavedTargetDetails interface {
	Kind() failurestore.DurableType
	String() string
}

// SavedArchiveDetailsはarchiveの保存先と実際の分類です。
// FilePathは今回保存したファイルに設定し、checkpointで確認した過去の成功では空です。
type SavedArchiveDetails struct {
	Target          string
	FilePath        string
	FailureCategory uploader.FailureCategory
}

func (SavedArchiveDetails) Kind() failurestore.DurableType {
	return failurestore.DurableTypeFileArchive
}

func (details SavedArchiveDetails) String() string {
	detail := fmt.Sprintf("%s (target %q, archive failure category %q)", details.Kind(), details.Target, details.FailureCategory)
	if details.FilePath != "" {
		detail += fmt.Sprintf(" (archive file %q)", details.FilePath)
	}
	return detail
}

// SavedSpoolDetailsはspoolの保存先と保存時点の状態です。
// FilePathは今回保存したファイルに設定し、checkpointで確認した過去の成功では空です。
type SavedSpoolDetails struct {
	Target   string
	FilePath string
	State    pkgspool.State
}

func (SavedSpoolDetails) Kind() failurestore.DurableType { return failurestore.DurableTypeSpool }

func (details SavedSpoolDetails) String() string {
	detail := fmt.Sprintf("%s (target %q, spool state %q)", details.Kind(), details.Target, details.State)
	if details.FilePath != "" {
		detail += fmt.Sprintf(" (spool file %q)", details.FilePath)
	}
	return detail
}

// Notifierはupload・退避・checkpoint保存に関する情報を内部ログの通知先へ渡す関数です。
// 通知先のpanicは、完了済みの保存結果を変更しません。
type Notifier func(context.Context, Notification)

// NotificationはManagedUploaderがNotifierへ渡す通知内容です。
// upload失敗、退避済みファイルの確認・保存、checkpoint保存のエラーや保全警告を通知します。
// 呼び出し元への送信結果は、別途UploadReportまたはerrorで返します。
// SavedTargetsには以前の試行で保存済みの退避先も含みます。
type Notification struct {
	DestinationID objectstorage.DestinationID
	SourcePath    string
	SavedTargets  []SavedTargetDetails
	// ConflictRenamesは同名衝突などで既存ファイルを.preservedへ移した結果です。
	// 元の配置と保全先を管理者へ通知するために使います。保全ファイルは自動削除・再送しません。
	ConflictRenames []infrafsstore.ConflictRename
	// PreservationWarningは、保存結果の矛盾に備えて元の圧縮ファイルを.preservedへ
	// コピー・同期した後の警告です。既存の通知先はOnWarnへ渡します。
	// 保全失敗はここには設定せず、SaveErr・CheckpointErrで通知します。
	PreservationWarning error
	// FailureCategoryは退避前の送信失敗の分類です。退避先の実際の分類はSavedTargetsに保持します。
	FailureCategory uploader.FailureCategory
	// UploadErrは退避前のobject storage送信で起きたエラーです。
	// 送信元の読取失敗や送信結果の形式不備も含みます。
	UploadErr error
	// SaveErrは退避先への保存エラーです。ファイルや親ディレクトリの同期未確認も含みます。
	// このエラーがある場合は元の圧縮ファイルを残し、保存を再試行します。
	SaveErr error
	// ExistingFileCheckErrは送信前の退避済みファイル確認エラーです。
	// この場合は元ファイルを残して送信を保留し、後続の周期で再試行します。
	ExistingFileCheckErr error
	// CheckpointErrは途中経過の保存エラーです。PendingSaveErrorならメモリへは受理済みです。
	CheckpointErr error
}
