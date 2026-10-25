package compressedlogfailure

import (
	"context"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// confirmFailureStorageCompletedは、指定宛先の全退避先に元ファイルと同名・同内容のファイルがあり、
// 同期も完了した場合にtrueを返します。途中経過の記録がない場合に送信前に呼び、重複送信を減らします。
// 内容の照合・同期は各退避先の処理へ委譲し、確認する退避先の一覧は保存時と共用します。
// 退避先が未設定、または一つでもファイルが見つからなければfalse、確認・同期に失敗すればerrorを返します。
// 競合ファイルを保全した場合は、その移動結果も返します。元の送信結果や失敗分類は推測しません。
// 呼び出し元はtrueなら送信を省略します。完了cursorの更新や元ファイルの削除はここでは行いません。
func (u *ManagedUploader) confirmFailureStorageCompleted(ctx context.Context, request uploader.UploadRequest, destination objectstorage.DestinationID) (bool, []infrafsstore.ConflictRename, error) {
	targets := u.failureTargets[destination]
	if len(targets) == 0 {
		return false, nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, u.saveTimeout)
	defer cancel()
	var conflicts []infrafsstore.ConflictRename
	for _, target := range targets {
		release, err := target.lock(ctx)
		if err != nil {
			return false, conflicts, fmt.Errorf("protect existing %s: %w", target.name(), err)
		}
		if release != nil {
			defer release()
		}
		file, found, err := target.reuse(ctx, request.FilePath())
		conflicts = append(conflicts, file.conflicts...)
		if err != nil {
			return false, conflicts, fmt.Errorf("reuse existing %s: %w", target.name(), err)
		}
		if !found {
			return false, conflicts, nil
		}
		if err := validateFailureTargetFile(target, file); err != nil {
			return false, conflicts, err
		}
	}
	// 元の送信結果は不明なのでUploadProgressを作らず、呼び出し元が通常どおり
	// 完了cursorを更新します。競合を保全した場合だけ通知し、通知が戻るまでは
	// 確認した退避先のpurgeを防ぎます。
	if len(conflicts) != 0 {
		u.notify(context.WithoutCancel(ctx), Notification{
			DestinationID: destination, SourcePath: request.FilePath(), ConflictRenames: conflicts,
		})
	}
	return true, conflicts, nil
}
