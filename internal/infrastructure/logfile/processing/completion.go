// Package processingは、通常ログとbackend送信失敗archiveのファイルについて、
// 圧縮・送信と退避・checkpointへの完了反映・削除を一件ずつ進める共通処理を提供します。
// inline、周期runner、停止中の復旧CLIが同じ処理を使います。対象選択、走査、保護と
// ロックの範囲、再試行の登録、結果の通知、依存先の終了は呼び出し側が管理します。
package processing

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// CompletionRequestは、一件の送信・退避と完了記録の反映結果です。
// Completeには、今回の有効宛先すべてでuploadが不要になり、少なくとも一つの有効宛先へ
// 実際に送信・退避したことを確認した結果（UploadResult.Delivered）を渡します。
// 宛先の登録前から存在したため対象外とした宛先だけで完了した場合はfalseです。
type CompletionRequest struct {
	Path           string
	Compressed     bool
	DestinationIDs []objectstorage.DestinationID
	Complete       bool
	// UnnamedUploaderActiveは、宛先IDを持たない単一uploaderが有効な場合だけ指定します。
	// その場合のCompleteは、同じパスの実送信成功を呼び出し元が保持していることを表します。
	UnnamedUploaderActive bool
}

// CanRemoveCompletedFileは、現在の全有効宛先で完了し、そのうち少なくとも一つへ
// 送信・退避した圧縮ファイルだけを削除可能とします。deliveredにはUploadResult.Delivered、
// または読み込んだCheckpointのCompleteとDeliveredToAnyの両方を確認した結果を渡します。
// 未圧縮ファイル、有効宛先がないファイル、登録前から存在したため対象外とした宛先だけで
// 完了したファイルは、通常の保持期間・世代数・容量によるpurgeへ残します。
func CanRemoveCompletedFile(compressed bool, destinations []objectstorage.DestinationID, delivered bool) bool {
	return compressed && len(destinations) > 0 && delivered
}

// CompletionResultは、削除を試みたかと、その検査・削除結果です。
// 後続の親ディレクトリ同期・CloseのエラーはRemoveCompletedFileのerrorで返します。
type CompletionResult struct {
	Attempted bool
	Removal   fsstore.FileRemovalResult
}

// RemoveCompletedFileは完了条件を確認し、圧縮ファイルの削除・親ディレクトリ同期を進めます。
// 削除条件はCanRemoveCompletedFileと同じで、request.Completeは送信・退避を確認した完了です。
// removeには利用側のロック・検証cache破棄を含む削除処理を渡します。nilなら通常ファイルを
// 検査して削除するfsstoreの共通処理を使います。同期に失敗した場合は、送信を繰り返さず
// 完了記録を維持したまま後片付けを再試行してください。
func RemoveCompletedFile(ctx context.Context, request CompletionRequest, remove func(string) (fsstore.FileRemovalResult, error)) (CompletionResult, error) {
	eligible := CanRemoveCompletedFile(request.Compressed, request.DestinationIDs, request.Complete) ||
		(request.Compressed && request.UnnamedUploaderActive && request.Complete)
	if !eligible {
		return CompletionResult{}, nil
	}
	if err := ctx.Err(); err != nil {
		return CompletionResult{}, err
	}
	if remove == nil {
		remove = fsstore.RemoveRegularFileAndSyncDirectory
	}
	removal, err := remove(request.Path)
	return CompletionResult{Attempted: true, Removal: removal}, err
}
