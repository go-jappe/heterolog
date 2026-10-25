package lifecycle

import (
	"context"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// advanceFileCompletionCursorは、メモリ上の完了cursorの更新・途中経過の整理・保存をStoreへ委譲します。
// keyは完了を記録する世代と圧縮の有無、idsは送信または必要な退避が完了した宛先です。
// pathは、そのファイルの完了結果を保持するmanagerの反映待ち一覧を引くために使います。
//
// 反映待ち一覧は、Storeが更新を受理する前に失敗しても、完了済みの宛先へ再送しないための情報です。
// 正常終了または保存だけの失敗なら更新は受理済みなので、指定した宛先を一覧から外します。
// 更新の受理を確認できないエラーでは一覧を保持し、呼び出し元がcursor更新を再試行します。
// このkeyとidsに該当する保存結果の矛盾では、その宛先だけを外して保全処理へ戻します。
//
// 戻り値のエラーは、一覧を整理した場合もそのまま返します。呼び出し元がエラー通知と
// 再開地点を判断し、ファイル保存の再試行はStore、未受理の更新や保全の再開は利用側が担当します。
func (m *objectStorageUploadManager) advanceFileCompletionCursor(
	ctx context.Context, path string, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (
	checkpointstore.Checkpoint, error) {
	// Storeが完了cursorと対象範囲の途中経過をメモリへ反映し、通常はそのstateをファイルへ保存します。
	// 保存だけ失敗した場合も更新済みの完了記録が返るため、保存待ちと未受理の失敗を下で区別します。
	checkpoint, err := m.checkpointStore.AdvanceCompletionCursor(ctx, key, ids)
	// 正常終了または保存だけの失敗なら、今回渡したids全体を反映待ち一覧から外します。
	forget := ids
	if destination, found := checkpointstore.SavedTargetConflictDestination(err, key, ids); found {
		// 世代・圧縮の有無がkeyと一致し、idsに含まれる宛先の矛盾だけを対象にします。
		// cursor更新を繰り返すだけでは解消しないため、その宛先を反映待ちから外して
		// 次回のManagedUploaderによる元ファイルの保全と矛盾の解消へ戻します。
		// 他の宛先の完了結果は保持し、まとめて再送することを防ぎます。
		forget = []objectstorage.DestinationID{destination}
	} else if err != nil && !checkpointstore.IsPendingSaveOnly(err) {
		// キャンセルなど、更新の受理を確認できない失敗では完了結果を一覧に残します。
		// 呼び出し元が送信をやり直さずcursor更新から再開できるよう、完了結果を保持して返します。
		// このkey・idsに一致する矛盾がなく、別のkey・宛先の矛盾や保存待ち以外のエラーが
		// 含まれる場合も、この分岐です。
		return checkpoint, err
	}
	if len(forget) > 0 {
		// 成功・保存待ちではids全体、該当する矛盾ではその宛先だけを一覧から外します。
		// Storeへの呼び出し後にmanagerのlockを取得し、ここでは反映待ち一覧の更新を排他します。
		m.stateMu.Lock()
		// 完了結果の登録時と同じ比較用keyを使い、パス表記が異なる場合も同じ一覧を整理します。
		pathKey := pathidentity.ComparisonKey(path)
		for _, id := range forget {
			delete(m.uploadedPathDestinations[pathKey], id)
		}
		if len(m.uploadedPathDestinations[pathKey]) == 0 {
			// 他の宛先の反映待ちもなければ、不要になったファイル単位の項目を削除します。
			delete(m.uploadedPathDestinations, pathKey)
		}
		m.stateMu.Unlock()
	}
	// 保存待ちや矛盾のエラーも返し、呼び出し元の通知・再試行判定へ渡します。
	return checkpoint, err
}
