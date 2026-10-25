package fsstore

import "errors"

// ErrDurabilityUnconfirmedは、file本体またはdirectoryの同期を完了できず、
// 保存結果の永続化を確認できないことを表します。データ喪失を確定する値ではありません。
// archive処理のreport/errorからerrors.Isで判定できます。同期未確認の保存先への
// 引き渡しは成功扱いにせず、呼出し元が所有する元fileを保持して再試行します。
// 新しく作成したdirectoryの親を同期できなかった場合は、同じprocessでその親に依存する
// 保存先を準備するたびに同期を再試行し、成功するまで、保存先が作成済みでもこのerrorを返します。
var ErrDurabilityUnconfirmed = errors.New("filesystem durability could not be confirmed")
