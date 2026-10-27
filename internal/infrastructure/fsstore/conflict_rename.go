package fsstore

// ConflictRenameは、正規のfile名を一つに保つために退避した既存fileを記録します。
// 退避したfileは自動のreplayとpurgeの対象外になるため、RetainedPathは運用者の
// 確認が必要です。RetainedPathは新しい保存先のpathではありません。
type ConflictRename struct {
	OriginalPath string
	RetainedPath string
}
