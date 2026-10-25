// Package failurestoreはpayload形式の異なるfailure pipelineが共有する
// durable store選択値を定義します。
package failurestore

// DurableTypeは失敗dataの永続化先を識別します。保存先ごとの設定と書込み契約は
// payload固有packageが定義し、この型は共通の選択値だけを表します。
type DurableType string

const (
	// DurableTypeNoneはdurable failure保存先を選択しないことを明示します。
	DurableTypeNone DurableType = "none"
	// DurableTypeFileArchiveは失敗dataをfilesystem archiveへ保存します。
	DurableTypeFileArchive DurableType = "fileArchive"
	// DurableTypeSpoolは失敗dataを再試行用spoolへ保存します。
	DurableTypeSpool DurableType = "spool"
)
