package lifecycle

import "context"

// RunReporterは、ログファイルの保守結果や処理中のエラーを利用側へ通知する契約です。
// RunOnceやReconcileObjectStorageUploadDestinationsは結果を戻り値で返します。
// 周期Runnerや起動処理の呼び出し元は、受け取った結果をReportRunへ渡せます。
// このpackageはloggerのOnErrorや出力先設定を扱わず、利用側が内部ログやメトリクスへ変換します。
//
// 構築時に指定したReporterへは、受理済みファイル処理の復旧結果を非同期で通知します。
// 圧縮ログの送信失敗archiveのpurge結果も、UploadFailureArchivePurgeに設定して別途通知し、
// 通常の一巡処理の結果には合算しません。
//
// checkpointの保存再試行エラーでは、DirectoryPathに対象系列の保存先を設定します。
// 通常周期の処理件数は含めません。矛盾した保存結果を保全し、処理を継続できた場合は、
// CheckpointWarningを設定してerr=nilで通知します。利用側では警告として扱います。
type RunReporter interface {
	ReportRun(ctx context.Context, report RunReport, err error)
}
