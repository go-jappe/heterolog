// Package lifecycleは、ログの保存ディレクトリを一巡し、各系列の圧縮・upload・
// 保持設定による削除を進めます。通常ログと、backend送信失敗レコードのJSONLを扱います。
// 通常稼働の周期Runnerと、アプリケーション停止中の手動復旧CLIが利用します。
//
// このpackageは対象の選択、実行順序、checkpointによる進捗管理と結果通知を担当します。
// 一ファイルの処理はlogfile/processingを共有し、ファイル操作とcheckpoint保存は
// fsstoreへ、uploadと失敗時の退避はobjectstorageへ委譲します。
// 周期の実行間隔と再起動はlogfile/runnerが、退避後の再送はspool/compressedlogが管理します。
// 手動復旧では最新世代も処理し、保持期限・世代数・容量によるpurgeは行いません。
package lifecycle
