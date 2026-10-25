// Package runnerは、ログ保存先のlifecycle処理を定期間隔で実行し、結果を報告します。
// Supervisorによる周期処理の再起動も扱います。ファイルの選択・圧縮・送信・purgeと
// 手動復旧の一巡処理はlogfile/lifecycleが担当します。
package runner
