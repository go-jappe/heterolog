// Package logfileは、ログファイルの処理を進めるための共通部品を提供します。
// inlineとrunnerが共有する再試行・終了管理を担当し、ファイル操作は利用側へ委ねます。
// checkpointファイルへの保存と、その保存失敗の再試行はfsstore/checkpointが担当します。
package logfile
