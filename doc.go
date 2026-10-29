// Package heterolog provides structured logging to files, standard output,
// Slack, OpenSearch, Loki, CloudWatch Logs, and ClickHouse.
//
// Start with DefaultConfig and New, and call Close or Shutdown when the
// application exits to flush buffered logs and release owned resources.
// Configuration is explicit: importing this package or constructing a logger
// does not load environment files, install signal handlers, or change the
// process-wide slog logger or OpenTelemetry providers. To configure loggers
// from HETEROLOG_ environment variables and an env file, call LoadEnvConfig.
//
// Advanced backend and durability settings are available in pkg/logger and
// its related public packages. A configuration must not be modified while a
// logger constructed from it is in use.
package heterolog

// このpackageは公開の窓口です。methodを持つ公開の型はここで定義し、各methodは内部の
// 実装へ処理を渡すだけにします。そのほかの公開の型は、pkg/の公開packageの型の別名として
// 置きます（中身を見せない関数の型のOptionだけは、internalの型の別名です）。loggerの
// 終了・検証・既定値はinternal/application/loggerruntime、出力先の実装を渡してloggerを
// 組み立てるのはinternal/bootstrap、slogと環境設定の入力窓口はinternal/presentationに
// あります。公開APIの説明は英語で書きます。

// 最小構成のcontainerやWindowsでも、設定したIANAのtime zoneを使えるようにします。
import _ "time/tzdata"
