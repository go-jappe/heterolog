package logging

import (
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// LogEntryは、top-level loggerがapplication入力から一度だけ生成するbackend非依存の
// 論理ログです。各backendはcloneを受け取り、OpenSearch document、Loki stream、
// CloudWatch Logs eventなどの保存先固有形式へ変換します。
type LogEntry struct {
	// OccurredAtは、このログが生成された時点で確定した発生時刻です。
	// buffer待機やfallbackを経ても同じ値を維持します。timezoneに依存しない同じ瞬間を
	// UTCで保持し、表示形式とtimezoneはLogEntryではなく各出力先が決定します。
	OccurredAt time.Time
	Level      pkglogger.LogLevel // ログレベル
	TraceID    string             //  オブザーバビリティ用
	SpanID     string             //  オブザーバビリティ用
	RequestID  string             //　リクエストID
	Message    string             // ログ本文

	// 以下はエラーの詳細ログ用
	File     []string // 呼び出し元のファイル
	Line     []int    // 行番号
	FuncName []string // 関数名
	Err      error    // 実際のエラー
	// StackTraceRequestedは、applicationがErrorWithStackを呼び出したことを表します。
	// 呼び出し元のstackはfan-out前にstackPCsへ保存します。各handlerは保存先ごとの
	// 深さでそのstackを展開し、自身のcloneへFile、Line、FuncNameを設定します。
	// ErrorWithStackにerrorが渡されなかった場合もErrはnilのまま、stackを持つERRORとして
	// 記録します。その場合、各出力先はerrorの欄だけを出力しません。
	StackTraceRequested bool
	stackPCs            []uintptr

	Internal bool   // 内部エラー（ロガー内部ログ）であるか真偽値
	Source   string // どのロガーで発生した内部エラー（内部ログ）ハンドル分岐に使用

	Sampled bool // ログサンプリングによる抽出対象であったか

	Fields map[string]any `json:",omitempty"`
}
