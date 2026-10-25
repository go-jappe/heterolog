package logrecovery

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const DefaultRetryInterval = logfile.DefaultLogFileProcessingRecoveryInterval

// ProgressPhaseは手動復旧の現在の処理段階です。
type ProgressPhase string

const (
	ProgressPreparing    ProgressPhase = "preparing"
	ProgressRunning      ProgressPhase = "running"
	ProgressRetryWaiting ProgressPhase = "retry-waiting"
	ProgressStopping     ProgressPhase = "stopping"
	ProgressForced       ProgressPhase = "forced"
	ProgressComplete     ProgressPhase = "complete"
)

// Progressは一回の試行結果または停止状況のコピーです。過去の失敗は進捗として報告し、
// 最終Reportには解消していない失敗だけを残します。
type Progress struct {
	Phase       ProgressPhase
	Scope       string
	Attempt     int
	Directory   DirectoryReport
	Err         error
	NextRetryAt time.Time
}

// RecoveryScopeは、手動復旧するファイルを生成元で選ぶ指定です。
type RecoveryScope string

const (
	// RecoveryScopeFileLoggerは、FileLoggerがfilesystemへ直接書き出したログ系列を対象にします。
	// default/error/debugと専用internalログの未圧縮.log、その圧縮済みファイル、
	// 圧縮完了後の元ログ削除待ちファイルを含みます。
	RecoveryScopeFileLogger RecoveryScope = "file-logger"
	// RecoveryScopeLogBackendFailureArchiveは、OpenSearchなどのログbackendへの送信失敗を
	// filesystemのretryable/permanentへ保存したJSONL archiveの系列を対象にします。
	// 未圧縮.jsonl、その圧縮済みファイル、圧縮完了後の元ログ削除待ちファイルを含みます。
	// 圧縮ログのobject storage送信失敗を保存するarchiveは、この対象には含みません。
	RecoveryScopeLogBackendFailureArchive RecoveryScope = "log-backend-failure-archive"
	// RecoveryScopeAllはFileLoggerのログ系列と、ログbackend送信失敗archiveの両方を対象にします。
	RecoveryScopeAll RecoveryScope = "all"
)

// IsValidは指定可能な復旧対象かを返します。空値の既定値への変換はRecoverが行います。
func (s RecoveryScope) IsValid() bool {
	switch s {
	case RecoveryScopeFileLogger, RecoveryScopeLogBackendFailureArchive, RecoveryScopeAll:
		return true
	default:
		return false
	}
}

// Optionsは手動復旧で読むlogger設定と、復旧対象ファイルの生成元を選びます。
// ゼロ値はsync loggerの設定と全復旧対象を指定します。
type Options struct {
	// LoggerContractはsync・async best-effort・deliveryのどの設定を読むかを指定します。
	LoggerContract pkglogger.LoggerContractKind
	Scope          RecoveryScope
	// Retryは失敗した対象を、同じStoreと処理状態を保持して再試行します。
	// falseなら従来どおり一巡だけ実行します。
	Retry         bool
	RetryInterval time.Duration
	// OnProgressは実行状況の通知先です。短時間で戻り、通知から復旧の終了を待たないでください。
	OnProgress func(Progress)
	// ForceStopは通常終了の待機を打ち切る明示的な要求です。閉じると受理済み処理へ
	// 中止を通知します。実行中のOS I/Oを強制終了するものではありません。
	ForceStop <-chan struct{}
}
