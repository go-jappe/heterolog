package mappedvalidation

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// ConfigSectionは、ConfigErrorで不正な設定の区分を表す名前です。エラー表示にも使います。
type ConfigSection string

const (
	// ConfigSectionLifecycleは、file lifecycleの実行方式・保持設定・使用量cacheの設定です。
	ConfigSectionLifecycle ConfigSection = "lifecycle"
	// ConfigSectionDirectoryは、ログの保存先directoryの設定です。
	ConfigSectionDirectory ConfigSection = "directory"
	// ConfigSectionFileNameは、ログファイル名を構成する設定です。
	ConfigSectionFileName ConfigSection = "file name"
	// ConfigSectionOverloadは、file書込み時の空き容量不足への方針です。
	ConfigSectionOverload ConfigSection = "overload"
	// ConfigSectionAppendLogRunnerは、lifecycle runnerの周期実行と自動再起動の設定です。
	ConfigSectionAppendLogRunner ConfigSection = "appendlog runner"
	// ConfigSectionFailureStoreは、送信失敗レコードの保存方式と同時処理数の設定です。
	ConfigSectionFailureStore ConfigSection = "failure store"
	// ConfigSectionFailureSpoolは、送信失敗レコードを再送するspoolの設定です。
	ConfigSectionFailureSpool ConfigSection = "failure spool"
	// ConfigSectionDirectDegradeは、保存せずに直接退避する場合の退避先の設定です。
	ConfigSectionDirectDegrade ConfigSection = "direct degrade"
	// ConfigSectionCloudWatchLogsStreamRoutingは、CloudWatch Logs streamの時間区分の設定です。
	ConfigSectionCloudWatchLogsStreamRouting ConfigSection = "cloudwatch logs stream routing"
)

// ConfigErrorは、設定元Sourceの設定区分Sectionが不正であることを表します。
// Errは不正の原因で、errors.Is/Asで辿れます。ValidateSyncLoggerConfigなどは複数のerrorを
// errors.Joinでまとめて返すため、全件を調べる場合はerror treeを辿ってください。
type ConfigError struct {
	// Sourceは"logger file"や"logger opensearch failure file archive"など、設定元の表示名です。
	Source string
	// Sectionは不正な設定の区分です。
	Section ConfigSection
	// Errは不正の原因です。
	Err error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("%s %s config is invalid: %v", e.Source, e.Section, e.Err)
}

func (e *ConfigError) Unwrap() error { return e.Err }

var (
	// ErrAppendLogRunnerStopOnErrorRequiresSupervisorは、runnerのStopOnErrorを有効にしたのに
	// 自動再起動のSupervisorが無効であることを表します。
	ErrAppendLogRunnerStopOnErrorRequiresSupervisor = errors.New(
		"appendlog runner stop on error requires supervisor in application runtime",
	)
	// ErrEmptyFileLifecycleModeは、config mapping後の通常file logger・file archiveの
	// lifecycle modeが空であることを表します。
	ErrEmptyFileLifecycleMode = errors.New("file lifecycle mode must not be empty after config mapping")
	// ErrEmptyDeliveryFileLifecycleModeは、config mapping後のDelivery file loggerの
	// lifecycle modeが空であることを表します。
	ErrEmptyDeliveryFileLifecycleMode = errors.New(
		"delivery file lifecycle mode must not be empty after config mapping",
	)
	// ErrArchiveCheckpointDirectoryRequiredは、archive checkpointの保存directoryが空であることを表します。
	ErrArchiveCheckpointDirectoryRequired = errors.New("archive checkpoint directory is required")
	// ErrArchiveCheckpointSourcePrefixRequiredは、archive checkpointのSourcePrefixが空であることを表します。
	ErrArchiveCheckpointSourcePrefixRequired = errors.New("archive checkpoint source prefix is required")
)

// LogDirectoryResolutionErrorは、保持設定を検証するために、Roleのログ保存先Directoryを
// 実体pathへ解決できなかったことを表します。Errは解決に失敗した原因です。
type LogDirectoryResolutionError struct {
	// Roleは"normal"・"error"・"debug"のいずれかです。
	Role string
	// Directoryは解決しようとした保存先のpathです。
	Directory string
	// Errは解決に失敗した原因です。
	Err error
}

func (e *LogDirectoryResolutionError) Error() string {
	return fmt.Sprintf("resolve %s log directory: %v", e.Role, e.Err)
}

func (e *LogDirectoryResolutionError) Unwrap() error { return e.Err }

// ArchiveCheckpointConfigErrorは、設定元Sourceのarchive checkpoint設定に必須値がないことを表します。
// ErrはErrArchiveCheckpointDirectoryRequiredまたはErrArchiveCheckpointSourcePrefixRequiredです。
type ArchiveCheckpointConfigError struct {
	// Sourceは設定元の表示名です。
	Source string
	// Errは不足している設定を表すsentinelです。
	Err error
}

func (e *ArchiveCheckpointConfigError) Error() string {
	return e.Source + " " + e.Err.Error()
}

func (e *ArchiveCheckpointConfigError) Unwrap() error { return e.Err }

// CloudWatchLogsStreamNameErrorは、設定元Sourceの基準stream名に時間区分のsuffixを付けた
// StreamNameが、CloudWatch Logsのstream名として不正であることを表します。
type CloudWatchLogsStreamNameError struct {
	// Sourceは設定元の表示名です。
	Source string
	// StreamNameは、検証用の固定日時でsuffixを付けたstream名です。
	StreamName string
	// Errはstream名を拒否した原因です。
	Err error
}

func (e *CloudWatchLogsStreamNameError) Error() string {
	return fmt.Sprintf("%s cloudwatch logs resolved stream name is invalid: %v", e.Source, e.Err)
}

func (e *CloudWatchLogsStreamNameError) Unwrap() error { return e.Err }

// CompressedLogFileUploadNameは、圧縮ログのupload失敗時にローカルへ保存するファイル名の
// 構成要素です。日付・連番は含みません。
type CompressedLogFileUploadName struct {
	// Sourceは"logger.file.default"や"logger.internal_log.file.error"など、出力元の名前です。
	Source string
	// Prefixは出力元の役割ごとのファイル名の接頭辞です。
	Prefix string
	// Targetはファイル名のtargetです。
	Target string
	// Extensionは圧縮前のファイルの拡張子です。
	Extension string
}

// CompressedLogFileUploadNameCollisionErrorは、通常のfile出力Baseと内部ログ専用のfile出力
// Internalが、同じServiceへのupload失敗時に同じローカル保存名を生成し得ることを表します。
type CompressedLogFileUploadNameCollisionError struct {
	// Serviceは両方の出力が選んだupload先サービスです。
	Service objectstorage.UploadTarget
	// Baseは通常のfile出力側のファイル名です。
	Base CompressedLogFileUploadName
	// Internalは内部ログ専用のfile出力側のファイル名です。
	Internal CompressedLogFileUploadName
}

func (e *CompressedLogFileUploadNameCollisionError) Error() string {
	return fmt.Sprintf(
		"object storage compressed log file upload name collision for service %q: %s uses prefix=%q target=%q extension=%q; %s uses prefix=%q target=%q extension=%q",
		e.Service,
		e.Base.Source,
		e.Base.Prefix,
		e.Base.Target,
		e.Base.Extension,
		e.Internal.Source,
		e.Internal.Prefix,
		e.Internal.Target,
		e.Internal.Extension,
	)
}
