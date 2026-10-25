package failurearchive

import pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

const (
	// FileExtensionはfailure archive writerとlifecycle runnerが共有するJSONL拡張子です。
	FileExtension = "jsonl"
	// FileNameFormatはfailure archiveの固定命名規則です。
	// pkg/fsstoreの公開DTOで使用する定数と同じ値を参照し、mapping、writer、
	// lifecycle runnerの形式が独立して変化しないようにします。
	FileNameFormat = pkgfsstore.FailureArchiveFileNameFormat
)
