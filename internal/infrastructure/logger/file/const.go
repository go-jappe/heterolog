package file

// 主にバリデーションに使用される各設定項目の最小値の定義
const (
	DateFormat = "2006-01-02" // period dateに使用中

	// 「ログファイル最大サイズ」の最小値（バイト単位）　これ以下の場合はエラーを返す
	MinAllowedLogFileSizeBytes = 1 * 1024 * 1024
	// ログ世代最大保持数の最小値
	MinAllowedMaxLogFileBackups int = 2
	//　バックアップ保持日数の最小値
	MinAllowedMaxLogFileAge int = 1
	//　「ログディレクトリ最大サイズ」の最小値
	MinAllowedMaxLogDirSize uint64 = 10 * 1024 * 1024
	// サンプリングレート 1/(指定値)　よって1未満はエラー
	MinSamplingRate int = 1
)
