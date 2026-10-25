package fsstoreconfig

import "time"

const (
	DefaultUsageCacheTTL = 15 * time.Minute
)

type CommonFileSystemStoreConfig struct {
	MinAvailableDiskSpace uint64 // ファイルストレージの容量がこの値を下回ったらfilesystemにはstoreしない
	// UsageCacheTTLは、最後にfilesystemから実測した使用量の概算値を
	// 再観測なしで使用できる時間です。TTL経過後に到着した次のinline cleanupで
	// 再観測します。概算値が上限を超えた場合はTTL経過を待ちません。
	UsageCacheTTL  time.Duration
	LogPath        string // ex.)/var/log/{appname}　(/var/log/ 部分)
	SpoolPath      string // ex.)/var/lib/{appname}/spool/{backendTarget} (/var/lib/部分)
	AppStatePath   string // failure archiveやarchive checkpointを保存するbase path。最終RuntimeConfig.Targetは利用側で付与する
	FileNameFormat string // 通常log fileに使用する命名規則。failure archiveはJSONL固定
	FileDateFormat string // 通常logとfailure archiveのfile名に含める日付命名規則
}
