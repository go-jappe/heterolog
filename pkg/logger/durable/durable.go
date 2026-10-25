package durable

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// DurableTypeは既存logger failure-store APIとのsource互換を保つaliasです。
// compressed log file upload failure側は、このpackageをimportせず同じ選択値を使用できます。
type DurableType = failurestore.DurableType

// SpillType はdurable storeを使わない場合の直接退避先です。
// 現在実装済みなのはstdoutだけです。s3/nfsは予約値で、選択すると
// failure handlerの構築時に起動エラーになります。
type SpillType string

type OverloadPolicy string   // spool限界時のポリシー
type SpillDestination string // 1次退避先がoverload時の2次退避先

const (
	// DurableTypeNoneはfailure storeの保存方式を明示的に選択しないことを表します。
	// 単独指定した場合、spoolとfile archiveのどちらも使用しません。
	DurableTypeNone        = failurestore.DurableTypeNone
	DurableTypeFileArchive = failurestore.DurableTypeFileArchive
	DurableTypeSpool       = failurestore.DurableTypeSpool

	// SpillTypeNoneはDurableTypeNone選択時の直接退避先を明示的に選択しないことを表します。
	// EnableDegradeがtrueの場合は設定矛盾としてvalidation errorになります。
	SpillTypeNone SpillType = "none"

	OverloadPolicyDrop    OverloadPolicy = "drop"
	OverloadPolicyDegrade OverloadPolicy = "degrade"

	SpillDestinationS3     SpillDestination = "s3"
	SpillDestinationStdout SpillDestination = "stdout"
	SpillDestinationNFS    SpillDestination = "nfs"
)

// FailureStoreConfig はbackend共通のdurable failure store設定です。
type FailureStoreConfig struct {
	// EnableDegradeはDurableTypeNoneを選択した場合だけ、
	// SpillTypeの退避先へ直接出力するかを制御する。
	// store使用中のoverload処理は各OverloadPolicyが別途制御する。
	EnableDegrade      bool
	MaxConcurrent      int // failure storeまたはdirect degradeが有効な場合は正の値が必須
	DurableType        []DurableType
	SpillType          []SpillType // stdoutのみ実装済み。s3/nfsは予約値で、選択すると起動時エラー
	DurableStoreConfig DurableStoreConfig
}

// DeliveryFailureStoreConfig は、Delivery loggerの最終宛先への送信失敗後に使う
// failure store設定です。即時送信するDelivery loggerは保存完了を待って結果を
// DeliveryReportへ含めます。送信自体をqueueへ受け付けるloggerでは、後続flushが
// この設定を使用します。
type DeliveryFailureStoreConfig struct {
	// EnableDegradeとSpillTypeはDurableTypeNone選択時の直接退避を制御する。
	EnableDegrade     bool
	DurableType       []DurableType
	SpillType         []SpillType // stdoutのみ実装済み。s3/nfsは予約値で、選択すると起動時エラー
	FileArchiveConfig DeliveryFileArchiveConfig
}

// DurableStoreConfig はbackend共通のdurable store設定です。
type DurableStoreConfig struct {
	FileArchiveConfig FileArchiveConfig
	SpoolConfig       SpoolConfig
}

type SpoolConfig struct {
	SpoolPath string // ex.)/var/lib/{appname}/spool (/var/lib/部分)
	Target    string // (appname部分)
	Dir       string // ex.)/var/lib/{appname}/spool/{Dir} (Dir部分)

	OverloadPolicyConfig SpoolOverloadPolicyConfig
	HandlerConfig        handler.HandlerConfig
}

// オーバーロード時の処理判定の設定。
//
// retryable な失敗データを spool へ保存してよいかを判定するための閾値群を持つ。
// 主な用途は、バックエンド送信失敗時に retry 用データを active spool へ退避する前に、
// その保存先自体が過負荷・逼迫していないかを確認すること。
//
// 各 overload 閾値は「0値なら無効」として扱う設計にすると運用しやすい。
// ActiveBacklogWarnAfterDays だけは保存可否に影響しない滞留警告用の閾値。
//
// ActiveBacklogWarnAfterDays を除く有効な条件に抵触した場合は、
// OverloadPolicy に従って drop または spill（SecondaryDestination への退避）を行う。
type SpoolOverloadPolicyConfig struct {
	// オーバーロード判定時の動作方針。
	//
	// drop:
	//   spool への保存を諦めて破棄する。
	//
	// spill:
	//   spool 以外の SecondaryDestination へ退避する。
	OverloadPolicy OverloadPolicy

	// OverloadPolicy が spill の場合の二次退避先。
	// 例: s3, stdout など。
	//
	// spool にこれ以上受け入れられない場合の代替保存先を表す。
	//
	// 0値または未設定時の扱いは実装側で定義する。
	// たとえば「spill 指定なのに退避先未設定なら drop 扱いにする」など。
	SpillDestination SpillDestination

	// 前回の統計 scan 以降、この回数を超えて overload 判定が呼ばれた場合、
	// StatsScanInterval の期限前でも ディレクトリ統計を再 scan する。
	//
	// 短時間に大量の保存判定が走る場合でも、古い統計値を使い続けることを防ぐための上限。
	//
	// 0 の場合は、呼び出し回数による再 scan は無効。
	MaxCallsSinceScan uint64

	// overload 判定用に保持している ディレクトリ統計の有効期間。
	// この期間を過ぎた場合、次回の overload 判定時に統計を再 scan する。
	//
	// 0 以下の場合は、毎回再 scan する。
	StatsScanInterval time.Duration

	// ディレクトリ配下の総サイズ上限。
	// この値を超えていた場合、新たな retryable データは spool に保存しない。
	//
	// 単位: bytes
	//
	// 0 の場合は無効。
	MaxDirSize uint64

	// active ディレクトリ内の最古ファイルに対して滞留警告を出す経過日数。
	// 最も古いデータがこの日数より古い場合は一日一回 Warn を出すが、
	// overload とはみなさず、新たな retryable データの保存は継続する。
	//
	// 単位: days
	//
	// 0 の場合は無効。負数は設定エラー。
	ActiveBacklogWarnAfterDays int

	// ホストまたは実行環境上で確保すべき最小空きディスク容量。
	// 利用可能なディスク空き容量がこの値を下回った場合、
	// 新たな retryable データは spool に保存しない。
	//
	// 単位: bytes
	//
	// 0 の場合は無効。
	MinAvailableDiskSpace uint64

	// メモリ逼迫状況をoverload判定材料にするか
	UseMemoryPressureForOverload bool

	// メモリ逼迫とみなす使用率閾値。
	// 例: 0.90 なら、使用率 90% 以上を critical 候補とする。
	//
	// 0.0 - 1.0 を想定。
	//
	// 0 の場合は無効。
	MemoryCriticalRatio float64

	// 最低限確保したい空きメモリ量。
	// 利用可能メモリがこの値を下回った場合、メモリ逼迫候補とする。
	//
	// 単位: bytes
	//
	// Ratio 判定と AND / OR のどちらで組み合わせるかは実装方針による。
	// 0 の場合は無効。
	MinAvailableMemoryBytes uint64

	// メモリ逼迫判定を何回連続で観測したら overload と確定するか。
	// 一時的なスパイクで直ちに spool 保存停止へ入るのを防ぐための連続ヒット回数。
	//
	// 例: 3 の場合、critical 判定が 3 回連続したときに overload とみなす。
	//
	// 0 の場合は無効。
	// ただし、実装によっては 0 を 1 回として扱ってもよい。
	MemoryCriticalConsecutiveHits int
}

// open
type FileArchiveConfig struct {
	Target                 string
	MaxFileSize            uint64
	FailureFileArchivePath string // ex.)/var/lib/{appname}　(linuxの場合の例)
	Dir                    string // ex.)/var/lib/{appname}/{dir}/
	DirCaseRetryable       string // ex.)/var/lib/{appname}/{dir}/{retryable}
	DirCasePermanent       string // ex.)/var/lib/{appname}/{dir}/{permanent}
	FileNameFormat         string
	FileDateFormat         string
	EnableRotate           bool
	UsageCacheTTL          time.Duration
	// MaxFileBackups、MaxFileAge、MaxDirSizeは保持世代数、保持日数、ディレクトリ合計byte数の上限です。
	// 0は上限なしで、その条件では削除しません。0以外は保持世代数2以上、保持日数1以上、
	// MaxDirSizeは10 MiB以上かつMaxFileSize以上を指定します。上限なしでも、空き容量が
	// OverloadPolicyConfig.MinAvailableDiskSpaceを下回った新しい失敗はarchiveへ保存しません。
	MaxFileBackups     int
	MaxFileAge         int
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     fsstore.CompressFormat
	// CompressionCodecはprogrammaticに追加する独自圧縮形式です。nilなら
	// CompressFormatに対応する組み込みcodecを使用します。
	CompressionCodec           fsstore.CompressionCodec
	RotationType               fsstore.FileRotationType // daily weekly monthly
	RotationDayOfWeek          time.Weekday
	MaxWriteAttempts           int
	FileOperationRetryWaitTime time.Duration
	ObjectStorageUploadConfig  objectstorage.UploadTargetConfig
	ArchiveCheckpointConfig    fsstore.ArchiveCheckpointConfig
	// LifecycleRunnerConfigはrunner modeの周期loopとoptional Supervisor設定です。
	LifecycleRunnerConfig fsstore.AppendLogRunnerConfig
	// LifecycleModeはrotation後のcompress、upload、purgeを実行する主体です。
	// 空値はLifecycleMode導入前と同じinlineとして扱います。
	LifecycleMode fsstore.FileLifecycleMode

	OverloadPolicyConfig FileArchiveOverloadPolicyConfig
	HandlerConfig        handler.HandlerConfig

	IsDebug                  bool
	ErrorTraceDepth          int // backend loggerのerror trace設定を継承
	ErrorTraceDepthDebugMode int // backend loggerのdebug時error trace設定を継承
}

type DeliveryFileArchiveConfig struct {
	Target                 string
	MaxFileSize            uint64
	FailureFileArchivePath string // ex.)/var/lib/{appname}　(linuxの場合の例)
	Dir                    string // ex.)/var/lib/{appname}/{dir}/
	DirCaseRetryable       string // ex.)/var/lib/{appname}/{dir}/{retryable}
	DirCasePermanent       string // ex.)/var/lib/{appname}/{dir}/{permanent}
	FileNameFormat         string
	FileDateFormat         string
	EnableRotate           bool
	UsageCacheTTL          time.Duration
	// MaxFileBackups、MaxFileAge、MaxDirSizeの意味と0（上限なし）の扱いはFileArchiveConfigと同じです。
	MaxFileBackups     int
	MaxFileAge         int
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     fsstore.CompressFormat
	// CompressionCodecの契約はFileArchiveConfig.CompressionCodecと同じです。
	CompressionCodec           fsstore.CompressionCodec
	RotationType               fsstore.FileRotationType // daily weekly monthly
	RotationDayOfWeek          time.Weekday
	MaxWriteAttempts           int
	FileOperationRetryWaitTime time.Duration
	ObjectStorageUploadConfig  objectstorage.UploadTargetConfig
	ArchiveCheckpointConfig    fsstore.ArchiveCheckpointConfig
	// LifecycleRunnerConfigの意味はFileArchiveConfigと同じです。
	LifecycleRunnerConfig fsstore.AppendLogRunnerConfig
	// LifecycleModeの意味と空値の扱いはFileArchiveConfigと同じです。
	LifecycleMode fsstore.FileLifecycleMode

	OverloadPolicyConfig FileArchiveOverloadPolicyConfig
	HandlerConfig        handler.HandlerConfig

	IsDebug                  bool
	ErrorTraceDepth          int // backend loggerのerror trace設定を継承
	ErrorTraceDepthDebugMode int // backend loggerのdebug時error trace設定を継承
}

// MinFileArchiveAvailableDiskSpaceはfailure archiveの空き容量閾値として
// 設定できる最小値です。
const MinFileArchiveAvailableDiskSpace uint64 = 100 * 1024 * 1024

// FileArchiveOverloadPolicyConfigはfailure archiveの過負荷判定と退避方法の設定です。
type FileArchiveOverloadPolicyConfig struct {
	OverloadPolicy        OverloadPolicy   // drop or degrade
	SpillDestination      SpillDestination // archiveに受け入れられない(degrade)場合の二次退避先　s3 stdoutなど
	MinAvailableDiskSpace uint64           // これを下回ったらarchiveには保存しない
}
