package failure

import (
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// FailureHandlerConfig はbackend送信失敗時のdurable処理設定です。
type FailureHandlerConfig struct {
	EnableDegrade bool                         // DurableType=noneの場合に別出力先へ直接退避するか
	MaxConcurrent int                          // 同時に実行できるdurable handler数
	DurableType   []durable.DurableType        // fileArchive、spool、または選択なしを表すnone
	SpillType     []durable.SpillType          // direct degrade先。現在はstdoutのみ実装済み。s3/nfsは予約値で、選択すると起動時エラー
	Fsstore       FailureFileSystemStoreConfig // fileArchive、spoolの設定
}

// Delivery用の失敗処理は、最終宛先への送信が失敗した後に実行する。
// 即時送信するDelivery loggerはfileArchiveの完了結果をDeliveryReportへ含める。
// 保存先はfileArchiveだけであり、retry用spoolはDeliveryLoggerでは使わない。
type DeliveryFailureHandlerConfig struct {
	EnableDegrade     bool                      // DurableType=noneの場合の直接退避可否
	DurableType       []durable.DurableType     // fileArchive、または選択なしを表すnone
	SpillType         []durable.SpillType       // 直接退避先。現在はstdoutのみ実装済み。s3/nfsは予約値で、選択すると起動時エラー
	FileArchiveConfig DeliveryFileArchiveConfig // fileArchiveの設定
}

func ParseSpillDestination(s string, defaultDestination durable.SpillDestination) durable.SpillDestination {
	normalized := strings.ToLower(strings.TrimSpace(s))
	if normalized == "" {
		return defaultDestination
	}

	switch normalized {
	case string(SpillDestinationS3):
		return SpillDestinationS3
	case string(SpillDestinationStdout):
		return SpillDestinationStdout
	case string(SpillDestinationNfs):
		return SpillDestinationNfs
	default:
		// 未知値をstdoutへ変換すると、overload handler factoryが設定誤りを
		// 検出できない。値を保持し、実装済みdestinationかどうかはfactoryで判定する。
		return durable.SpillDestination(normalized)
	}
}

func ParseOverloadPolicy(s string, defaultPolicy durable.OverloadPolicy) durable.OverloadPolicy {
	normalized := strings.ToLower(strings.TrimSpace(s))
	if normalized == "" {
		return defaultPolicy
	}

	switch normalized {
	case string(OverloadPolicyDrop):
		return OverloadPolicyDrop
	case string(OverloadPolicyDegrade):
		return OverloadPolicyDegrade
	default:
		return durable.OverloadPolicy(normalized)
	}
}

// ParseSpillTypesはDurableType=noneの場合に使用する直接退避先を正規化します。
// 各durable storeがoverloadしたときの退避先はParseSpillDestinationが扱う。
// defaultの選択や対応可否は扱わず、未知値・空値・重複も後段のvalidatorへ渡します。
func ParseSpillTypes(s []string) []durable.SpillType {
	result := make([]durable.SpillType, 0, len(s))
	for _, value := range s {
		normalized := strings.ToLower(strings.TrimSpace(value))
		switch normalized {
		case string(durable.SpillTypeNone):
			result = append(result, durable.SpillTypeNone)
		case string(DegradeDestinationS3):
			result = append(result, DegradeDestinationS3)
		case string(DegradeDestinationStdout):
			result = append(result, DegradeDestinationStdout)
		case string(DegradeDestinationNfs):
			result = append(result, DegradeDestinationNfs)
		default:
			result = append(result, durable.SpillType(normalized))
		}
	}

	return result
}

// file archive 設定
type FailureFileSystemStoreConfig struct {
	FileArchiveConfig FileArchiveConfig
	SpoolConfig       SpoolConfig
}

type FileArchiveConfig struct {
	MaxFileSize uint64

	// 例: backendfailurearchive
	// 例: /var/log/{target}/{dir}/
	// 例: /var/log/heterolog/backendfailurearchive
	Dir string // 例: /var/log/{target}/{dir}/
	// retryable固定のため
	DirCaseRetryable           string // 例: /var/log/{target}/{dir}/{retryable}
	DirCasePermanent           string // 例: /var/log/{target}/{dir}/{permanent}
	EnableRotate               bool
	MaxFileBackups             int
	MaxFileAge                 int
	MaxDirSize                 uint64
	EnableFileCompress         bool
	CompressFormat             fsstore.CompressFormat
	FileRotation               fsstore.FileRotationType // daily weekly monthly
	FileRotationDayOfWeek      time.Weekday
	MaxWriteRetries            int
	FileOperationRetryWaitTime time.Duration
	ObjectStorageUploadConfig  objectstorage.UploadTargetConfig
	// LifecycleModeはrotation後のcompress、upload、purgeを実行する主体を選択します。
	// disableは実行せず、inlineはfailure handler、runnerは専用runnerが担当します。
	LifecycleMode fsstore.FileLifecycleMode

	OverloadPolicyConfig FileArchiveOverloadPolicyConfig
	HandlerConfig        internalerror.RoutingConfig
}

type DeliveryFileArchiveConfig struct {
	MaxFileSize uint64

	// 例: backendfailurearchive
	// 例: /var/log/{target}/{dir}/
	// 例: /var/log/heterolog/backendfailurearchive
	Dir                        string // 例: /var/log/{target}/{dir}/
	DirCaseRetryable           string // 例: /var/log/{target}/{dir}/{retryable}
	DirCasePermanent           string // 例: /var/log/{target}/{dir}/{permanent}
	EnableRotate               bool
	MaxFileBackups             int
	MaxFileAge                 int
	MaxDirSize                 uint64
	EnableFileCompress         bool
	CompressFormat             fsstore.CompressFormat
	FileRotation               fsstore.FileRotationType // daily weekly monthly
	FileRotationDayOfWeek      time.Weekday
	MaxWriteRetries            int
	FileOperationRetryWaitTime time.Duration
	ObjectStorageUploadConfig  objectstorage.UploadTargetConfig
	// LifecycleModeの意味は通常failure archiveと同じです。
	LifecycleMode fsstore.FileLifecycleMode

	OverloadPolicyConfig FileArchiveOverloadPolicyConfig
	HandlerConfig        internalerror.RoutingConfig
}

// 　オーバーロード時の処理判定の設定(ディスクスペース空き容量による制限のみ)
type FileArchiveOverloadPolicyConfig struct {
	OverloadPolicy   durable.OverloadPolicy   // dropまたはdegrade
	SpillDestination durable.SpillDestination // spoolに受け入れられない(degrade)場合の二次退避先　s3 stdoutなど
}

type SpoolConfig struct {
	Dir string // 例: /var/lib/{appname}/spool/{Dir} (Dir部分: backend名など)

	OverloadPolicyConfig SpoolOverloadPolicyConfig
	HandlerConfig        internalerror.RoutingConfig
}

// オーバーロード時の処理判定とactive backlog滞留Warnの設定。
// ActiveBacklogWarnAfterDaysは通知専用で、オーバーロード判定には使用しない。
type SpoolOverloadPolicyConfig struct {
	OverloadPolicy               durable.OverloadPolicy   // dropまたはdegrade
	SpillDestination             durable.SpillDestination // spoolに受け入れられない(spill)場合の二次退避先　s3 stdoutなど
	MaxCallsSinceScan            uint64                   // 前回の統計 scan キャッシュの最大利用回数の設定値
	StatsScanInterval            time.Duration            // overload 判定用に保持している ディレクトリ統計の有効期間
	MaxDirSize                   uint64                   // 「active」ディレクトリサイズが当閾値を越えたらspoolには保存しない
	ActiveBacklogWarnAfterDays   int                      // 「active」の最古データがこの日数より古ければWarn通知する。spoolへの保存は拒否しない
	UseMemoryPressureForOverload bool                     // メモリ逼迫状況に応じてOverload判定を行うかの可否
}
