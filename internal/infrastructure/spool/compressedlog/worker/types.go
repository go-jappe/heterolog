package worker

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"time"
)

// RouteRegistrationは、一つの圧縮ログのファイル名系列に、現在の送信処理と宛先を対応付けます。
// 宛先IDはUploaderから取得します。Uploaderは宛先を一つだけ持ち、登録中はそのIDを変更しません。
// 取得したファイルは送信前に登録内容と照合します。異なる宛先の登録が同じファイルに
// 一致した場合は拒否し、同じ宛先への重複登録は一つの送信経路として扱います。
type RouteRegistration struct {
	// FileNameはspoolへコピーされた圧縮ログの命名規則です。Dirは照合に使いません。
	FileName logpattern.FileNamePattern
	Uploader Uploader
}

// RouteRegistryは、一つのサービス／logger契約のspool rootに登録された送信経路の
// 順序付き一覧です。取得時点の登録内容を保持します。
type RouteRegistry []RouteRegistration

// RunReportは一周期の回復件数と、最大一件の再送結果です。
// File.Claimedがfalseなら、この周期では再送対象を取得していません。
type RunReport struct {
	RecoveredProcessingCount int
	File                     CompressedLogRunReport
	// RetryAfterMarkersは、この周期に通知が必要になった、宛先の待機期限を保存したmarkerの
	// 結果です。再起動前の期限の短縮と、markerの読み込み・保存・削除の失敗を含みます。
	RetryAfterMarkers []RetryAfterMarkerReport
}

// DeferredReasonは、spoolからの再送を後の周期へ保留した理由です。
type DeferredReason string

const (
	// DeferredReasonNoneは再送の保留がないことを表します。
	DeferredReasonNone DeferredReason = ""
	// DeferredReasonRouteUnavailableは、送信経路を確定できず再送を保留したことを表します。
	DeferredReasonRouteUnavailable DeferredReason = "route_unavailable"
	// DeferredReasonCircuitBreakerは、circuit breakerが送信を抑止したことを表します。
	DeferredReasonCircuitBreaker DeferredReason = "circuit_breaker"
	// DeferredReasonRetryableUploadFailureは、再試行可能な送信失敗により再送を保留したことを表します。
	DeferredReasonRetryableUploadFailure DeferredReason = "retryable_upload_failure"
	// DeferredReasonRetryAfterは、同じ宛先が以前の一時的な失敗で返したRetry-Afterの
	// 待機中のため、送信せずに再送を保留したことを表します。
	DeferredReasonRetryAfter DeferredReason = "retry_after"
)

// CompressedLogRunReportは、取得した一件のファイルとその再送先の処理結果です。
type CompressedLogRunReport struct {
	DestinationID objectstorage.DestinationID
	// Claimedは、この実行で再送対象をactiveからprocessingへ移動できたことを表します。
	Claimed  bool
	FileName string

	// UploadAttemptedは送信処理を開始したことを表します。
	UploadAttempted bool
	// UploadSucceededはuploadの成功を表します。doneへの状態遷移が失敗してもtrueです。
	UploadSucceeded bool
	// FailureCategoryは送信失敗の分類です。未試行・成功時は空です。
	FailureCategory uploader.FailureCategory

	// Stateはこの処理で最後に確認したファイルの配置状態です。未取得なら空です。
	// 移動に失敗した場合はprocessing、移動後のdirectory同期だけが失敗した場合は
	// 移動先の状態を返します。同期失敗はRunOnceのerrorで報告します。
	State pkgspool.State

	// DeferredReasonは再送の保留理由です。保留がなければDeferredReasonNoneです。
	// activeへ戻せたかどうかはStateで確認します。
	DeferredReason DeferredReason
	// DeferredUntilはcircuit breakerまたは送信先のRetry-Afterによる再送の保留期限です。
	// 送信失敗でbreakerが開いた場合やRetry-Afterを受け取った場合も設定し、
	// 両方あれば遅い方です。期限がなければゼロ値です。
	DeferredUntil time.Time
	// RetryAfterClampは、送信先が指定したRetry-Afterを上限まで短縮した場合の内容です。
	// 短縮しなかった場合はnilです。
	RetryAfterClamp *pkgspool.RetryAfterClamp
}
