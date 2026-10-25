package worker

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
)

type ProcessSkipReason string

const (
	ProcessSkipReasonProcessingFileTransitionRetryFull ProcessSkipReason = "processing_file_transition_retry_full"

	// ProcessSkipReasonRetryAfterは、backendがRetry-Afterで指定した時刻まで、
	// この送信先への送信を控えたことを表します。
	ProcessSkipReasonRetryAfter ProcessSkipReason = "retry_after"
)

// ProcessReport は、通常の spool file process の1回分の結果を表す。
//
// 通常は、1つの processing file を読み込み、backend send した処理結果を表す。
//
// ただし、processing file transition retry の滞留などにより通常の read -> send を開始しなかった場合は、
// ProcessingPath や RecordCount は空のまま、SkipReason に中断理由を入れる。
// Retry-Afterの期限を保存したrecordを読み込んで送信を控えた場合は、ProcessingPathと
// RecordCountにそのfileを入れる。fileはprocessingに残り、期限後に送信する。
//
// processing file transition retry 自体の処理結果は含めない。
type ProcessReport struct {
	// TargetBackendService は、処理対象の backend service 名。
	//
	// 例: "opensearch", "loki"
	TargetBackendService string

	// Status は、この report 単体で見たときの通常 spool 処理結果を表す。
	// no_target or skipped or completed or quarantined or failed
	Status ProcessStatus

	// SkipReason は、通常の read -> send 処理を開始しなかった理由。
	//
	// 空の場合は skip していない。
	// これは error ではなく、worker の制御上の抑止理由を表す。
	SkipReason ProcessSkipReason

	// ProcessingPath は、今回処理した processing file の path。
	//
	// active file を claim した場合も、claim 後の processing path を入れる。
	// 正常に処理が完了した場合、この path の file は report 返却時点で
	// すでに done / dead / quarantine へ移動、または削除されている可能性がある。
	ProcessingPath string

	// RecordCount は、processing file から読み込んだ record 数。
	RecordCount int

	// SuccessRecordCount は、backend send に成功した record 数。
	SuccessRecordCount int

	// DeadRecordCountは、表現を信頼できるterminal failureとしてdead扱いにしたrecord数。
	DeadRecordCount int

	// QuarantineRecordCountは、payload不正や分類不能などにより、そのままの自動再送が
	// 安全でないと判断し、quarantineを後処理先として決定したrecord数。
	// 保存完了件数ではないため、quarantine移動が失敗したreportにも設定される。
	QuarantineRecordCount int

	// QuarantineReasonはfile全体を隔離した場合の理由です。record別のquarantineでは、
	// 各SpoolRecordのfailure metadataが理由を保持するためzero valueです。
	QuarantineReason spool.QuarantineReason

	// RetryRecordCount は、backendが再送可能と判定したrecordや、
	// context終了・未送信によりactiveへ戻したrecord数。
	RetryRecordCount int

	// NotBeforeは、backendがRetry-Afterで指定した、この送信先へ次に送信してよい時刻です。
	// SkipReasonがretry_afterの場合は送信を控えた期限、この周期の送信結果に
	// Retry-Afterが含まれた場合は記録した期限を入れます。zero valueは待機なしを表します。
	NotBefore time.Time

	// RetryAfterClampは、backendが指定したRetry-After、または保存済みの再送下限時刻を
	// 上限まで短縮した場合の内容です。短縮しなかった周期ではnilです。
	RetryAfterClamp *spool.RetryAfterClamp

	// RetryAfterMarkerErrは、送信先の待機期限の読み込み・保存・削除でRetryAfterStoreが
	// 返したerrorです。警告であり、StatusやErr、戻り値のerrorには含めません。
	// 保存に失敗した期限もmemoryで保持して待機を続け、期限が残る間は以後の周期で保存を
	// 再試行します。保存の失敗は一連の失敗につき一度だけ設定します。保存できないまま
	// 再起動した場合は、期限が引き継がれない場合があります。
	// 読み込みに失敗した場合は、保存済みの期限がないものとして送信を続けます。
	RetryAfterMarkerErr error

	// Err は、通常処理中に発生した error。
	//
	// nil の場合は、error は発生していない。
	// SkipReason が設定されている場合も、制御上の中断であれば Err は nil のままでよい。
	Err error
}

// ProcessingFileTransitionRetryReport は、失敗していたprocessing fileの
// 状態遷移をretryした結果を表します。
//
// backend sendは行わず、Operationに対応するstore / move / removeだけを再試行します。
type ProcessingFileTransitionRetryReport struct {
	TargetBackendService string

	// no_target or succeeded or failed
	Status ProcessingFileTransitionRetryStatus

	ProcessingPath string

	Operation ProcessingFileTransitionRetryOp

	Err error
}

// ProcessingFileTransitionRetryStatus は、状態遷移retryを1件処理した結果です。
type ProcessingFileTransitionRetryStatus string

const (
	ProcessingFileTransitionRetryStatusNoTarget  ProcessingFileTransitionRetryStatus = "no_target"
	ProcessingFileTransitionRetryStatusSucceeded ProcessingFileTransitionRetryStatus = "succeeded"
	ProcessingFileTransitionRetryStatusFailed    ProcessingFileTransitionRetryStatus = "failed"
)
