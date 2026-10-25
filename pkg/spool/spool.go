package spool

import (
	"encoding/json"
	"errors"
	"time"
)

type State string

const (
	StateActive     State = "active"
	StateProcessing State = "processing"
	StateDone       State = "done"
	// StateDead stores records whose representation can be trusted and whose
	// delivery result is confirmed terminal. Investigation starts with the
	// backend, credentials, destination configuration, or time policy rather
	// than with spool data integrity.
	StateDead State = "dead"
	// StateQuarantine isolates records or files that cannot be trusted for
	// unchanged automatic replay. They require data inspection, repair,
	// conversion, or a manual decision before being reintroduced.
	StateQuarantine State = "quarantine"
)

func (s State) IsValid() bool {
	switch s {
	case StateActive, StateProcessing, StateDone, StateDead, StateQuarantine:
		return true
	default:
		return false
	}
}

// QuarantineReasonは、processing file全体をrecord単位に分解せず隔離した理由です。
// 値はquarantine先のfile名にも保存されるため、運用者は元fileをdecodeできない場合でも
// 最初に調べるべき障害箇所を識別できます。
type QuarantineReason string

const (
	// QuarantineReasonInvalidSpoolFileは、NDJSON破損、空file、必須field欠落など、
	// spool fileそのものをrecordとして復元できなかったことを表します。
	QuarantineReasonInvalidSpoolFile QuarantineReason = "invalid-spool-file"
	// QuarantineReasonInvalidExportReportは、backend送信後のrecord別結果に欠落・重複などがあり、
	// 元fileを安全に分配できなかったことを表します。
	QuarantineReasonInvalidExportReport QuarantineReason = "invalid-export-report"
	// QuarantineReasonInvalidTransitionStateは、送信後の状態遷移retry情報が壊れており、
	// 再開位置を安全に決定できなかったことを表します。
	QuarantineReasonInvalidTransitionState QuarantineReason = "invalid-transition-state"
)

func (r QuarantineReason) IsValid() bool {
	switch r {
	case QuarantineReasonInvalidSpoolFile,
		QuarantineReasonInvalidExportReport,
		QuarantineReasonInvalidTransitionState:
		return true
	default:
		return false
	}
}

// invalidSpoolFileDetail は、ErrInvalidSpoolFile の下位分類として扱う error。
// これにより、例えば ErrMalformedSpoolJSON で wrap された error でも、
//
//	errors.Is(err, ErrInvalidSpoolFile)
//
// が true になる。
type invalidSpoolFileDetail string

func (e invalidSpoolFileDetail) Error() string {
	return string(e)
}

func (e invalidSpoolFileDetail) Is(target error) bool {
	return target == ErrInvalidSpoolFile
}

// spoolOperationDetail は、ErrSpoolOperation の下位分類として扱う error。
// これにより、例えば ErrReadProcessingSpoolFile で wrap された error でも、
//
//	errors.Is(err, ErrSpoolOperation)
//
// が true になる。
type spoolOperationDetail string

func (e spoolOperationDetail) Error() string {
	return string(e)
}

func (e spoolOperationDetail) Is(target error) bool {
	return target == ErrSpoolOperation
}

var (
	// ErrInvalidSpoolFile は、spool file 自体が処理不能な形式であることを表す。
	//
	// worker はこの error を検知した場合、その processing file を
	// quarantine へ隔離してよい。
	//
	// 例:
	// - NDJSON として壊れている
	// - JSON decode できない行がある
	// - SpoolRecord として復元できない
	// - 必須項目が欠落している
	ErrInvalidSpoolFile = errors.New("invalid spool file")

	// ErrEmptySpoolFile は、spool file が空であることを表す。
	//
	// 空ファイルは通常の再送対象ではないため、基本的には
	// ErrInvalidSpoolFile と同じく quarantine 対象にする。
	ErrEmptySpoolFile error = invalidSpoolFileDetail("empty spool file")

	// ErrMalformedSpoolJSON は、spool file 内の JSON / NDJSON が壊れていることを表す。
	//
	// 例:
	// - 1行が JSON として decode できない
	// - NDJSON の途中行が破損している
	ErrMalformedSpoolJSON error = invalidSpoolFileDetail("malformed spool json")

	// ErrInvalidSpoolRecord は、JSON としては読めたが SpoolRecord として不正なことを表す。
	//
	// 例:
	// - OccurredAt が zero
	// - Payload が空
	// - Payload が JSON として不正
	ErrInvalidSpoolRecord error = invalidSpoolFileDetail("invalid spool record")

	// ErrInvalidSpoolState は、未知または不正な spool state を表す。
	//
	// 例:
	// - "active", "processing", "done", "dead", "quarantine" 以外の state
	ErrInvalidSpoolState = errors.New("invalid spool state")

	// ErrSpoolOperation は、spool に対する操作失敗の親 error。
	//
	// I/O、path validation、state 移動、split 書き込みなど、
	// spool file の形式不正ではなく、操作として失敗した error の分類に使う。
	ErrSpoolOperation = errors.New("spool operation failed")

	// ErrListProcessingSpoolFiles は、processing 配下の spool file 一覧取得に失敗したことを表す。
	ErrListProcessingSpoolFiles error = spoolOperationDetail("list processing spool files")

	// ErrValidateProcessingSpoolFilePath は、processing spool file path の検証に失敗したことを表す。
	ErrValidateProcessingSpoolFilePath error = spoolOperationDetail("validate processing spool file path")

	// ErrReadProcessingSpoolFile は、processing spool file の読み込みに失敗したことを表す。
	//
	// これは I/O error の分類であり、spool file の形式不正を意味しない。
	ErrReadProcessingSpoolFile error = spoolOperationDetail("read processing spool file")

	// ErrDecodeProcessingSpoolFile は、processing spool file の decode に失敗したことを表す。
	//
	// 下位 error として ErrInvalidSpoolFile 系が含まれる場合、
	// worker はその file を quarantine へ隔離してよい。
	ErrDecodeProcessingSpoolFile error = spoolOperationDetail("decode processing spool file")

	// ErrMoveProcessingSpoolFileToDone は、processing spool file の done 移動に失敗したことを表す。
	ErrMoveProcessingSpoolFileToDone error = spoolOperationDetail("move processing spool file to done")

	// ErrMoveProcessingSpoolFileToDead は、processing spool file の dead 移動に失敗したことを表す。
	ErrMoveProcessingSpoolFileToDead error = spoolOperationDetail("move processing spool file to dead")

	// ErrMoveProcessingSpoolFileToQuarantine は、processing spool file の quarantine 移動に失敗したことを表す。
	ErrMoveProcessingSpoolFileToQuarantine error = spoolOperationDetail("move processing spool file to quarantine")

	// ErrRemoveProcessingSpoolFile は、processing spool file の削除に失敗したことを表す。
	ErrRemoveProcessingSpoolFile error = spoolOperationDetail("remove processing spool file")

	// ErrStoreProcessingRecordsToDone は、processing file の一部 records を done に再退避する処理に失敗したことを表す。
	ErrStoreProcessingRecordsToDone error = spoolOperationDetail("store processing records to done")

	// ErrStoreProcessingRecordsToDead は、processing file の一部 records を dead に再退避する処理に失敗したことを表す。
	ErrStoreProcessingRecordsToDead error = spoolOperationDetail("store processing records to dead")

	// ErrStoreProcessingRecordsToQuarantine は、processing file の一部 records を quarantine に再退避する処理に失敗したことを表す。
	ErrStoreProcessingRecordsToQuarantine error = spoolOperationDetail("store processing records to quarantine")

	// ErrInvalidProcessingSplitDestinationState は、processing file の split 保存先として不正な state を表す。
	//
	// State としては valid でも、processing split の保存先として許可しない state があるため、
	// ErrInvalidSpoolState とは分ける。
	ErrInvalidProcessingSplitDestinationState error = spoolOperationDetail("invalid processing split destination state")

	// ErrResolveDestinationSpoolStateDir は、split 保存先 state dir の解決に失敗したことを表す。
	ErrResolveDestinationSpoolStateDir error = spoolOperationDetail("resolve destination spool state dir")

	// ErrBuildSpoolNDJSON は、SpoolRecord から NDJSON body を構築できなかったことを表す。
	ErrBuildSpoolNDJSON error = spoolOperationDetail("build spool ndjson")

	// ErrWriteSplitSpoolFile は、split 後の spool file 書き込みに失敗したことを表す。
	ErrWriteSplitSpoolFile error = spoolOperationDetail("write split spool file")
)

// spool に保存する 1 件分のレコード。
// 再送待ち・一時退避・dead/quarantine 退避などの共通保存形式として扱う。
type SpoolRecord struct {
	OccurredAt  time.Time // sender から渡された時刻
	FinalizedAt time.Time // done, dead, quarantine に移動した時刻

	FirstResendAttemptedAt time.Time
	LastResendAttemptedAt  time.Time
	ResendAttemptCount     int

	// NotBeforeは、このrecordをbackendへ再送してよい最も早い時刻です。backendが
	// Retry-Afterで待機を指定した場合に、Workerが再送待ちへ戻すrecordへ保存します。
	// zero valueは制限なしを表し、このfieldを持たない既存のspool fileも従来どおり再送します。
	// 期限を過ぎた値は再送を制限しないため、done・dead・quarantineへの保存時も変更しません。
	// 読み込んだ時点から再送待機の上限より先の値は、その時点から上限までに短縮します。
	NotBefore time.Time `json:",omitzero"`

	RequestID string          // 相関・追跡用の識別子。再送自体には不要
	Payload   json.RawMessage // backend への再送に利用する本文

	// InitialFailureOperationからInitialFailureReasonまでは、recordが最初にspoolへ
	// 保存された失敗を保持します。LastFailureReasonは再送で得た最新の失敗理由へ更新されます。
	// いずれも調査用であり、再送payloadの生成には使用しません。
	InitialFailureOperation  string `json:",omitempty"`
	InitialFailureStatusCode int    `json:",omitempty"`
	InitialFailureCode       string `json:",omitempty"`
	InitialFailureReason     string `json:",omitempty"`
	LastFailureReason        string `json:",omitempty"`
}
