package durable

import (
	"slices"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// ValidateFailureHandlerMaxConcurrentは、failure handlerが同時に実行できるchild処理数を
// 検証します。0以下を暗黙のdefaultへ置き換えると設定漏れを隠すため、正の値だけを
// 受け付けます。0以下の値はErrInvalidFailureHandlerMaxConcurrentを持つerrorです。
func ValidateFailureHandlerMaxConcurrent(maxConcurrent int) error {
	if maxConcurrent <= 0 {
		return newValidationError(
			ErrInvalidFailureHandlerMaxConcurrent,
			"failure handler max concurrent must be positive: %d", maxConcurrent,
		)
	}
	return nil
}

// ValidateSpoolActiveBacklogWarnAfterDaysは、active spoolの滞留警告を
// 開始する経過日数を検証します。0は警告を無効にする明示的な値として
// 受け付け、負数は設定エラーにします。この値はspoolへの保存可否には使用しません。
// 負数はErrNegativeSpoolActiveBacklogWarnAfterDaysを持つerrorです。
func ValidateSpoolActiveBacklogWarnAfterDays(warnAfterDays int) error {
	if warnAfterDays < 0 {
		return newValidationError(
			ErrNegativeSpoolActiveBacklogWarnAfterDays,
			"spool active backlog warn-after days must not be negative: %d", warnAfterDays,
		)
	}
	return nil
}

// ValidateFailureStoreDurableTypesは通常failure処理の保存先選択を検証します。
// noneはspoolとfile archiveをどちらも使用しない明示値として単独指定だけを受け付け、
// 実保存先はfileArchiveとspoolを一度ずつ選択できます。不正な選択はScopeが
// "failure store"の*DurableTypeErrorです。
func ValidateFailureStoreDurableTypes(types []DurableType) error {
	return failurestore.ValidateDurableTypes(
		"failure store",
		types,
		DurableTypeFileArchive,
		DurableTypeSpool,
	)
}

// ValidateDeliveryFailureStoreDurableTypesはDelivery failure storeで選択された
// 保存方式を検証します。noneは保存先なし、実保存先はfileArchiveだけを選択できます。
// 不正な選択はScopeが"delivery failure store"の*DurableTypeErrorです。
func ValidateDeliveryFailureStoreDurableTypes(types []DurableType) error {
	return failurestore.ValidateDurableTypes(
		"delivery failure store",
		types,
		DurableTypeFileArchive,
	)
}

// ValidateDirectDegradeSpillTypesはfailure storeを使わない直接退避先を検証します。
// 現在実装済みのstdoutを一度だけ選択でき、空・予約値・未知値・重複は設定エラーです。
// 不正な選択は*SpillTypeErrorです。
func ValidateDirectDegradeSpillTypes(types []SpillType) error {
	if len(types) == 0 {
		return &SpillTypeError{Err: ErrSpillTypesRequired}
	}
	// noneの併用は、個々の値の対応可否や重複より先に報告します。
	if len(types) > 1 && slices.Contains(types, SpillTypeNone) {
		return &SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone}
	}

	seen := make(map[SpillType]struct{}, len(types))
	for _, spillType := range types {
		if spillType == SpillTypeNone {
			return &SpillTypeError{Type: spillType, Err: ErrSpillTypeNoneWithDegrade}
		}
		if spillType != SpillType(SpillDestinationStdout) {
			return &SpillTypeError{Type: spillType, Err: ErrUnsupportedSpillType}
		}
		if _, duplicated := seen[spillType]; duplicated {
			return &SpillTypeError{Type: spillType, Err: ErrDuplicateSpillType}
		}
		seen[spillType] = struct{}{}
	}
	return nil
}
