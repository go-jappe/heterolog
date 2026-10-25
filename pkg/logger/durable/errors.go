package durable

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// DurableTypeErrorは、durable store選択の不正です。ValidateFailureStoreDurableTypesと
// ValidateDeliveryFailureStoreDurableTypesが返す、failurestore.DurableTypeErrorと同じ型です。
type DurableTypeError = failurestore.DurableTypeError

var (
	// ErrDurableTypesRequiredは、failurestore.ErrDurableTypesRequiredと同じsentinelです。
	ErrDurableTypesRequired = failurestore.ErrDurableTypesRequired
	// ErrDurableTypeNoneNotAloneは、failurestore.ErrDurableTypeNoneNotAloneと同じsentinelです。
	ErrDurableTypeNoneNotAlone = failurestore.ErrDurableTypeNoneNotAlone
	// ErrUnsupportedDurableTypeは、failurestore.ErrUnsupportedDurableTypeと同じsentinelです。
	ErrUnsupportedDurableType = failurestore.ErrUnsupportedDurableType
	// ErrDuplicateDurableTypeは、failurestore.ErrDuplicateDurableTypeと同じsentinelです。
	ErrDuplicateDurableType = failurestore.ErrDuplicateDurableType
)

var (
	// ErrInvalidFailureHandlerMaxConcurrentは、failure handlerの同時実行数が0以下であることを
	// 表します。
	ErrInvalidFailureHandlerMaxConcurrent = errors.New("failure handler max concurrent must be positive")
	// ErrNegativeSpoolActiveBacklogWarnAfterDaysは、active spoolの滞留警告を開始する
	// 経過日数が負値であることを表します。
	ErrNegativeSpoolActiveBacklogWarnAfterDays = errors.New(
		"spool active backlog warn-after days must not be negative",
	)
)

var (
	// ErrSpillTypesRequiredは、直接退避先の選択が一つもないことを表します。
	ErrSpillTypesRequired = errors.New("direct degrade spill types are required")
	// ErrSpillTypeNoneNotAloneは、SpillTypeNoneがほかのspill typeと同時に選択されたことを
	// 表します。
	ErrSpillTypeNoneNotAlone = errors.New(`direct degrade spill type "none" must be specified alone`)
	// ErrSpillTypeNoneWithDegradeは、直接退避が有効なのにSpillTypeNoneが選択されたことを
	// 表します。SpillTypeNoneは直接退避を無効にする場合だけ指定できます。
	ErrSpillTypeNoneWithDegrade = errors.New(
		`direct degrade spill type "none" requires degrade to be disabled`,
	)
	// ErrUnsupportedSpillTypeは、未実装の予約値または未知の値がspill typeに選択されたことを
	// 表します。
	ErrUnsupportedSpillType = errors.New("unsupported direct degrade spill type")
	// ErrDuplicateSpillTypeは、同じspill typeが複数回選択されたことを表します。
	ErrDuplicateSpillType = errors.New("duplicate direct degrade spill type")
)

// SpillTypeErrorは、直接退避先の選択が不正であることを表します。不正の種類はErrで、
// errors.Isで判別できます。
type SpillTypeError struct {
	// Typeは不正なspill typeです。ErrSpillTypesRequiredでは空です。
	Type SpillType
	// Errは、ErrSpillTypesRequired、ErrSpillTypeNoneNotAlone、ErrSpillTypeNoneWithDegrade、
	// ErrUnsupportedSpillType、ErrDuplicateSpillTypeのいずれかです。
	Err error
}

// Errorは、不正の種類に応じて、Typeを含む説明を返します。
func (e *SpillTypeError) Error() string {
	switch e.Err {
	case ErrSpillTypesRequired:
		return "direct degrade spill types are required"
	case ErrSpillTypeNoneNotAlone:
		return fmt.Sprintf("direct degrade spill type %q must be specified alone", e.Type)
	case ErrSpillTypeNoneWithDegrade:
		return fmt.Sprintf("direct degrade spill type %q requires degrade to be disabled", e.Type)
	case ErrUnsupportedSpillType:
		return fmt.Sprintf("unsupported direct degrade spill type: %q", e.Type)
	case ErrDuplicateSpillType:
		return fmt.Sprintf("duplicate direct degrade spill type: %q", e.Type)
	default:
		return fmt.Sprintf("invalid direct degrade spill type %q: %v", e.Type, e.Err)
	}
}

// Unwrapは、不正の種類を表すErrを返します。
func (e *SpillTypeError) Unwrap() error { return e.Err }

// validationErrorは、従来のmessageを保ったまま、errors.Isで種類を判別できる検証errorです。
type validationError struct {
	kind    error
	message string
}

func (e *validationError) Error() string { return e.message }

func (e *validationError) Unwrap() error { return e.kind }

func newValidationError(kind error, format string, args ...any) error {
	return &validationError{kind: kind, message: fmt.Sprintf(format, args...)}
}
