package opensearch

import (
	"errors"
	"fmt"
)

// ErrNilBulkResponseは、Bulk APIの応答がnilのため、record別の結果を確定できないことを表します。
var ErrNilBulkResponse = errors.New("resp is nil")

// ErrInvalidBulkResponseは、Bulk APIの応答を送信requestと対応付けられないため、record別の結果を
// 確定できないことを表します。実際に返すerrorはBulkResponseValidationErrorで、Kindで不正の種類を
// 判定できます。
var ErrInvalidBulkResponse = errors.New("invalid opensearch bulk response")

// BulkResponseValidationErrorのKindです。
var (
	// ErrBulkResponseItemCountMismatchは、response itemとrequestの件数が一致しないことを表します。
	ErrBulkResponseItemCountMismatch = errors.New("bulk response item count mismatch")
	// ErrBulkRequestMissingIndexは、送信requestのindexが空であることを表します。
	ErrBulkRequestMissingIndex = errors.New("bulk request is missing index")
	// ErrBulkRequestMissingIDは、送信requestのidが空であることを表します。
	ErrBulkRequestMissingID = errors.New("bulk request is missing id")
	// ErrBulkRequestInvalidOperationは、送信requestのoperationからBulk actionを決められないことを表します。
	ErrBulkRequestInvalidOperation = errors.New("bulk request has invalid operation")
	// ErrBulkResponseItemActionCountは、response itemが空か、複数のactionを含むことを表します。
	ErrBulkResponseItemActionCount = errors.New("bulk response item is empty or contains multiple actions")
	// ErrBulkResponseItemMissingIDは、response itemの_idが空であることを表します。
	ErrBulkResponseItemMissingID = errors.New("bulk response item is missing _id")
	// ErrBulkResponseItemMissingIndexは、response itemの_indexが空であることを表します。
	ErrBulkResponseItemMissingIndex = errors.New("bulk response item is missing _index")
	// ErrBulkResponseItemInvalidStatusは、response itemのstatusがHTTP statusの範囲外であることを表します。
	ErrBulkResponseItemInvalidStatus = errors.New("bulk response item has invalid HTTP status")
	// ErrBulkResponseItemSuccessWithErrorは、成功statusのresponse itemがerror objectを持つことを表します。
	ErrBulkResponseItemSuccessWithError = errors.New("bulk response item has success status with error object")
	// ErrBulkResponseItemIDMismatchは、response itemの_idが同じ位置のrequestと異なることを表します。
	ErrBulkResponseItemIDMismatch = errors.New("bulk response item _id mismatch")
	// ErrBulkResponseItemActionMismatchは、response itemのactionが同じ位置のrequestと異なることを表します。
	ErrBulkResponseItemActionMismatch = errors.New("bulk response item action mismatch")
	// ErrBulkResponseItemInvalidSuccessResultは、成功statusとresultの組がoperationの契約に合わないことを表します。
	ErrBulkResponseItemInvalidSuccessResult = errors.New("bulk response item has invalid success result")
)

// BulkResponseValidationErrorは、Bulk APIの応答を送信requestと対応付けられない理由を表します。
// errors.Is(err, ErrInvalidBulkResponse)とerrors.Is(err, Kind)で判定できます。
type BulkResponseValidationError struct {
	// Kindは不正の種類です。
	Kind error
	// Indexは、不正を検出したrequestとresponse itemの位置です。件数の不一致では-1です。
	Index int
	// GotItemsとWantItemsは、件数の不一致でのresponse item数とrequest数です。
	GotItems  int
	WantItems int
	// Statusは、response itemのHTTP statusです。
	Status int
	// GotIDとWantIDは、response itemの_idと同じ位置のrequestのidです。
	GotID  string
	WantID string
	// GotActionとWantActionは、response itemのactionとrequestから決まるactionです。
	GotAction  string
	WantAction string
	// Resultは、成功status itemのresultです。
	Result string
	// Errは、requestのoperationを解釈できなかった原因です。
	Err error
}

func (e *BulkResponseValidationError) Error() string {
	switch e.Kind {
	case ErrBulkResponseItemCountMismatch:
		return fmt.Sprintf("bulk response item count mismatch: got=%d want=%d", e.GotItems, e.WantItems)
	case ErrBulkRequestMissingIndex:
		return fmt.Sprintf("reqs[%d] missing index", e.Index)
	case ErrBulkRequestMissingID:
		return fmt.Sprintf("reqs[%d] missing id", e.Index)
	case ErrBulkRequestInvalidOperation:
		return fmt.Sprintf("reqs[%d] has invalid operation: %v", e.Index, e.Err)
	case ErrBulkResponseItemActionCount:
		return fmt.Sprintf("response items[%d] is empty or contains multiple actions", e.Index)
	case ErrBulkResponseItemMissingID:
		return fmt.Sprintf("response items[%d] is missing _id", e.Index)
	case ErrBulkResponseItemMissingIndex:
		return fmt.Sprintf("response items[%d] is missing _index", e.Index)
	case ErrBulkResponseItemInvalidStatus:
		return fmt.Sprintf("response items[%d] has invalid HTTP status: %d", e.Index, e.Status)
	case ErrBulkResponseItemSuccessWithError:
		return fmt.Sprintf("response items[%d] has success status %d with error object", e.Index, e.Status)
	case ErrBulkResponseItemIDMismatch:
		return fmt.Sprintf("response items[%d] _id mismatch: got=%q want=%q", e.Index, e.GotID, e.WantID)
	case ErrBulkResponseItemActionMismatch:
		return fmt.Sprintf(
			"response items[%d] action mismatch for _id=%q: got=%q want=%q",
			e.Index, e.WantID, e.GotAction, e.WantAction,
		)
	case ErrBulkResponseItemInvalidSuccessResult:
		return fmt.Sprintf(
			"response items[%d] has invalid success result for action %q: status=%d result=%q",
			e.Index, e.WantAction, e.Status, e.Result,
		)
	default:
		return fmt.Sprintf("%v: response items[%d]: %v", ErrInvalidBulkResponse, e.Index, e.Kind)
	}
}

// Isは、ErrInvalidBulkResponseとKindに一致します。
func (e *BulkResponseValidationError) Is(target error) bool {
	return target == ErrInvalidBulkResponse || (e.Kind != nil && target == e.Kind)
}

// Unwrapは、requestのoperationを解釈できなかった原因を返します。
func (e *BulkResponseValidationError) Unwrap() error { return e.Err }
