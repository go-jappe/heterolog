package opensearch

import (
	"fmt"
	"net/http"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

const failureSampleLimit = 3

// BulkFailedRequest は、Bulk API の失敗 item と元の送信 request を対応づけた結果です。
// InputIndexは、OpenSearchが保証するrequestとresponse itemの同じ並び順を使って、
// 呼び出し時のreqs上の位置を保持します。同じ_idを持つrequestが複数あっても、後段は
// IDではなくこの位置で元の入力へ結果を戻せます。
type BulkFailedRequest struct {
	InputIndex  int
	Request     pkgopensearch.DocumentWriteRequest
	Status      int
	ErrorType   pkgopensearch.ErrorType
	ErrorReason string
}

// BulkFailureClassification は、BulkResponse 内の item 単位失敗を、確定した
// durable handling disposition ごとに分けた結果です。
//
// RetryableFailures は flush manager や spool sender が次の attempt に回す対象です。
// DeadFailures は構造的に信頼できるrequestがbackend側のterminal failureになった対象です。
// QuarantineFailures はrecord固有のpayload不正が確定し、そのまま自動再送してはならない対象です。
//
// StatusCounts は診断用の集計で、成功 item は基本的に含めません。
// ただし create op の 409 conflict は idempotent 成功としてfailure sliceへは
// 入れませんが、OpenSearch が返した失敗風 status の観測値として集計には残ります。
type BulkFailureClassification struct {
	RetryableFailures  []BulkFailedRequest
	DeadFailures       []BulkFailedRequest
	QuarantineFailures []BulkFailedRequest

	StatusCounts             map[int]int // 例: 429=12, 503=3, 400=1
	RetryableCount           int
	DeadFailedCount          int
	QuarantineFailedCount    int
	SampleDeadFailures       []string // エラー要約（先頭3件など）
	SampleQuarantineFailures []string // エラー要約（先頭3件など）
}

// ClassifyFailedBulkDocs は OpenSearch の BulkResponse を item 単位で読み、
// retryable、dead、quarantine に分類します。
//
// Bulk API は一度の response 内に成功、retryable、dead、quarantine が混在します。
// そのため HTTP リクエスト全体の成否だけではなく、response.Items の各 item を
// 送信時の DocumentWriteRequest に入力順で対応づけてから分類します。OpenSearchのBulk APIは
// response itemをrequestと同じ順序で返すため、_idを照合keyにはしません。reqsのIDは
// 空でない必要がありますが、同じ固定IDへの複数operationは正当なので重複を許容します。
//
// retryable 判定は internal/infrastructure/http/retryable の
// ClassifyRetryByStatus に寄せています。これにより 408/429/5xx などの
// HTTP status retry rule が transport retry wrapper と bulk item retry でずれないようにします。
//
// create op の 409 conflict は、呼び出し元が事前に _id を固定している前提では
// 「同じ document がすでに作成済み」とみなせるため、成功扱いとして全failure sliceから除外します。
// 一方で、item数の不一致、空item、missing _id/_index、同じ位置のrequestと異なる_id、
// action不一致、operation固有のstatus/result契約を外れた2xx応答は成否を確定できないため、
// terminal failureにはせずerrorを返します。responseの_indexはwrite aliasやdata streamが
// concrete/backing indexへ解決された名前になり得るため、requestとの完全一致は要求しません。
// resp.Errorsはitem単位の対応関係を保証しないため、falseの場合も全itemを検証します。
//
// 契約:
// - RetryableFailuresは、retryできる失敗したrequestだけを含みます。
// - DeadFailuresは、record payloadが安全でないことを証明しないterminal failureを含みます。
// - QuarantineFailuresは、既知のrecord固有のpayload failureを含みます。
// - 成功したrequestと冪等なcreateのconflictは、どのfailure sliceからも除外されます。
// - 不正な形式や不完全なresponseでは、terminalな結果を推測せずerrorを返します。
func ClassifyFailedBulkDocs(
	resp *pkgopensearch.BulkResponse,
	reqs []pkgopensearch.DocumentWriteRequest,
) (BulkFailureClassification, error) {
	result := BulkFailureClassification{
		StatusCounts: make(map[int]int),
	}

	if resp == nil {
		return result, ErrNilBulkResponse
	}

	if err := validateBulkResponseItems(resp.Items, reqs); err != nil {
		return result, err
	}

	for inputIndex, item := range resp.Items {
		classification := classifyBulkItem(inputIndex, item, reqs[inputIndex])
		result.merge(classification)
	}

	return result, nil
}

// validateBulkResponseItemsは、送信requestとresponse itemが1対1で対応することを検証します。
// 対応不能なresponseをterminal failureやsuccessへ推測すると、未配送recordをdeadへ送り、
// またはprocessing fileを削除する危険があるため、分類前にresponse全体を拒否します。
func validateBulkResponseItems(
	items []map[string]pkgopensearch.BulkItemResult,
	reqs []pkgopensearch.DocumentWriteRequest,
) error {
	if len(items) != len(reqs) {
		return &BulkResponseValidationError{
			Kind:      ErrBulkResponseItemCountMismatch,
			Index:     -1,
			GotItems:  len(items),
			WantItems: len(reqs),
		}
	}

	for i, item := range items {
		req := reqs[i]
		if req.Index == "" {
			return &BulkResponseValidationError{Kind: ErrBulkRequestMissingIndex, Index: i}
		}
		if req.ID == "" {
			return &BulkResponseValidationError{Kind: ErrBulkRequestMissingID, Index: i}
		}
		expectedAction, err := req.BulkActionKey()
		if err != nil {
			return &BulkResponseValidationError{Kind: ErrBulkRequestInvalidOperation, Index: i, Err: err}
		}

		action, result, ok := firstBulkAction(item)
		if !ok {
			return &BulkResponseValidationError{Kind: ErrBulkResponseItemActionCount, Index: i}
		}
		if result.ID == "" {
			return &BulkResponseValidationError{Kind: ErrBulkResponseItemMissingID, Index: i}
		}
		if result.Index == "" {
			return &BulkResponseValidationError{Kind: ErrBulkResponseItemMissingIndex, Index: i}
		}
		if result.Status < 200 || result.Status > 599 {
			return &BulkResponseValidationError{Kind: ErrBulkResponseItemInvalidStatus, Index: i, Status: result.Status}
		}
		if result.Status < 300 && result.Error != nil {
			return &BulkResponseValidationError{Kind: ErrBulkResponseItemSuccessWithError, Index: i, Status: result.Status}
		}
		if result.ID != req.ID {
			return &BulkResponseValidationError{
				Kind:   ErrBulkResponseItemIDMismatch,
				Index:  i,
				GotID:  result.ID,
				WantID: req.ID,
			}
		}
		if action != expectedAction {
			return &BulkResponseValidationError{
				Kind:       ErrBulkResponseItemActionMismatch,
				Index:      i,
				WantID:     req.ID,
				GotAction:  action,
				WantAction: expectedAction,
			}
		}
		if result.Status < 300 {
			if err := validateBulkSuccessResult(i, expectedAction, result); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateBulkSuccessResultは2xx itemがoperation固有の正常応答を満たすか検証します。
// 壊れたresponseをsuccessと推測すると未配送recordを失うため、statusとresultの組を
// OpenSearchのIndex/Bulk API契約どおりに限定します。
func validateBulkSuccessResult(
	inputIndex int,
	action string,
	result pkgopensearch.BulkItemResult,
) error {
	valid := false
	// 設定されたingest pipelineは、どちらのwrite operationもerrorなしでdropすることがあります。
	// 元のactionとdocumentの識別情報は、上で検証済みです。
	droppedByPipeline := result.Status == http.StatusOK && result.Result == "noop"
	switch action {
	case string(pkgopensearch.WriteOpCreate):
		valid = droppedByPipeline || (result.Status == http.StatusCreated && result.Result == "created")
	case string(pkgopensearch.WriteOpIndex):
		valid = droppedByPipeline || (result.Status == http.StatusOK && result.Result == "updated") ||
			(result.Status == http.StatusCreated && result.Result == "created")
	}
	if valid {
		return nil
	}

	return &BulkResponseValidationError{
		Kind:       ErrBulkResponseItemInvalidSuccessResult,
		Index:      inputIndex,
		WantAction: action,
		Status:     result.Status,
		Result:     result.Result,
	}
}

// classifyBulkItem は BulkResponse.Items の 1 要素を分類します。
//
// OpenSearch の bulk item は {"create": {...}} のように action 名を key にした
// 1 要素 map で返るため、まず action と result を取り出し、
// 同じ位置の元requestと対応づけてからretryable/dead/quarantineを決めます。
func classifyBulkItem(
	inputIndex int,
	item map[string]pkgopensearch.BulkItemResult,
	req pkgopensearch.DocumentWriteRequest,
) BulkFailureClassification {
	result := BulkFailureClassification{
		StatusCounts: make(map[int]int),
	}

	// 呼び出し元がresponse全体を検証済みなので、actionとrequest対応は必ず存在します。
	_, bulkResult, _ := firstBulkAction(item)

	if !isFailedBulkResult(bulkResult) {
		return result
	}

	result.StatusCounts[bulkResult.Status]++

	if isIdempotentCreateConflictStatus(req, bulkResult.Status) {
		return result
	}

	errorType, reason := bulkErrorInfo(bulkResult)

	failed := BulkFailedRequest{
		InputIndex:  inputIndex,
		Request:     req,
		Status:      bulkResult.Status,
		ErrorType:   errorType,
		ErrorReason: reason,
	}

	if retryable.ClassifyRetryByStatus(bulkResult.Status, 0).Retry {
		result.RetryableFailures = append(result.RetryableFailures, failed)
		result.RetryableCount++
		return result
	}

	if IsRecordPayloadErrorType(errorType) {
		result.QuarantineFailures = append(result.QuarantineFailures, failed)
		result.QuarantineFailedCount++
		if len(result.SampleQuarantineFailures) < failureSampleLimit {
			result.SampleQuarantineFailures = append(
				result.SampleQuarantineFailures,
				summarizeBulkItemFailure(bulkResult),
			)
		}
		return result
	}

	result.DeadFailures = append(result.DeadFailures, failed)
	result.DeadFailedCount++
	if len(result.SampleDeadFailures) < failureSampleLimit {
		result.SampleDeadFailures = append(result.SampleDeadFailures, summarizeBulkItemFailure(bulkResult))
	}

	return result
}

// firstBulkAction は bulk item の action 名と結果を取り出します。
// OpenSearch の各 item は1 actionだけを含む必要があり、それ以外は不正として返します。
func firstBulkAction(item map[string]pkgopensearch.BulkItemResult) (string, pkgopensearch.BulkItemResult, bool) {
	if len(item) != 1 {
		return "", pkgopensearch.BulkItemResult{}, false
	}
	for action, result := range item {
		return action, result, true
	}
	return "", pkgopensearch.BulkItemResult{}, false
}

// isFailedBulkResult は、status または error object のどちらかで失敗 item かを判定します。
// 一部の異常 response では error が nil でも 3xx 以上の status が返る可能性があるため、
// status と error の両方を見ています。
func isFailedBulkResult(result pkgopensearch.BulkItemResult) bool {
	return result.Status >= 300 || result.Error != nil
}

// bulkErrorInfo は BulkItemResult の error object から、保存・通知向けの type/reason を取り出します。
// error object が無い失敗 response では空文字を返し、status 側の情報だけを残します。
func bulkErrorInfo(r pkgopensearch.BulkItemResult) (pkgopensearch.ErrorType, string) {
	if r.Error == nil {
		return pkgopensearch.ErrorType(""), ""
	}
	return r.Error.Type, r.Error.Reason
}

// merge は item ごとの分類結果を response 全体の分類結果に合成します。
// failure sample はログや通知が大きくなりすぎないよう、各分類で上限件数だけ残します。
func (c *BulkFailureClassification) merge(other BulkFailureClassification) {
	c.RetryableFailures = append(c.RetryableFailures, other.RetryableFailures...)
	c.DeadFailures = append(c.DeadFailures, other.DeadFailures...)
	c.QuarantineFailures = append(c.QuarantineFailures, other.QuarantineFailures...)
	c.RetryableCount += other.RetryableCount
	c.DeadFailedCount += other.DeadFailedCount
	c.QuarantineFailedCount += other.QuarantineFailedCount

	for status, count := range other.StatusCounts {
		c.StatusCounts[status] += count
	}

	for _, sample := range other.SampleDeadFailures {
		if len(c.SampleDeadFailures) >= failureSampleLimit {
			break
		}
		c.SampleDeadFailures = append(c.SampleDeadFailures, sample)
	}
	for _, sample := range other.SampleQuarantineFailures {
		if len(c.SampleQuarantineFailures) >= failureSampleLimit {
			break
		}
		c.SampleQuarantineFailures = append(c.SampleQuarantineFailures, sample)
	}
}

// summarizeBulkItemFailure は永久失敗の調査用に、bulk item の失敗を短く要約します。
func summarizeBulkItemFailure(r pkgopensearch.BulkItemResult) string {
	if r.Error == nil {
		return fmt.Sprintf("status=%d type= reason=", r.Status)
	}
	return fmt.Sprintf("status=%d type=%s reason=%s", r.Status, r.Error.Type, r.Error.Reason)
}
