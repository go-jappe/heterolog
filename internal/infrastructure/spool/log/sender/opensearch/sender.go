package opensearch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

// OpensearchSpoolSenderの構築、spool payloadの検証、Bulk応答の分類で返すerrorの種類です。
var (
	// ErrNilClockは、構築時にclockがnilであることを表します。
	ErrNilClock = errors.New("clock is nil")
	// ErrNilClientは、構築時にOpenSearch clientがnilであることを表します。
	ErrNilClient = errors.New("opensearch client is nil")
	// ErrEmptySpoolPayloadは、spool recordのpayloadが空であることを表します。
	ErrEmptySpoolPayload = errors.New("spool record payload is empty")
	// ErrDecodeSpoolPayloadは、spool payloadをdocument write requestとして解析できないことを表します。
	ErrDecodeSpoolPayload = errors.New("failed to decode opensearch spool payload")
	// ErrMissingSpoolPayloadFieldは、spool payloadの必須fieldが空であることを表します。
	ErrMissingSpoolPayloadField = errors.New("opensearch spool payload field is missing")
	// ErrInvalidSpoolPayloadFieldは、spool payloadのfieldの値を再送に使えないことを表します。
	ErrInvalidSpoolPayloadField = errors.New("opensearch spool payload field is invalid")
	// ErrUnclassifiableBulkResponseは、Bulk APIの応答をrecord別の結果へ対応付けられないことを表します。
	// 原因の応答検証errorはerrors.Is・errors.Asで辿れます。
	ErrUnclassifiableBulkResponse = errors.New("out of control bulk response error")
)

// SpoolPayloadFieldErrorは、spool payloadのfieldが再送の契約を満たさないことを表します。
// errors.Is(err, Kind)で種類を判定でき、値を検証した理由（pkg/opensearchのsentinel）も
// errors.Isで判定できます。保存されたpayloadの値は、errors.Asで取り出せるfieldにも原因にも含めません。
type SpoolPayloadFieldError struct {
	// Fieldは、document write requestのfield名（index、id、doc、op）です。
	Field string
	// Kindは、ErrMissingSpoolPayloadFieldまたはErrInvalidSpoolPayloadFieldです。
	Kind error
	// Errは、値を検証した理由です。保存された値は含みません。
	Err     error
	message string
}

func (e *SpoolPayloadFieldError) Error() string {
	if e.message == "" {
		return fmt.Sprintf("%v: %s", e.Kind, e.Field)
	}
	return e.message
}

// Isは、Kindに一致します。
func (e *SpoolPayloadFieldError) Is(target error) bool { return e.Kind != nil && target == e.Kind }

// Unwrapは、値を検証した理由を返します。
func (e *SpoolPayloadFieldError) Unwrap() error { return e.Err }

// bulkDocumentFailureErrorは、Bulk APIがdocument単位で返した失敗原因を保持します。
// Bulk request全体のHTTP statusとは異なるため、transport用HTTPStatusErrorへは変換しません。
type bulkDocumentFailureError struct {
	documentID string
	status     int
	errorType  string
	reason     string
}

func (e *bulkDocumentFailureError) Error() string {
	return fmt.Sprintf(
		"opensearch bulk document failed: id=%q status=%d type=%q reason=%q",
		e.documentID,
		e.status,
		e.errorType,
		e.reason,
	)
}

type OpensearchSpoolSender struct {
	Clock               clock.Clock
	osClient            opensearch.Client
	maxBulkRequestBytes int
}

func NewOpensearchSpoolSender(
	clock clock.Clock,
	osClient opensearch.Client,
	maxBulkRequestBytes int,
) (*OpensearchSpoolSender, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if osClient == nil {
		return nil, ErrNilClient
	}
	if err := infraopensearch.ValidateMaxBulkRequestBytes(maxBulkRequestBytes); err != nil {
		return nil, err
	}

	return &OpensearchSpoolSender{
		Clock:               clock,
		osClient:            osClient,
		maxBulkRequestBytes: maxBulkRequestBytes,
	}, nil
}

func (s *OpensearchSpoolSender) Send(
	ctx context.Context,
	records []spool.SpoolRecord,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))

	if len(records) == 0 {
		return report, nil
	}

	bulkRecords := make([]infraopensearch.BulkRecord, 0, len(records))

	for inputIndex, record := range records {
		doc, err := s.convertSpoolRecordToWriteDocumentRequest(record)
		if err != nil {
			report = append(report, spool.RecordOutcome{
				InputIndex: inputIndex,
				Status:     spool.RecordOutcomeStatusInvalid,
				Err:        err,
			})
			continue
		}

		bulkRecords = append(bulkRecords, infraopensearch.BulkRecord{
			InputIndex: inputIndex,
			Document:   doc,
		})
	}

	// 全件 invalid なら backend send は行わない。
	// backend へ再送を試みていないため、再送試行メタデータも更新しない。
	if len(bulkRecords) == 0 {
		return report, nil
	}

	plan, err := infraopensearch.PlanBulkRequests(bulkRecords, s.maxBulkRequestBytes)
	if err != nil {
		appendBulkRecordOutcomes(
			&report,
			bulkRecords,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
		return report, err
	}
	for _, rejected := range plan.Rejected {
		report = append(report, spool.RecordOutcome{
			InputIndex: rejected.Record.InputIndex,
			Status:     spool.RecordOutcomeStatusInvalid,
			Err:        rejected.Err,
		})
	}

	for requestIndex, request := range plan.Requests {
		if ctxErr := ctx.Err(); ctxErr != nil {
			appendUnattemptedBulkRequestOutcomes(&report, plan.Requests[requestIndex:], ctxErr)
			return report, ctxErr
		}

		// 各計画済みrequestに実際の送信開始時刻を記録する。413後の
		// 再帰分割は同じ初回requestの継続なので、この時刻を引き継ぐ。
		attemptedAt := s.Clock.Now()
		sendReport, sendErr := s.sendBulkRecords(
			ctx,
			request.Records,
			attemptedAt,
		)
		report = append(report, sendReport...)
		if sendErr != nil {
			appendUnattemptedBulkRequestOutcomes(&report, plan.Requests[requestIndex+1:], sendErr)
			return report, sendErr
		}
		// Retry-Afterは同じOpenSearchへの次の送信までの待機を指定するため、
		// 残りのrequestは送信せず、未送信の再送対象として返す。
		if throttled, ok := sendReport.LongestRetryAfter(); ok {
			appendUnattemptedBulkRequestOutcomes(&report, plan.Requests[requestIndex+1:], spool.DeferredByRetryAfter(throttled))
			return report, nil
		}
	}

	return report, nil
}

func appendUnattemptedBulkRequestOutcomes(
	report *spool.ExportReport,
	requests []infraopensearch.PlannedBulkRequest,
	err error,
) {
	for _, request := range requests {
		appendBulkRecordOutcomes(
			report,
			request.Records,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
	}
}

// sendBulkRecordsはplannerが一つのBulk requestへ割り当てたrecordを送信し、response
// itemの入力順を使って元のSpoolRecordへ結果を戻します。HTTP 413、またはdocument内容の
// 不正でrequest全体が拒否された場合はどのdocumentも処理されていないため、複数documentなら
// 半分ずつ再送して原因を隔離し、単一documentでも拒否される場合だけquarantineへ確定します。
// 再送可能な応答のRetry-Afterはoutcomeへ保持し、分割後の残りのdocumentも送信しません。
func (s *OpensearchSpoolSender) sendBulkRecords(
	ctx context.Context,
	records []infraopensearch.BulkRecord,
	attemptedAt time.Time,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	if len(records) == 0 {
		return report, nil
	}
	if err := ctx.Err(); err != nil {
		appendBulkRecordOutcomes(
			&report,
			records,
			spool.RecordOutcomeStatusRetryable,
			attemptedAt,
			err,
		)
		return report, err
	}

	docs := make([]opensearch.DocumentWriteRequest, len(records))
	for index, record := range records {
		docs[index] = record.Document
	}

	bulkResp, err := s.osClient.IndexDocuments(ctx, docs)
	if err != nil {

		if infraopensearch.IsRecordScopedRequestRejection(err) {
			if len(records) == 1 {
				appendBulkRecordOutcomes(
					&report,
					records,
					spool.RecordOutcomeStatusInvalid,
					attemptedAt,
					err,
				)
				return report, nil
			}

			middle := len(records) / 2
			leftReport, leftErr := s.sendBulkRecords(ctx, records[:middle], attemptedAt)
			report = append(report, leftReport...)
			if throttled, ok := leftReport.LongestRetryAfter(); ok {
				// 分けた後半は未送信である。Retry-Afterの期限まで送信せず、
				// 初回requestの送信時刻を残して再送対象にする。
				appendBulkRecordOutcomes(
					&report,
					records[middle:],
					spool.RecordOutcomeStatusRetryable,
					attemptedAt,
					spool.DeferredByRetryAfter(throttled),
				)
				return report, leftErr
			}
			rightReport, rightErr := s.sendBulkRecords(ctx, records[middle:], attemptedAt)
			report = append(report, rightReport...)
			return report, errors.Join(leftErr, rightErr)
		}

		decision := retryable.ClassifyDurableRequestOutcome(ctx, err, infraopensearch.ClassifyDurableRequestFailure)
		ctxErr := ctx.Err()
		status := spool.RecordOutcomeStatusPermanent
		var retryAfter time.Duration
		if decision.Quarantine {
			status = spool.RecordOutcomeStatusInvalid
		} else if decision.Retry {
			status = spool.RecordOutcomeStatusRetryable
			retryAfter = decision.RetryAfter
		}
		appendBulkRecordOutcomes(
			&report,
			records,
			status,
			attemptedAt,
			joinDistinctErrors(err, ctxErr),
		)
		setOutcomeRetryAfter(report, retryAfter)
		return report, ctxErr
	}

	classification, err := infraopensearch.ClassifyFailedBulkDocs(bulkResp, docs)
	if err != nil {
		classificationErr := fmt.Errorf("%w: %w", ErrUnclassifiableBulkResponse, err)
		// responseをrecord単位へ安全に対応づけられない場合も、全送信対象を
		// quarantineへ隔離する。構造errorを呼び出し元へ維持する。
		appendBulkRecordOutcomes(
			&report,
			records,
			spool.RecordOutcomeStatusInvalid,
			attemptedAt,
			classificationErr,
		)
		return report, classificationErr
	}

	statuses := make([]spool.RecordOutcomeStatus, len(records))
	failureErrors := make([]error, len(records))
	classified := make([]bool, len(records))
	for index := range statuses {
		statuses[index] = spool.RecordOutcomeStatusSuccess
	}

	recordFailure := func(
		failure infraopensearch.BulkFailedRequest,
		status spool.RecordOutcomeStatus,
	) error {
		if failure.InputIndex < 0 || failure.InputIndex >= len(records) {
			return fmt.Errorf(
				"bulk failure input index is out of range: index=%d records=%d",
				failure.InputIndex,
				len(records),
			)
		}
		if classified[failure.InputIndex] {
			return fmt.Errorf(
				"bulk request classified more than once: input_index=%d",
				failure.InputIndex,
			)
		}
		classified[failure.InputIndex] = true
		statuses[failure.InputIndex] = status
		failureErrors[failure.InputIndex] = newBulkDocumentFailureError(failure)
		return nil
	}

	for _, failure := range classification.RetryableFailures {
		if err := recordFailure(failure, spool.RecordOutcomeStatusRetryable); err != nil {
			return retryableClassificationFailureReport(records, attemptedAt, err)
		}
	}
	for _, failure := range classification.DeadFailures {
		if err := recordFailure(failure, spool.RecordOutcomeStatusPermanent); err != nil {
			return retryableClassificationFailureReport(records, attemptedAt, err)
		}
	}
	for _, failure := range classification.QuarantineFailures {
		if err := recordFailure(failure, spool.RecordOutcomeStatusInvalid); err != nil {
			return retryableClassificationFailureReport(records, attemptedAt, err)
		}
	}

	// response全体の対応付けが完了してから、各結果へ元の入力indexを保持します。
	// report自体の並びに依存せず、InputIndexでinputRecordsへ対応づけるためです。
	for recordIndex, record := range records {
		report = append(report, spool.RecordOutcome{
			InputIndex:  record.InputIndex,
			Status:      statuses[recordIndex],
			AttemptedAt: attemptedAt,
			Err:         failureErrors[recordIndex],
		})
	}

	return report, nil
}

func retryableClassificationFailureReport(
	records []infraopensearch.BulkRecord,
	attemptedAt time.Time,
	err error,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	appendBulkRecordOutcomes(
		&report,
		records,
		spool.RecordOutcomeStatusRetryable,
		attemptedAt,
		err,
	)
	return report, err
}

func appendBulkRecordOutcomes(
	report *spool.ExportReport,
	records []infraopensearch.BulkRecord,
	status spool.RecordOutcomeStatus,
	attemptedAt time.Time,
	err error,
) {
	for _, record := range records {
		*report = append(*report, spool.RecordOutcome{
			InputIndex:  record.InputIndex,
			Status:      status,
			AttemptedAt: attemptedAt,
			Err:         err,
		})
	}
}

// setOutcomeRetryAfterは、OpenSearchが再送可能な応答で指定したRetry-Afterをoutcomeへ設定します。
// 0以下の値は指定なしとして扱います。
func setOutcomeRetryAfter(report spool.ExportReport, retryAfter time.Duration) {
	if retryAfter <= 0 {
		return
	}
	for i := range report {
		report[i].RetryAfter = retryAfter
	}
}

func newBulkDocumentFailureError(
	failure infraopensearch.BulkFailedRequest,
) error {
	return &bulkDocumentFailureError{
		documentID: failure.Request.ID,
		status:     failure.Status,
		errorType:  string(failure.ErrorType),
		reason:     failure.ErrorReason,
	}
}

func joinDistinctErrors(primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil || errors.Is(secondary, primary) {
		return primary
	}
	return errors.Join(primary, secondary)
}

func (s *OpensearchSpoolSender) convertSpoolRecordToWriteDocumentRequest(
	record spool.SpoolRecord,
) (opensearch.DocumentWriteRequest, error) {
	var req opensearch.DocumentWriteRequest

	if len(record.Payload) == 0 {
		return req, ErrEmptySpoolPayload
	}

	if err := jsonutil.UnmarshalPreservingNumbers(record.Payload, &req); err != nil {
		// decodeのerrorは保存されたpayloadの値を保持し得るため、errors.Asで取り出せないよう
		// 原因としては包まず、文言だけを残します。種類はErrDecodeSpoolPayloadで判定します。
		return req, fmt.Errorf("%w: %v", ErrDecodeSpoolPayload, err)
	}

	if req.Index == "" {
		return req, &SpoolPayloadFieldError{
			Field: "index", Kind: ErrMissingSpoolPayloadField,
			message: "document write request index is empty",
		}
	}
	if err := opensearch.ValidateIndex(req.Index); err != nil {
		return req, &SpoolPayloadFieldError{
			Field: "index", Kind: ErrInvalidSpoolPayloadField, Err: err,
			message: fmt.Sprintf("invalid document write request index: %v", err),
		}
	}

	if req.ID == "" {
		return req, &SpoolPayloadFieldError{
			Field: "id", Kind: ErrMissingSpoolPayloadField,
			message: "document write request id is empty",
		}
	}

	if req.Doc == nil {
		return req, &SpoolPayloadFieldError{
			Field: "doc", Kind: ErrMissingSpoolPayloadField,
			message: "document write request doc is nil",
		}
	}

	switch req.Op {
	case opensearch.WriteOpIndex, opensearch.WriteOpCreate:
		return req, nil
	case "":
		return req, &SpoolPayloadFieldError{
			Field: "op", Kind: ErrMissingSpoolPayloadField,
			message: "document write request op is empty",
		}
	default:
		return req, &SpoolPayloadFieldError{
			Field: "op", Kind: ErrInvalidSpoolPayloadField,
			message: fmt.Sprintf("unsupported document write request op: %s", req.Op),
		}
	}
}
