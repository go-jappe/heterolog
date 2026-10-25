package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

// ClickHouseSpoolSenderの構築とspool payloadの検証で返すerrorの種類です。
var (
	// ErrNilClockは、構築時にclockがnilであることを表します。
	ErrNilClock = errors.New("clock is nil")
	// ErrNilClientは、構築時にClickHouse clientがnilであることを表します。
	ErrNilClient = errors.New("clickhouse client is nil")
	// ErrInvalidMaxInsertRequestBytesは、一回のINSERTのbyte上限が正でないことを表します。
	ErrInvalidMaxInsertRequestBytes = errors.New("clickhouse max INSERT request bytes must be greater than zero")
	// ErrEmptySpoolPayloadは、spool recordのpayloadが空であることを表します。
	ErrEmptySpoolPayload = errors.New("spool record payload is empty")
	// ErrDecodeSpoolPayloadは、spool payloadをdurable documentとして解析できないことを表します。
	ErrDecodeSpoolPayload = errors.New("failed to decode clickhouse spool payload")
	// ErrNilSpoolPayloadDocumentは、spool payloadがJSON nullであることを表します。
	ErrNilSpoolPayloadDocument = errors.New("clickhouse spool payload document is nil")
	// ErrMissingSpoolPayloadFieldは、durable documentの必須fieldが空であることを表します。
	ErrMissingSpoolPayloadField = errors.New("clickhouse spool payload field is missing")
	// ErrSpoolPayloadFieldWhitespaceは、durable documentのfieldの前後に空白があることを表します。
	ErrSpoolPayloadFieldWhitespace = errors.New("clickhouse spool payload field contains surrounding whitespace")
	// ErrUnknownSpoolPayloadReplayModeは、replay_modeが未設定または未知の値であることを表します。
	ErrUnknownSpoolPayloadReplayMode = errors.New("clickhouse spool payload replay mode is unknown")
	// ErrNotReplayableSpoolPayloadは、replay_modeが再送しないrecordを表すことを示します。
	ErrNotReplayableSpoolPayload = errors.New("clickhouse spool payload is not replayable")
	// ErrZeroSpoolPayloadTimestampは、durable documentのtimestampがzero値であることを表します。
	ErrZeroSpoolPayloadTimestamp = errors.New("document timestamp is zero")
)

// SpoolPayloadFieldErrorは、durable documentのfieldが再送の契約を満たさないことを表します。
// errors.Is(err, Kind)で種類を判定できます。保存されたpayloadの値は、errors.Asで
// 取り出せるfieldに含めません。
type SpoolPayloadFieldError struct {
	// Fieldは、durable documentのfield名です。
	Field string
	// Kindは、ErrMissingSpoolPayloadField、ErrSpoolPayloadFieldWhitespace、
	// ErrUnknownSpoolPayloadReplayMode、ErrNotReplayableSpoolPayloadのいずれかです。
	Kind    error
	message string
}

func (e *SpoolPayloadFieldError) Error() string {
	if e.message == "" {
		return fmt.Sprintf("%v: %s", e.Kind, e.Field)
	}
	return e.message
}

func (e *SpoolPayloadFieldError) Unwrap() error { return e.Kind }

type Client interface {
	InsertLogsWithOptions(
		ctx context.Context,
		records []pkgclickhouse.LogRecord,
		opts pkgclickhouse.InsertOptions,
	) (*pkgclickhouse.InsertResponse, error)
}

// ClickHouseSpoolSender は spool に退避された ClickHouse record を再送します。
//
// batch modeは初回INSERTと同じinsert_deduplication_tokenでgroup再送します。
// record isolation modeはroot tokenと永続的なrecord IDからtokenを導出し、
// 必ず一件ずつ送信します。batch modeの複数record groupがrecordの値に起因する
// 拒否やHTTP 413で失敗した場合も、同じ規則で導出したtokenで一件ずつ再送し、
// 一件でも拒否されたrecordだけをquarantineへ隔離します。
type ClickHouseSpoolSender struct {
	Clock                 clock.Clock
	client                Client
	maxInsertRequestBytes int
}

func NewClickHouseSpoolSender(
	clock clock.Clock,
	client Client,
	maxInsertRequestBytes int,
) (*ClickHouseSpoolSender, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if client == nil {
		return nil, ErrNilClient
	}
	if maxInsertRequestBytes <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidMaxInsertRequestBytes, maxInsertRequestBytes)
	}

	return &ClickHouseSpoolSender{
		Clock:                 clock,
		client:                client,
		maxInsertRequestBytes: maxInsertRequestBytes,
	}, nil
}

func (s *ClickHouseSpoolSender) Send(
	ctx context.Context,
	records []spool.SpoolRecord,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	if len(records) == 0 {
		return report, nil
	}
	if err := ctx.Err(); err != nil {
		appendClickHouseRecordOutcomes(
			&report,
			allClickHouseInputIndexes(len(records)),
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
		return report, err
	}

	groups := make([]clickHouseSpoolGroup, 0, len(records))
	batchGroupByToken := make(map[string]int)

	for inputIndex, record := range records {
		payload, err := s.convertSpoolRecordToDurablePayload(record)
		if err != nil {
			report = append(report, spool.RecordOutcome{
				InputIndex: inputIndex,
				Status:     spool.RecordOutcomeStatusInvalid,
				Err:        err,
			})
			continue
		}

		switch payload.ReplayMode {
		case infraclickhouse.DurableReplayModeBatch:
			groupIdx, ok := batchGroupByToken[payload.InsertDeduplicationToken]
			if !ok {
				groupIdx = len(groups)
				batchGroupByToken[payload.InsertDeduplicationToken] = groupIdx
				groups = append(groups, clickHouseSpoolGroup{
					token: payload.InsertDeduplicationToken,
				})
			}
			groups[groupIdx].inputIndexes = append(groups[groupIdx].inputIndexes, inputIndex)
			groups[groupIdx].recordIDs = append(groups[groupIdx].recordIDs, payload.RecordID)
			groups[groupIdx].logRecords = append(groups[groupIdx].logRecords, payload.LogRecord)

		case infraclickhouse.DurableReplayModeRecordIsolation:
			token, deriveErr := infraclickhouse.DeriveRecordIsolationToken(
				payload.InsertDeduplicationToken,
				payload.RecordID,
			)
			if deriveErr != nil {
				report = append(report, spool.RecordOutcome{
					InputIndex: inputIndex,
					Status:     spool.RecordOutcomeStatusInvalid,
					Err:        fmt.Errorf("derive clickhouse record isolation token: %w", deriveErr),
				})
				continue
			}

			// isolation recordは同じroot tokenやrecord IDを持つ別入力ともgroup化しません。
			// 一件/INSERTを維持し、同じ派生tokenの再送重複はClickHouse側のdeduplicationへ
			// 委ねます。
			groups = append(groups, clickHouseSpoolGroup{
				token:        token,
				inputIndexes: []int{inputIndex},
				recordIDs:    []string{payload.RecordID},
				logRecords:   []pkgclickhouse.LogRecord{payload.LogRecord},
			})
		}
	}

	if len(groups) == 0 {
		return report, nil
	}

	for groupIndex, group := range groups {
		if err := ctx.Err(); err != nil {
			appendClickHouseGroupOutcomes(
				&report,
				groups[groupIndex:],
				spool.RecordOutcomeStatusRetryable,
				time.Time{},
				err,
			)
			return report, err
		}

		requestPlan, planErr := infraclickhouse.PlanInsertRequests(
			group.logRecords,
			s.maxInsertRequestBytes,
		)
		if planErr != nil {
			appendClickHouseRecordOutcomes(
				&report,
				group.inputIndexes,
				spool.RecordOutcomeStatusInvalid,
				time.Time{},
				planErr,
			)
			continue
		}
		if len(requestPlan.Rejected) > 0 || len(requestPlan.Requests) != 1 {
			// 同じdeduplication tokenを複数の物理INSERTへ分割すると、二件目以降を
			// ClickHouseが一件目の重複として除外し得ます。hard limit導入前に作られた
			// oversized groupは内容を変えて自動再送できないため、
			// backendを呼ぶ前に全recordをinvalidとします。
			oversizedErr := fmt.Errorf(
				"clickhouse durable token group %q cannot be replayed as one INSERT within %d bytes",
				group.token,
				s.maxInsertRequestBytes,
			)
			appendClickHouseRecordOutcomes(
				&report,
				group.inputIndexes,
				spool.RecordOutcomeStatusInvalid,
				time.Time{},
				oversizedErr,
			)
			continue
		}

		attemptedAt := s.Clock.Now()
		groupReport, stopErr := s.insertGroup(ctx, group, attemptedAt)
		report = append(report, groupReport...)
		if stopErr != nil {
			appendClickHouseGroupOutcomes(
				&report,
				groups[groupIndex+1:],
				spool.RecordOutcomeStatusRetryable,
				time.Time{},
				stopErr,
			)
			return report, stopErr
		}
		// Retry-Afterは同じClickHouseへの次の送信までの待機を指定するため、
		// 残りのgroupは送信せず、未送信の再送対象として返します。
		if throttled, ok := groupReport.LongestRetryAfter(); ok {
			appendClickHouseGroupOutcomes(
				&report,
				groups[groupIndex+1:],
				spool.RecordOutcomeStatusRetryable,
				time.Time{},
				spool.DeferredByRetryAfter(throttled),
			)
			return report, nil
		}
	}

	return report, nil
}

// insertGroupは一つのgroupを一回のINSERTで送信し、各recordの結果を返します。
// 呼び出し元contextの終了や応答の欠落でこの周期の送信を止める場合は、そのerrorを返します。
// 呼び出し元は未送信の残りのgroupを再送対象にします。再送可能な応答のRetry-Afterは
// outcomeへ保持し、呼び出し元はその期限まで残りのgroupを送信しません。
func (s *ClickHouseSpoolSender) insertGroup(
	ctx context.Context,
	group clickHouseSpoolGroup,
	attemptedAt time.Time,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(group.inputIndexes))
	output, err := s.client.InsertLogsWithOptions(ctx, group.logRecords, pkgclickhouse.InsertOptions{
		InsertDeduplicationToken: group.token,
	})
	if err == nil {
		if output == nil {
			// 応答がなければINSERTの成否を確認できません。OpenSearch・CloudWatch Logsと
			// 同じく、送信したgroupは自動再送せずquarantineへ隔離します。未送信のgroupは
			// 再送対象として残し、この周期の送信を止めます。
			missingResponseErr := infraclickhouse.ErrMissingInsertResponse
			appendClickHouseRecordOutcomes(
				&report,
				group.inputIndexes,
				spool.RecordOutcomeStatusInvalid,
				attemptedAt,
				missingResponseErr,
			)
			return report, missingResponseErr
		}
		appendClickHouseRecordOutcomes(
			&report,
			group.inputIndexes,
			spool.RecordOutcomeStatusSuccess,
			attemptedAt,
			nil,
		)
		return report, nil
	}

	var marshalErr *pkgclickhouse.LogRecordMarshalError
	if errors.As(err, &marshalErr) && marshalErr != nil &&
		marshalErr.Index >= 0 && marshalErr.Index < len(group.inputIndexes) {
		failedInputIndex := group.inputIndexes[marshalErr.Index]
		report = append(report, spool.RecordOutcome{
			InputIndex: failedInputIndex,
			Status:     spool.RecordOutcomeStatusInvalid,
			Err:        err,
		})
		remainingInputIndexes := make([]int, 0, len(group.inputIndexes)-1)
		for groupRecordIndex, inputIndex := range group.inputIndexes {
			if groupRecordIndex != marshalErr.Index {
				remainingInputIndexes = append(remainingInputIndexes, inputIndex)
			}
		}
		appendClickHouseRecordOutcomes(
			&report,
			remainingInputIndexes,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
		return report, nil
	}

	decision := retryable.ClassifyDurableRequestOutcome(ctx, err, func(classifyCtx context.Context, attemptErr error) retryable.RetryDecision {
		return infraclickhouse.ClassifyDurableInsertFailure(classifyCtx, attemptErr, len(group.logRecords))
	})
	if decision.Retry && decision.Reason == infraclickhouse.RetryReasonRecordIsolationRequired && len(group.logRecords) > 1 {
		return s.insertIsolatedRecords(ctx, group, attemptedAt, err)
	}
	ctxErr := ctx.Err()
	status := spool.RecordOutcomeStatusPermanent
	var retryAfter time.Duration
	if decision.Quarantine {
		status = spool.RecordOutcomeStatusInvalid
	} else if decision.Retry {
		status = spool.RecordOutcomeStatusRetryable
		retryAfter = decision.RetryAfter
	}
	appendClickHouseRecordOutcomes(&report, group.inputIndexes, status, attemptedAt, errors.Join(err, ctxErr))
	setOutcomeRetryAfter(report, retryAfter)
	return report, ctxErr
}

// insertIsolatedRecordsは、recordの値に起因する拒否やHTTP 413で失敗したbatch groupを、
// root tokenとrecord IDから導出したtokenで一件ずつ再送します。拒否されたbatch INSERTは
// 確定していないため、正常なrecordは個別に配送し、一件でも拒否されたrecordだけを
// quarantineへ隔離します。replay modeはpayloadへ永続化しないため、この周期で確定しなかった
// recordは次回も同じbatch tokenでgroup再送します。派生tokenは同じ規則で再び導出されるので、
// 既に配送済みのrecordの重複はClickHouseのdeduplicationが除外します。
// 一件ずつの送信もbatch INSERTの継続として、その送信時刻を引き継ぎます。
// 途中でRetry-Afterを返された場合は、残りのrecordを送信せず再送対象にします。
func (s *ClickHouseSpoolSender) insertIsolatedRecords(
	ctx context.Context,
	group clickHouseSpoolGroup,
	attemptedAt time.Time,
	batchErr error,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(group.inputIndexes))
	for recordIndex, inputIndex := range group.inputIndexes {
		if ctxErr := ctx.Err(); ctxErr != nil {
			appendClickHouseRecordOutcomes(
				&report,
				group.inputIndexes[recordIndex:],
				spool.RecordOutcomeStatusRetryable,
				attemptedAt,
				errors.Join(batchErr, ctxErr),
			)
			return report, ctxErr
		}
		token, deriveErr := infraclickhouse.DeriveRecordIsolationToken(group.token, group.recordIDs[recordIndex])
		if deriveErr != nil {
			report = append(report, spool.RecordOutcome{
				InputIndex:  inputIndex,
				Status:      spool.RecordOutcomeStatusInvalid,
				AttemptedAt: attemptedAt,
				Err: errors.Join(
					batchErr,
					fmt.Errorf("derive clickhouse record isolation token: %w", deriveErr),
				),
			})
			continue
		}
		recordReport, stopErr := s.insertGroup(ctx, clickHouseSpoolGroup{
			token:        token,
			inputIndexes: []int{inputIndex},
			recordIDs:    []string{group.recordIDs[recordIndex]},
			logRecords:   []pkgclickhouse.LogRecord{group.logRecords[recordIndex]},
		}, attemptedAt)
		report = append(report, recordReport...)
		if stopErr != nil {
			appendClickHouseRecordOutcomes(
				&report,
				group.inputIndexes[recordIndex+1:],
				spool.RecordOutcomeStatusRetryable,
				attemptedAt,
				errors.Join(batchErr, stopErr),
			)
			return report, stopErr
		}
		if throttled, ok := recordReport.LongestRetryAfter(); ok {
			// 残りのrecordは未送信です。Retry-Afterの期限まで送信せず、
			// batch INSERTの送信時刻を残して再送対象にします。
			appendClickHouseRecordOutcomes(
				&report,
				group.inputIndexes[recordIndex+1:],
				spool.RecordOutcomeStatusRetryable,
				attemptedAt,
				errors.Join(batchErr, spool.DeferredByRetryAfter(throttled)),
			)
			return report, nil
		}
	}
	return report, nil
}

// setOutcomeRetryAfterは、ClickHouseが再送可能な応答で指定したRetry-Afterをoutcomeへ設定します。
// 0以下の値は指定なしとして扱います。
func setOutcomeRetryAfter(report spool.ExportReport, retryAfter time.Duration) {
	if retryAfter <= 0 {
		return
	}
	for i := range report {
		report[i].RetryAfter = retryAfter
	}
}

type clickHouseSpoolGroup struct {
	token string

	// ClickHouseは同じinsert_deduplication_tokenを持つrecordを一つのINSERTへまとめます。
	// 入力がtoken順に並んでいる保証はないため、例えばA, B, Aはgroup A=[0,2]、
	// group B=[1]となり、送信・結果確定の順序が元入力順と一致しません。
	// group単位の結果を元のSpoolRecordへ戻すため、各recordのinputIndexを保持します。
	inputIndexes []int
	// recordIDsはinputIndexesと同じ順で、一件ずつ再送するときの派生token導出に使います。
	recordIDs  []string
	logRecords []pkgclickhouse.LogRecord
}

func allClickHouseInputIndexes(inputCount int) []int {
	inputIndexes := make([]int, inputCount)
	for inputIndex := range inputIndexes {
		inputIndexes[inputIndex] = inputIndex
	}
	return inputIndexes
}

func appendClickHouseGroupOutcomes(
	report *spool.ExportReport,
	groups []clickHouseSpoolGroup,
	status spool.RecordOutcomeStatus,
	attemptedAt time.Time,
	err error,
) {
	for _, group := range groups {
		appendClickHouseRecordOutcomes(
			report,
			group.inputIndexes,
			status,
			attemptedAt,
			err,
		)
	}
}

func appendClickHouseRecordOutcomes(
	report *spool.ExportReport,
	inputIndexes []int,
	status spool.RecordOutcomeStatus,
	attemptedAt time.Time,
	err error,
) {
	for _, inputIndex := range inputIndexes {
		*report = append(*report, spool.RecordOutcome{
			InputIndex:  inputIndex,
			Status:      status,
			AttemptedAt: attemptedAt,
			Err:         err,
		})
	}
}

func (s *ClickHouseSpoolSender) convertSpoolRecordToDurablePayload(
	record spool.SpoolRecord,
) (infraclickhouse.DurablePayload, error) {
	if len(record.Payload) == 0 {
		return infraclickhouse.DurablePayload{}, ErrEmptySpoolPayload
	}

	var doc *infraclickhouse.DurablePayload
	if err := jsonutil.UnmarshalPreservingNumbers(record.Payload, &doc); err != nil {
		// decodeのerrorは保存されたpayloadの値（*time.ParseErrorのValueなど）を保持し得るため、
		// errors.Asで取り出せないよう原因としては包まず、文言だけを残します。種類はErrDecodeSpoolPayloadで判定します。
		return infraclickhouse.DurablePayload{}, fmt.Errorf("%w: %v", ErrDecodeSpoolPayload, err)
	}
	if doc == nil {
		return infraclickhouse.DurablePayload{}, ErrNilSpoolPayloadDocument
	}

	trimmedToken := strings.TrimSpace(doc.InsertDeduplicationToken)
	if trimmedToken == "" {
		return infraclickhouse.DurablePayload{}, newSpoolPayloadFieldError(
			pkgclickhouse.SettingInsertDeduplicationToken, ErrMissingSpoolPayloadField,
			"document %s is missing or empty", pkgclickhouse.SettingInsertDeduplicationToken,
		)
	}
	if trimmedToken != doc.InsertDeduplicationToken {
		return infraclickhouse.DurablePayload{}, newSpoolPayloadFieldError(
			pkgclickhouse.SettingInsertDeduplicationToken, ErrSpoolPayloadFieldWhitespace,
			"document %s contains surrounding whitespace", pkgclickhouse.SettingInsertDeduplicationToken,
		)
	}
	trimmedRecordID := strings.TrimSpace(doc.RecordID)
	if trimmedRecordID == "" {
		return infraclickhouse.DurablePayload{}, newSpoolPayloadFieldError(
			infraclickhouse.DurablePayloadFieldRecordID, ErrMissingSpoolPayloadField,
			"document %s is missing or empty", infraclickhouse.DurablePayloadFieldRecordID,
		)
	}
	if trimmedRecordID != doc.RecordID {
		return infraclickhouse.DurablePayload{}, newSpoolPayloadFieldError(
			infraclickhouse.DurablePayloadFieldRecordID, ErrSpoolPayloadFieldWhitespace,
			"document %s contains surrounding whitespace", infraclickhouse.DurablePayloadFieldRecordID,
		)
	}
	if !doc.ReplayMode.IsKnown() {
		return infraclickhouse.DurablePayload{}, newSpoolPayloadFieldError(
			infraclickhouse.DurablePayloadFieldReplayMode, ErrUnknownSpoolPayloadReplayMode,
			"document %s is missing or unknown: %q", infraclickhouse.DurablePayloadFieldReplayMode, doc.ReplayMode,
		)
	}
	if doc.ReplayMode == infraclickhouse.DurableReplayModeNone {
		return infraclickhouse.DurablePayload{}, newSpoolPayloadFieldError(
			infraclickhouse.DurablePayloadFieldReplayMode, ErrNotReplayableSpoolPayload,
			"document %s %q is not replayable", infraclickhouse.DurablePayloadFieldReplayMode, doc.ReplayMode,
		)
	}
	if doc.Timestamp.IsZero() {
		return infraclickhouse.DurablePayload{}, ErrZeroSpoolPayloadTimestamp
	}

	return *doc, nil
}

// newSpoolPayloadFieldErrorは、保存された値をfieldに持たないSpoolPayloadFieldErrorを作ります。
func newSpoolPayloadFieldError(field string, kind error, format string, args ...any) error {
	return &SpoolPayloadFieldError{Field: field, Kind: kind, message: fmt.Sprintf(format, args...)}
}
