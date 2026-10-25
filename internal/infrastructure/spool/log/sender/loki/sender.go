package loki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/storedvalue"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// LokiSpoolSenderの構築とspool payloadの検証で返すerrorの種類です。
var (
	// ErrNilClockは、構築時にclockがnilであることを表します。
	ErrNilClock = errors.New("clock is nil")
	// ErrNilClientは、構築時にLoki clientがnilであることを表します。
	ErrNilClient = errors.New("loki client is nil")
	// ErrEmptySpoolPayloadは、spool recordのpayloadが空であることを表します。
	ErrEmptySpoolPayload = errors.New("spool record payload is empty")
	// ErrDecodeSpoolPayloadは、spool payloadをlog entryとして解析できないことを表します。
	ErrDecodeSpoolPayload = errors.New("failed to unmarshal loki spool payload")
	// ErrEmptySpoolPayloadLabelsは、spool payloadにlabelがないことを表します。
	ErrEmptySpoolPayloadLabels = errors.New("loki spool payload labels is empty")
	// ErrInvalidSpoolPayloadLabelsは、spool payloadのlabelをLokiが受け付けないことを表します。
	// pkg/lokiの検証errorはerrors.Is・errors.Asで辿れます。
	ErrInvalidSpoolPayloadLabels = errors.New("loki spool payload has invalid labels")
	// ErrEmptySpoolPayloadLabelKeyは、spool payloadのlabel名が空であることを表します。
	ErrEmptySpoolPayloadLabelKey = errors.New("loki spool payload labels contains empty key")
	// ErrEmptySpoolPayloadLineは、spool payloadのlog lineが空であることを表します。
	ErrEmptySpoolPayloadLine = errors.New("loki spool payload line is empty")
	// ErrEmptySpoolPayloadTimestampは、spool payloadのtimestampがzero値であることを表します。
	ErrEmptySpoolPayloadTimestamp = errors.New("loki spool payload timestamp is empty")
)

type Client interface {
	Push(ctx context.Context, streams []pkgloki.Stream) error
}

// labeledLogEntry は、Lokiへ送る1件のlog entryと所属streamのlabelsを保持します。
type labeledLogEntry struct {
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
	Line      string            `json:"line"`
}

// replayRecordは、decodeとpayloadの事前検証を通過した一件のspool recordです。
// inputIndexは、Lokiの上限によるrequest分割後も、送信結果を元のSend入力へ
// 対応付けるために保持します。
type replayRecord struct {
	inputIndex int
	labels     map[string]string
	entry      pkgloki.Entry
}

// LokiSpoolSender replays records at-least-once. Loki has no idempotent
// document ID in this path, so a crash after Push and before the processing
// file transition is persisted can resend the same log line.
type LokiSpoolSender struct {
	Clock      clock.Clock
	client     Client
	pushLimits infraloki.PushLimits
}

func NewLokiSpoolSender(
	clock clock.Clock,
	client Client,
	pushLimits infraloki.PushLimits,
) (*LokiSpoolSender, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if client == nil {
		return nil, ErrNilClient
	}
	if err := pushLimits.Validate(); err != nil {
		return nil, err
	}

	return &LokiSpoolSender{
		Clock:      clock,
		client:     client,
		pushLimits: pushLimits,
	}, nil
}

func (s *LokiSpoolSender) Send(
	ctx context.Context,
	records []spool.SpoolRecord,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	if len(records) == 0 {
		return report, nil
	}

	// LokiのPush結果はrequest全体に対する一つの結果であり、backendがrecord indexを
	// 返すわけでも、送信前にrecordを並べ替えるわけでもありません。
	// ただし、decode不能なrecordを送信対象から除外した後も、共通ExportReport契約では
	// 各outcomeが元のSpoolRecordをInputIndexで示す必要があります。そのため、
	// 送信可能なrecordと元indexをreplayRecordとして対応付けます。
	replayRecords := make([]replayRecord, 0, len(records))

	for inputIndex, record := range records {
		labels, entry, err := s.convertSpoolRecordToEntry(record)
		if err != nil {
			report = append(report, spool.RecordOutcome{
				InputIndex: inputIndex,
				Status:     spool.RecordOutcomeStatusInvalid,
				Err:        err,
			})
			continue
		}
		replayRecords = append(replayRecords, replayRecord{
			inputIndex: inputIndex,
			labels:     labels,
			entry:      entry,
		})
	}

	if len(replayRecords) == 0 {
		return report, nil
	}

	pushRecords := make([]infraloki.PushRecord, 0, len(replayRecords))
	for _, record := range replayRecords {
		pushRecords = append(pushRecords, infraloki.PushRecord{
			InputIndex: record.inputIndex,
			Labels:     record.labels,
			Entry:      record.entry,
		})
	}
	plan, err := infraloki.PlanPushRequests(pushRecords, s.pushLimits)
	if err != nil {
		appendReplayOutcomes(
			&report,
			replayRecords,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
		return report, err
	}
	for _, rejected := range plan.Rejected {
		// 送信前に単一record固有の上限超過と確定しているため、workerが
		// quarantineへ移すinvalid outcomeとして返す。
		report = append(report, spool.RecordOutcome{
			InputIndex: rejected.Record.InputIndex,
			Status:     spool.RecordOutcomeStatusInvalid,
			Err:        rejected.Err,
		})
	}

	for requestIndex, request := range plan.Requests {
		if ctxErr := ctx.Err(); ctxErr != nil {
			appendUnattemptedRequestOutcomes(&report, plan.Requests[requestIndex:], ctxErr)
			return report, ctxErr
		}

		// 各計画済みrequestに実際の送信開始時刻を記録します。413後の分割再送は
		// 同じ初回requestの継続なので、この時刻を引き継ぎます。
		attemptedAt := s.Clock.Now()
		requestReport, ctxErr := s.pushRequest(ctx, request, attemptedAt)
		report = append(report, requestReport...)
		if ctxErr != nil {
			appendUnattemptedRequestOutcomes(&report, plan.Requests[requestIndex+1:], ctxErr)
			return report, ctxErr
		}
		// Retry-Afterは同じLokiへの次の送信までの待機を指定するため、
		// 残りのrequestは送信せず、未送信の再送対象として返します。
		if throttled, ok := requestReport.LongestRetryAfter(); ok {
			appendUnattemptedRequestOutcomes(&report, plan.Requests[requestIndex+1:], spool.DeferredByRetryAfter(throttled))
			return report, nil
		}
	}

	return report, nil
}

// pushRequestは計画済みの一requestを送信し、結果を元の入力indexへ対応付けます。
// HTTP 413ではどのentryも受理されていないため、複数entryのrequestは半分ずつに分けて
// 再送し、一件でも拒否された場合だけquarantineへ分類します。HTTP 400は一部のstreamを
// 受理した可能性があるため、分割して再送せずquarantineへ分類します。
// 再送可能な応答のRetry-Afterはoutcomeへ保持し、分割後の残りのrequestも送信しません。
// 呼び出し元contextが終了した場合は、そのerrorを返します。
func (s *LokiSpoolSender) pushRequest(
	ctx context.Context,
	request infraloki.PlannedPushRequest,
	attemptedAt time.Time,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(request.Records))
	pushErr := s.client.Push(ctx, request.Streams)
	if pushErr == nil {
		appendPushRecordOutcomes(
			&report,
			request.Records,
			spool.RecordOutcomeStatusSuccess,
			attemptedAt,
			nil,
		)
		return report, nil
	}

	if infraloki.IsRequestEntityTooLarge(pushErr) {
		if first, second, ok := infraloki.SplitPushRequest(request); ok {
			halves := []infraloki.PlannedPushRequest{first, second}
			for halfIndex, half := range halves {
				if ctxErr := ctx.Err(); ctxErr != nil {
					// 分けたrequestは未送信です。初回requestの送信時刻を残して再送対象にします。
					for _, unsent := range halves[halfIndex:] {
						appendPushRecordOutcomes(&report, unsent.Records, spool.RecordOutcomeStatusRetryable, attemptedAt, ctxErr)
					}
					return report, ctxErr
				}
				halfReport, ctxErr := s.pushRequest(ctx, half, attemptedAt)
				report = append(report, halfReport...)
				if ctxErr != nil {
					for _, unsent := range halves[halfIndex+1:] {
						appendPushRecordOutcomes(&report, unsent.Records, spool.RecordOutcomeStatusRetryable, attemptedAt, ctxErr)
					}
					return report, ctxErr
				}
				if throttled, ok := halfReport.LongestRetryAfter(); ok {
					// 分けたrequestは未送信です。Retry-Afterの期限まで送信せず、
					// 初回requestの送信時刻を残して再送対象にします。
					for _, unsent := range halves[halfIndex+1:] {
						appendPushRecordOutcomes(&report, unsent.Records, spool.RecordOutcomeStatusRetryable, attemptedAt, spool.DeferredByRetryAfter(throttled))
					}
					return report, nil
				}
			}
			return report, nil
		}
	}

	// Cancellation does not override a confirmed refusal. Classify the backend
	// result independently, then quarantine cancellation during dispatch.
	decision := retryable.ClassifyDurableRequestOutcome(ctx, pushErr, infraloki.ClassifyDurableRequestFailure)
	ctxErr := ctx.Err()
	status := spool.RecordOutcomeStatusPermanent
	var retryAfter time.Duration
	if decision.Quarantine {
		status = spool.RecordOutcomeStatusInvalid
	} else if decision.Retry {
		status = spool.RecordOutcomeStatusRetryable
		retryAfter = decision.RetryAfter
	}
	appendPushRecordOutcomes(&report, request.Records, status, attemptedAt, joinDistinctErrors(pushErr, ctxErr))
	setOutcomeRetryAfter(report, retryAfter)
	return report, ctxErr
}

// setOutcomeRetryAfterは、Lokiが再送可能な応答で指定したRetry-Afterをoutcomeへ設定します。
// 0以下の値は指定なしとして扱います。
func setOutcomeRetryAfter(report spool.ExportReport, retryAfter time.Duration) {
	if retryAfter <= 0 {
		return
	}
	for i := range report {
		report[i].RetryAfter = retryAfter
	}
}

func appendReplayOutcomes(
	report *spool.ExportReport,
	records []replayRecord,
	status spool.RecordOutcomeStatus,
	attemptedAt time.Time,
	err error,
) {
	for _, record := range records {
		*report = append(*report, spool.RecordOutcome{
			InputIndex:  record.inputIndex,
			Status:      status,
			AttemptedAt: attemptedAt,
			Err:         err,
		})
	}
}

func appendPushRecordOutcomes(
	report *spool.ExportReport,
	records []infraloki.PushRecord,
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

func appendUnattemptedRequestOutcomes(
	report *spool.ExportReport,
	requests []infraloki.PlannedPushRequest,
	err error,
) {
	for _, request := range requests {
		appendPushRecordOutcomes(
			report,
			request.Records,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
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

func (s *LokiSpoolSender) convertSpoolRecordToEntry(
	record spool.SpoolRecord,
) (map[string]string, pkgloki.Entry, error) {
	if len(record.Payload) == 0 {
		return nil, pkgloki.Entry{}, ErrEmptySpoolPayload
	}

	var doc labeledLogEntry
	if err := json.Unmarshal(record.Payload, &doc); err != nil {
		// decodeのerrorは保存されたpayloadの値（*time.ParseErrorのValueなど）を保持し得るため、
		// errors.Asで取り出せないよう原因としては包まず、文言だけを残します。種類はErrDecodeSpoolPayloadで判定します。
		return nil, pkgloki.Entry{}, fmt.Errorf("%w: %v", ErrDecodeSpoolPayload, err)
	}
	if len(doc.Labels) == 0 {
		return nil, pkgloki.Entry{}, ErrEmptySpoolPayloadLabels
	}
	if err := pkgloki.ValidateLabels(doc.Labels); err != nil {
		// 保存されたlabel名はerrors.Asで取り出せないよう、*pkgloki.InvalidLabelNameErrorを
		// 文言とpkg/lokiの種類のsentinelだけを残したerrorへ置き換えます。
		return nil, pkgloki.Entry{}, fmt.Errorf("%w: %w", ErrInvalidSpoolPayloadLabels,
			storedvalue.WithoutValues(err, pkgloki.ErrInvalidLabelName, pkgloki.ErrLabelsRequired))
	}

	labels := make(map[string]string, len(doc.Labels))
	for key, value := range doc.Labels {
		if strings.TrimSpace(key) == "" {
			return nil, pkgloki.Entry{}, ErrEmptySpoolPayloadLabelKey
		}
		labels[key] = value
	}
	if doc.Line == "" {
		return nil, pkgloki.Entry{}, ErrEmptySpoolPayloadLine
	}
	if doc.Timestamp.IsZero() {
		return nil, pkgloki.Entry{}, ErrEmptySpoolPayloadTimestamp
	}

	return labels, pkgloki.Entry{Timestamp: doc.Timestamp, Line: doc.Line}, nil
}
