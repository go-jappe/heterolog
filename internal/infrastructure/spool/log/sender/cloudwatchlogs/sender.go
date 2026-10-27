package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/storedvalue"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// CloudWatchLogsSpoolSenderの構築、spool payloadの検証、応答の分類で返すerrorの種類です。
var (
	// ErrNilClockは、構築時にclockがnilであることを表します。
	ErrNilClock = errors.New("clock is nil")
	// ErrNilClientは、構築時にCloudWatch Logs clientがnilであることを表します。
	ErrNilClient = errors.New("cloudwatch logs client is nil")
	// ErrEmptySpoolPayloadは、spool recordのpayloadが空であることを表します。
	ErrEmptySpoolPayload = errors.New("spool record payload is empty")
	// ErrDecodeSpoolPayloadは、spool payloadをCloudWatch Logs documentとして解析できないことを表します。
	ErrDecodeSpoolPayload = errors.New("failed to unmarshal cloudwatch logs spool payload")
	// ErrNilSpoolPayloadDocumentは、spool payloadがJSON nullであることを表します。
	ErrNilSpoolPayloadDocument = errors.New("cloudwatch logs spool payload document is nil")
	// ErrMissingSpoolPayloadFieldは、documentに必須fieldがないことを表します。
	ErrMissingSpoolPayloadField = errors.New("cloudwatch logs spool payload field is missing")
	// ErrInvalidSpoolPayloadFieldは、documentのfieldの値を再送に使えないことを表します。
	ErrInvalidSpoolPayloadField = errors.New("cloudwatch logs spool payload field is invalid")
	// ErrLogEventRejectedは、CloudWatch Logsが受付期間外としてeventを拒否したことを表します。
	ErrLogEventRejected = errors.New("cloudwatch logs rejected log event")
	// ErrInvalidPutLogEventsResponseは、PutLogEventsの応答からevent別の結果を確定できないことを表します。
	// 応答がない場合はErrMissingPutLogEventsOutputも含みます。
	ErrInvalidPutLogEventsResponse = errors.New("invalid cloudwatch logs PutLogEvents response")
	// ErrRequestBoundaryは、一つのspool fileから復元したrecordが一回のPutLogEvents requestに
	// 相当しないことを表します。実際に返すerrorはRequestBoundaryErrorです。
	ErrRequestBoundary = errors.New("cloudwatch logs spool file must represent exactly one PutLogEvents request")
)

// RequestBoundaryErrorのKindです。
var (
	// ErrNoSendableRecordsは、送信できるrecordが一件もないことを表します。
	ErrNoSendableRecords = errors.New("no sendable records")
	// ErrMixedLogStreamsは、recordが異なるlog streamを指すことを表します。
	ErrMixedLogStreams = errors.New("records target different log streams")
	// ErrInvalidStoredEventsは、保存されたeventを一回のrequestとして検証できないことを表します。
	ErrInvalidStoredEvents = errors.New("stored events are invalid")
	// ErrMultiplePutLogEventsRequestsは、保存されたeventに複数のrequestが必要なことを表します。
	ErrMultiplePutLogEventsRequests = errors.New("stored events require multiple requests")
)

// RequestBoundaryErrorは、一つのspool fileが一回のPutLogEvents requestに相当しない理由を表します。
// errors.Is(err, ErrRequestBoundary)とerrors.Is(err, Kind)で判定できます。
type RequestBoundaryError struct {
	// Kindは理由の種類です。
	Kind error
	// InputIndexは、最初のrecordと異なるstreamを指すrecordの入力位置です。
	InputIndex int
	// logStreamNameとwantLogStreamNameは、そのrecordと最初のrecordに保存されたstream名です。
	// 保存されたrecordの値なので表示文にだけ含め、errors.Asで取り出せる公開fieldにはしません。
	logStreamName     string
	wantLogStreamName string
	// Requestsは、保存されたeventを送るのに必要なrequest数です。
	Requests int
	// Errは、保存されたeventを検証したerrorです。
	Err error
}

func (e *RequestBoundaryError) Error() string {
	switch e.Kind {
	case ErrNoSendableRecords:
		return fmt.Sprintf("%v: no sendable records", ErrRequestBoundary)
	case ErrMixedLogStreams:
		return fmt.Sprintf(
			"%v: input record %d targets stream %q, want %q",
			ErrRequestBoundary, e.InputIndex, e.logStreamName, e.wantLogStreamName,
		)
	case ErrInvalidStoredEvents:
		return fmt.Sprintf("%v: validate stored events: %v", ErrRequestBoundary, e.Err)
	case ErrMultiplePutLogEventsRequests:
		return fmt.Sprintf("%v: stored events require %d requests", ErrRequestBoundary, e.Requests)
	default:
		return fmt.Sprintf("%v: %v", ErrRequestBoundary, e.Kind)
	}
}

// Isは、ErrRequestBoundaryとKindに一致します。
func (e *RequestBoundaryError) Is(target error) bool {
	return target == ErrRequestBoundary || (e.Kind != nil && target == e.Kind)
}

// Unwrapは、保存されたeventを検証したerrorを返します。
func (e *RequestBoundaryError) Unwrap() error { return e.Err }

// RejectedLogEventErrorは、CloudWatch Logsが受付期間外としてeventを拒否したことを表します。
// errors.Is(err, ErrLogEventRejected)で判定できます。
type RejectedLogEventError struct {
	// InputIndexは、拒否されたrecordの入力上の位置です。
	InputIndex int
	// EventIndexは、送信したrequest内のevent位置です。
	EventIndex int
	// Reasonは、CloudWatch Logsが返した拒否理由です。
	Reason pkgcloudwatchlogs.RejectedLogEventReason
}

func (e *RejectedLogEventError) Error() string {
	return fmt.Sprintf(
		"cloudwatch logs rejected input record %d at event index %d: reason=%s",
		e.InputIndex, e.EventIndex, e.Reason,
	)
}

func (e *RejectedLogEventError) Unwrap() error { return ErrLogEventRejected }

// SpoolPayloadFieldErrorは、spool payloadのfieldが再送の契約を満たさないことを表します。
// errors.Is(err, Kind)で種類を判定できます。保存されたpayloadの値は、errors.Asで取り出せる
// fieldにも原因にも含めません。
type SpoolPayloadFieldError struct {
	// Fieldは、documentのfield名です。
	Field string
	// Kindは、ErrMissingSpoolPayloadFieldまたはErrInvalidSpoolPayloadFieldです。
	Kind    error
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

// putLogEventsResponseErrorは、PutLogEventsの応答を検証したerrorの文言を保ったまま、
// ErrInvalidPutLogEventsResponseとして判定できるようにします。
type putLogEventsResponseError struct {
	missing bool
	err     error
}

func (e *putLogEventsResponseError) Error() string { return e.err.Error() }

func (e *putLogEventsResponseError) Is(target error) bool {
	return target == ErrInvalidPutLogEventsResponse ||
		(e.missing && target == cloudwatchlogs.ErrMissingPutLogEventsOutput)
}

func (e *putLogEventsResponseError) Unwrap() error { return e.err }

// CloudWatchLogsSpoolSender は spool に退避された record を CloudWatch Logs へ再送します。
//
// このspool senderはat-least-once前提です。
// PutLogEventsがCloudWatch Logs側に到達したあと、processing fileの状態遷移完了前に
// プロセスが停止すると、次回起動時に同じ record が再送される可能性があります。
// CloudWatch Logs には OpenSearch の _id 指定のような厳密な重複排除境界がないため、
// 再送による重複イベントは許容する設計です。
//
// 一つのspool fileは、初回送信時の一つのPutLogEvents requestに対応します。
// payloadに保存された実log streamを復元し、同一stream、件数、byte数、
// timestamp差のrequest境界を検証したうえで、PutLogEventsを一回実行します。
// 一つのfileが複数requestに相当する場合は再分割せず、送信対象recordをinvalidとして
// quarantineできる結果を返します。中継先などがHTTP 413でrequest全体を拒否した場合だけ、
// どのeventも受理されていないため、timestamp順を保って半分ずつに分けて再送します。
type CloudWatchLogsSpoolSender struct {
	Clock  clock.Clock
	client pkgcloudwatchlogs.Client
}

// NewCloudWatchLogsSpoolSender は CloudWatchLogsSpoolSender を生成します。
//
// clockとclientは必須です。再送先streamは、初回送信と同じrequest境界を
// 維持するため、runtime設定から再計算せずspool payloadから復元します。
func NewCloudWatchLogsSpoolSender(
	clock clock.Clock,
	client pkgcloudwatchlogs.Client,
) (*CloudWatchLogsSpoolSender, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if client == nil {
		return nil, ErrNilClient
	}
	return &CloudWatchLogsSpoolSender{
		Clock:  clock,
		client: client,
	}, nil
}

// Send は spool record を CloudWatch Logs event に復元して送信し、
// ExportReport に分類結果を詰めて返します。
//
// 分類方針:
//   - payload の復元に失敗した record と、空の message や1 eventのmessage上限
//     （pkgcloudwatchlogs.MaxPutLogEventMessageBytes）を超える record は、record単位で
//     invalid（quarantine）とし、同じfileの他のrecordは送信します
//   - PutLogEvents が成功し、CloudWatch Logs 側でも reject されなかった record は success
//   - PutLogEventsは成功したがeventがrejectされた場合、responseのindex範囲に従い、
//     too_newはretryable、too_old／expiredはpermanent（dead）、範囲外はsuccess
//   - PutLogEvents自体が失敗した場合、durable再送用の分類結果に応じて
//     送信対象record全体をretryable、permanent（dead）、結果不明ならinvalid（quarantine）
//   - HTTP 413で拒否された複数eventは分割して再送し、一件でも拒否されたeventはinvalid（quarantine）
//   - 再送可能な応答のRetry-Afterはoutcomeへ保持し、分割後の残りのeventは送信せずretryable
//   - response不在や不整合、送信中のcontext終了による結果不明はinvalid（quarantine）
//
// context終了前に未送信だったrecordだけをretryableにし、診断用のerrorも返します。
func (s *CloudWatchLogsSpoolSender) Send(
	ctx context.Context,
	records []spool.SpoolRecord,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	if len(records) == 0 {
		return report, nil
	}
	if err := ctx.Err(); err != nil {
		appendCloudWatchLogsOutcomes(
			&report,
			allCloudWatchLogsInputIndexes(len(records)),
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
		return report, err
	}

	eventRecords := make([]cloudWatchLogsSpoolEventRecord, 0, len(records))

	// SpoolRecord の Payload は CloudWatch Logs 固有の document を直接 JSON 化したものです。
	// CloudWatch Logsへ送るために、document内のlog_stream、line、timestampを復元します。
	// 初回PutLogEventsと同じrequest境界を保つため、log_streamはruntime設定から
	// 再計算しません。元messageが空のrecordは送信対象から外します。
	for inputIndex, record := range records {
		eventRecord, err := convertSpoolRecordToEventRecord(inputIndex, record)
		if err != nil {
			report = append(report, spool.RecordOutcome{
				InputIndex: inputIndex,
				Status:     spool.RecordOutcomeStatusInvalid,
				Err:        err,
			})
			continue
		}
		if err := cloudwatchlogs.ClassifyEventMessage(eventRecord.event.Message); err != nil {
			report = append(report, spool.RecordOutcome{
				InputIndex: inputIndex,
				Status:     spool.RecordOutcomeStatusInvalid,
				Err:        err,
			})
			continue
		}
		eventRecords = append(eventRecords, eventRecord)
	}

	if err := ctx.Err(); err != nil {
		appendCloudWatchLogsEventOutcomes(
			&report,
			eventRecords,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			err,
		)
		return report, err
	}
	// すべて invalid の場合は、backend へは送信せず invalid の分類だけ返します。
	if len(eventRecords) == 0 {
		return report, nil
	}

	requestRecords, streamName, contractErr := singleCloudWatchLogsSpoolRequest(eventRecords)
	if contractErr != nil {
		appendCloudWatchLogsEventOutcomes(
			&report,
			eventRecords,
			spool.RecordOutcomeStatusInvalid,
			time.Time{},
			contractErr,
		)
		return report, contractErr
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		appendCloudWatchLogsEventOutcomes(
			&report,
			requestRecords,
			spool.RecordOutcomeStatusRetryable,
			time.Time{},
			ctxErr,
		)
		return report, ctxErr
	}

	// Sender.Sendの一回の呼び出しが、このspool fileに対応するPutLogEvents試行です。
	// 別streamへのfan-outは行わず、413後の分割再送も同じ試行の継続として送信時刻を引き継ぎます。
	attemptedAt := s.Clock.Now()
	putReport, err := s.putEvents(ctx, streamName, requestRecords, attemptedAt)
	report = append(report, putReport...)
	return report, err
}

// putEventsは同じstreamのtimestamp順のrecordを一回のPutLogEventsで送信し、
// 結果を元の入力indexへ対応付けます。HTTP 413ではどのeventも受理されていないため、
// 複数eventなら前半と後半へ分けて再送し、一件でも拒否されたeventだけをquarantineへ分類します。
// 再送可能な応答のRetry-Afterはoutcomeへ保持し、分割後の残りのeventも送信しません。
func (s *CloudWatchLogsSpoolSender) putEvents(
	ctx context.Context,
	streamName string,
	records []cloudWatchLogsSpoolEventRecord,
	attemptedAt time.Time,
) (spool.ExportReport, error) {
	report := make(spool.ExportReport, 0, len(records))
	output, err := s.client.PutLogEvents(ctx, pkgcloudwatchlogs.PutLogEventsRequest{
		LogStreamName: streamName,
		Events:        cloudWatchLogsSpoolEvents(records),
		AttemptedAt:   attemptedAt,
	})
	if err == nil {
		if validateErr := output.Validate(len(records)); validateErr != nil {
			responseErr := &putLogEventsResponseError{missing: output == nil, err: validateErr}
			appendCloudWatchLogsEventOutcomes(
				&report,
				records,
				spool.RecordOutcomeStatusInvalid,
				attemptedAt,
				responseErr,
			)
			return report, responseErr
		}
		addCloudWatchLogsSpoolPutResult(&report, records, output, attemptedAt)
		return report, nil
	}

	if cloudwatchlogs.IsRequestEntityTooLarge(err) && len(records) > 1 {
		middle := len(records) / 2
		var splitErrs []error
		var deferredErr error
		for _, half := range [][]cloudWatchLogsSpoolEventRecord{records[:middle], records[middle:]} {
			if deferredErr != nil {
				// 分けたrequestは未送信です。Retry-Afterの期限まで送信せず、
				// 初回requestの送信時刻を残して再送対象にします。
				appendCloudWatchLogsEventOutcomes(&report, half, spool.RecordOutcomeStatusRetryable, attemptedAt, deferredErr)
				continue
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				// 分けたrequestは未送信です。初回requestの送信時刻を残して再送対象にします。
				appendCloudWatchLogsEventOutcomes(&report, half, spool.RecordOutcomeStatusRetryable, attemptedAt, ctxErr)
				splitErrs = append(splitErrs, ctxErr)
				continue
			}
			halfReport, halfErr := s.putEvents(ctx, streamName, half, attemptedAt)
			report = append(report, halfReport...)
			splitErrs = append(splitErrs, halfErr)
			if throttled, ok := halfReport.LongestRetryAfter(); ok {
				deferredErr = spool.DeferredByRetryAfter(throttled)
			}
		}
		return report, errors.Join(splitErrs...)
	}

	// cancelが応答の返却と競合しても、既知の応答を判断の根拠とします。
	// 試行したrequestのcontext errorから受理を確定することはできません。
	decision := cloudwatchlogs.ClassifyDurableRequestOutcome(ctx, err)
	ctxErr := ctx.Err()
	status := spool.RecordOutcomeStatusPermanent
	var retryAfter time.Duration
	if decision.Quarantine {
		status = spool.RecordOutcomeStatusInvalid
	} else if decision.Retry {
		status = spool.RecordOutcomeStatusRetryable
		retryAfter = decision.RetryAfter
	}
	appendCloudWatchLogsEventOutcomes(&report, records, status, attemptedAt, errors.Join(err, ctxErr))
	setOutcomeRetryAfter(report, retryAfter)
	return report, ctxErr
}

// setOutcomeRetryAfterは、CloudWatch Logsが再送可能な応答で指定したRetry-Afterを
// outcomeへ設定します。0以下の値は指定なしとして扱います。
func setOutcomeRetryAfter(report spool.ExportReport, retryAfter time.Duration) {
	if retryAfter <= 0 {
		return
	}
	for i := range report {
		report[i].RetryAfter = retryAfter
	}
}

// singleCloudWatchLogsSpoolRequestは、一つのspool fileから復元したrecordが
// 同じ実log streamに属し、一回のPutLogEvents制限内に収まることを検証します。
// timestamp順に並べ替えたrecordと保存済みstream名を返しますが、複数requestに
// 相当する場合は再分割せず契約errorを返します。
func singleCloudWatchLogsSpoolRequest(
	records []cloudWatchLogsSpoolEventRecord,
) ([]cloudWatchLogsSpoolEventRecord, string, error) {
	if len(records) == 0 {
		return nil, "", &RequestBoundaryError{Kind: ErrNoSendableRecords}
	}

	streamName := records[0].logStreamName
	for _, record := range records[1:] {
		if record.logStreamName == streamName {
			continue
		}
		return nil, "", &RequestBoundaryError{
			Kind:              ErrMixedLogStreams,
			InputIndex:        record.inputIndex,
			logStreamName:     record.logStreamName,
			wantLogStreamName: streamName,
		}
	}

	requestRecords := append([]cloudWatchLogsSpoolEventRecord(nil), records...)
	sortCloudWatchLogsSpoolEventRecords(requestRecords)
	batches, err := pkgcloudwatchlogs.SplitPutLogEventsBatches(
		cloudWatchLogsSpoolEvents(requestRecords),
	)
	if err != nil {
		return nil, "", &RequestBoundaryError{Kind: ErrInvalidStoredEvents, Err: err}
	}
	if len(batches) != 1 {
		return nil, "", &RequestBoundaryError{Kind: ErrMultiplePutLogEventsRequests, Requests: len(batches)}
	}

	return requestRecords, streamName, nil
}

// cloudWatchLogsSpoolEventRecord は CloudWatch Logs event と元の入力indexの対応を保持します。
//
// PutLogEventsへ渡すeventはtimestamp昇順へ並べ替える必要があり、responseのrejected event
// indexはその送信順を指します。送信順の結果を元のSpoolRecordへ戻せるよう、
// sort前のinputIndexとeventをこの構造体で一緒に保持します。
type cloudWatchLogsSpoolEventRecord struct {
	inputIndex    int
	logStreamName string
	event         pkgcloudwatchlogs.Event
}

// sortCloudWatchLogsSpoolEventRecords は CloudWatch Logs の要求に合わせて
// event timestamp の昇順に並べ替えます。
func sortCloudWatchLogsSpoolEventRecords(records []cloudWatchLogsSpoolEventRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		return cloudWatchLogsEventTimestampMillis(records[i].event.Timestamp) <
			cloudWatchLogsEventTimestampMillis(records[j].event.Timestamp)
	})
}

// cloudWatchLogsSpoolEvents は送信用の Event slice だけを取り出します。
func cloudWatchLogsSpoolEvents(records []cloudWatchLogsSpoolEventRecord) []pkgcloudwatchlogs.Event {
	if len(records) == 0 {
		return nil
	}

	events := make([]pkgcloudwatchlogs.Event, 0, len(records))
	for _, record := range records {
		events = append(events, record.event)
	}
	return events
}

// addCloudWatchLogsSpoolPutResult は PutLogEvents 成功後の結果を record 単位で分類します。
//
// PutLogEvents は HTTP request としては成功しても、古すぎる timestamp、未来 timestamp、
// retention 期間外などの理由で一部 event だけを rejected として返すことがあります。
// そのため request 成功 = 全 record 成功とは扱わず、CloudWatch Logs から返る
// rejected event index を見て record 単位で success / retryable / permanent に分けます。
func addCloudWatchLogsSpoolPutResult(
	report *spool.ExportReport,
	eventRecords []cloudWatchLogsSpoolEventRecord,
	output *pkgcloudwatchlogs.PutLogEventsOutput,
	attemptedAt time.Time,
) {
	rejectedByIndex := make(map[int]pkgcloudwatchlogs.RejectedLogEvent)
	if output != nil && output.HasRejectedLogEvents() {
		for _, rejected := range output.RejectedLogEventsInfo.RejectedEvents(len(eventRecords)) {
			rejectedByIndex[rejected.Index] = rejected
		}
	}

	for i, eventRecord := range eventRecords {
		rejected, ok := rejectedByIndex[i]
		if !ok {
			*report = append(*report, spool.RecordOutcome{
				InputIndex:  eventRecord.inputIndex,
				Status:      spool.RecordOutcomeStatusSuccess,
				AttemptedAt: attemptedAt,
				Err:         nil,
			})
			continue
		}
		rejectionErr := &RejectedLogEventError{
			InputIndex: eventRecord.inputIndex,
			EventIndex: rejected.Index,
			Reason:     rejected.Reason,
		}

		if cloudwatchlogs.ClassifyRejectedLogEvent(rejected).Retry {
			*report = append(*report, spool.RecordOutcome{
				InputIndex:  eventRecord.inputIndex,
				Status:      spool.RecordOutcomeStatusRetryable,
				AttemptedAt: attemptedAt,
				Err:         rejectionErr,
			})
			continue
		}
		*report = append(*report, spool.RecordOutcome{
			InputIndex:  eventRecord.inputIndex,
			Status:      spool.RecordOutcomeStatusPermanent,
			AttemptedAt: attemptedAt,
			Err:         rejectionErr,
		})
	}
}

func allCloudWatchLogsInputIndexes(inputCount int) []int {
	inputIndexes := make([]int, inputCount)
	for inputIndex := range inputIndexes {
		inputIndexes[inputIndex] = inputIndex
	}
	return inputIndexes
}

func appendCloudWatchLogsEventOutcomes(
	report *spool.ExportReport,
	eventRecords []cloudWatchLogsSpoolEventRecord,
	status spool.RecordOutcomeStatus,
	attemptedAt time.Time,
	err error,
) {
	inputIndexes := make([]int, len(eventRecords))
	for i, eventRecord := range eventRecords {
		inputIndexes[i] = eventRecord.inputIndex
	}
	appendCloudWatchLogsOutcomes(report, inputIndexes, status, attemptedAt, err)
}

func appendCloudWatchLogsOutcomes(
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

// cloudWatchLogsEventTimestampMillis は CloudWatch Logs の並び順判定に使う
// millisecond precision の Unix timestamp を返します。
func cloudWatchLogsEventTimestampMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

// convertSpoolRecordToEventRecordはspool recordのpayloadから、初回PutLogEventsに使用した
// 実log streamとCloudWatch Logs eventを復元します。
//
// payloadはCloudWatch Logs固有documentを直接JSON化したものです。documentの
// log_streamを再送request先、lineをevent message、timestampをevent timestampに使います。
// messageが存在して空の場合は、元LogEntryが空messageだったrecordとしてinvalidにします。
func convertSpoolRecordToEventRecord(
	inputIndex int,
	record spool.SpoolRecord,
) (cloudWatchLogsSpoolEventRecord, error) {
	if len(record.Payload) == 0 {
		return cloudWatchLogsSpoolEventRecord{}, ErrEmptySpoolPayload
	}

	var doc map[string]any
	if err := json.Unmarshal(record.Payload, &doc); err != nil {
		// decodeのerrorは保存されたpayloadの値を保持し得るため、errors.Asで取り出せないよう
		// 原因としては包まず、文言だけを残します。種類はErrDecodeSpoolPayloadで判定します。
		return cloudWatchLogsSpoolEventRecord{}, fmt.Errorf("%w: %v", ErrDecodeSpoolPayload, err)
	}
	if doc == nil {
		return cloudWatchLogsSpoolEventRecord{}, ErrNilSpoolPayloadDocument
	}

	if err := validateOriginalMessage(doc); err != nil {
		return cloudWatchLogsSpoolEventRecord{}, err
	}

	streamName, err := stringField(doc, "log_stream")
	if err != nil {
		return cloudWatchLogsSpoolEventRecord{}, err
	}
	if err := cloudwatchlogs.ValidateApplicationLogStreamName(streamName); err != nil {
		// 保存されたstream名の文字はerrors.Asで取り出せないよう、LogStreamNameCharacterErrorは
		// 文言と種類のsentinelだけを残します。ほかの検証errorはstream名を値として保持しません。
		var characterErr *cloudwatchlogs.LogStreamNameCharacterError
		if errors.As(err, &characterErr) {
			err = storedvalue.WithoutValues(err, characterErr.Kind)
		}
		return cloudWatchLogsSpoolEventRecord{}, err
	}

	line, err := stringField(doc, "line")
	if err != nil {
		return cloudWatchLogsSpoolEventRecord{}, err
	}
	timestamp, err := timestampField(doc, "timestamp")
	if err != nil {
		return cloudWatchLogsSpoolEventRecord{}, err
	}

	return cloudWatchLogsSpoolEventRecord{
		inputIndex:    inputIndex,
		logStreamName: streamName,
		event: pkgcloudwatchlogs.Event{
			Timestamp: timestamp,
			Message:   line,
		},
	}, nil
}

// validateOriginalMessage は保存元 LogEntry の message が空の場合に再送対象から外します。
// source LogEntry がない record は message field を持たないため、line の検証に委ねます。
func validateOriginalMessage(doc map[string]any) error {
	raw, ok := doc["message"]
	if !ok {
		return nil
	}
	value, ok := raw.(string)
	if !ok {
		return &SpoolPayloadFieldError{
			Field: "message", Kind: ErrInvalidSpoolPayloadField,
			message: "document message is not a string",
		}
	}
	return cloudwatchlogs.ClassifyEventMessage(value)
}

// stringField は doc から必須の非空 string field を取り出します。
func stringField(doc map[string]any, key string) (string, error) {
	raw, ok := doc[key]
	if !ok {
		return "", &SpoolPayloadFieldError{
			Field: key, Kind: ErrMissingSpoolPayloadField,
			message: fmt.Sprintf("document %s is missing", key),
		}
	}
	value, ok := raw.(string)
	if !ok || value == "" {
		return "", &SpoolPayloadFieldError{
			Field: key, Kind: ErrInvalidSpoolPayloadField,
			message: fmt.Sprintf("document %s is not a non-empty string", key),
		}
	}
	return value, nil
}

// timestampField は doc から RFC3339Nano 形式の timestamp field を取り出します。
func timestampField(doc map[string]any, key string) (time.Time, error) {
	value, err := stringField(doc, key)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		// 解析のerror（*time.ParseError）は保存された値を保持するため、原因としては持たず、
		// 文言だけを残します。
		return time.Time{}, &SpoolPayloadFieldError{
			Field: key, Kind: ErrInvalidSpoolPayloadField,
			message: fmt.Sprintf("document %s is not RFC3339Nano timestamp: %v", key, err),
		}
	}
	return t, nil
}
