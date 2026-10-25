package cloudwatchlogs

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

const maxPutLogEventsTimeSpan = 24 * time.Hour

var (
	// ErrEventTimestampRequiredは、batch分割に必要なeventのtimestampがzero値であることを
	// 表します。
	ErrEventTimestampRequired = errors.New("cloudwatch logs event timestamp is required")
	// ErrEventMessageEmptyは、eventのmessageが空であることを表します。CloudWatch Logsは
	// 空のmessageを受け付けません。
	ErrEventMessageEmpty = errors.New("cloudwatch logs event message is empty")
	// ErrEventsNotSortedは、eventsがtimestamp昇順ではないことを表します。
	ErrEventsNotSorted = errors.New("cloudwatch logs event timestamps must be sorted ascending")
	// ErrTooManyEventsは、一回のrequestのevent件数がMaxPutLogEventsCountを超えることを表します。
	ErrTooManyEvents = errors.New("cloudwatch logs put batch has too many events")
	// ErrBatchTooLargeは、一回のrequestのbyte数がCloudWatch Logsの上限を超えることを表します。
	ErrBatchTooLarge = errors.New("cloudwatch logs put batch is too large")
	// ErrBatchTimeSpanTooLongは、一回のrequest内の最古eventと最新eventのtimestamp差が
	// 24時間を超えることを表します。
	ErrBatchTimeSpanTooLong = errors.New("cloudwatch logs put batch time span exceeds 24h")
)

// SplitPutLogEventsBatchesは、timestamp昇順のeventsをCloudWatch Logsの一request当たりの
// 件数、byte数、最古eventと最新eventのtimestamp差（最大24時間）の上限に収まる
// 連続したbatchへ分割します。24時間は暦日ではなく、一request内のtimestamp差です。
//
// AWSの受付可能期間は検証しません。古すぎるeventや未来のeventも送信し、AWS responseの
// rejectedLogEventsInfoを正として呼び出し側が分類します。入力順は変更せず、各batch内と
// batch間で元の順序を維持します。messageがMaxPutLogEventMessageBytesを超え、単独でも
// 一回のrequestに収まらないeventはerrorにします。
func SplitPutLogEventsBatches(events []Event) ([][]Event, error) {
	if len(events) == 0 {
		return nil, nil
	}

	batches := make([][]Event, 0, 1)
	batchStart := 0
	batchBytes := 0
	var batchFirstTimestamp int64
	var previousTimestamp int64

	for index, event := range events {
		if event.Timestamp.IsZero() {
			return nil, &validationError{kind: ErrEventTimestampRequired, message: fmt.Sprintf(
				"invalid cloudwatch logs put batch: event %d timestamp is required for batch splitting",
				index,
			)}
		}
		messageBytes := jsonutil.UTF8StringBytes(event.Message)
		if event.Message == "" {
			return nil, &validationError{kind: ErrEventMessageEmpty, message: fmt.Sprintf(
				"invalid cloudwatch logs put batch: event %d message is empty", index,
			)}
		}
		// message上限は付加サイズを含めて一回のrequestへ収まる値なので、
		// この判定を通ったeventは単独のbatchとして送信できます。
		if messageBytes > MaxPutLogEventMessageBytes {
			return nil, &EventTooLargeError{
				Index:        index,
				MessageBytes: messageBytes,
				MaxBytes:     MaxPutLogEventMessageBytes,
			}
		}
		eventBytes := messageBytes + putLogEventOverheadBytes

		timestamp := eventTimestampMillis(event.Timestamp)
		if index > 0 && previousTimestamp > timestamp {
			return nil, unsortedEventsError(index, timestamp, previousTimestamp)
		}

		batchCount := index - batchStart
		if batchCount == 0 {
			batchFirstTimestamp = timestamp
		}
		if batchCount > 0 &&
			(batchCount >= MaxPutLogEventsCount ||
				batchBytes+eventBytes > maxPutLogEventsBatchBytes ||
				timestamp-batchFirstTimestamp > maxPutLogEventsTimeSpan.Milliseconds()) {
			batches = append(batches, append([]Event(nil), events[batchStart:index]...))
			batchStart = index
			batchBytes = 0
			batchFirstTimestamp = timestamp
		}

		batchBytes += eventBytes
		previousTimestamp = timestamp
	}

	batches = append(batches, append([]Event(nil), events[batchStart:]...))
	return batches, nil
}

// unsortedEventsErrorは、index番目のeventのtimestampが直前のeventより前であることを、
// ErrEventsNotSortedを持つerrorとして返します。
func unsortedEventsError(index int, timestamp, previousTimestamp int64) error {
	return &validationError{kind: ErrEventsNotSorted, message: fmt.Sprintf(
		"invalid cloudwatch logs put batch: event timestamps must be sorted ascending: event %d timestamp %d is before event %d timestamp %d",
		index,
		timestamp,
		index-1,
		previousTimestamp,
	)}
}

// EventTooLargeErrorは、一件のeventのmessageがPutLogEventsで送信できる上限を超えることを
// 表します。このeventはbatchを分割しても送信できません。
type EventTooLargeError struct {
	// Indexは入力events内の位置です。
	Index int
	// MessageBytesはmessageのUTF-8 byte数、MaxBytesは一件のmessageに許される上限です。
	MessageBytes int
	MaxBytes     int
}

func (e *EventTooLargeError) Error() string {
	return fmt.Sprintf(
		"invalid cloudwatch logs put batch: event %d size %d exceeds %d bytes",
		e.Index,
		e.MessageBytes,
		e.MaxBytes,
	)
}
