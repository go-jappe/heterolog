package cloudwatchlogs

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

const maxPutLogEventsTimeSpan = 24 * time.Hour

var (
	// ErrEventTimestampRequired reports that the timestamp of an event, which
	// batch splitting needs, is the zero value.
	ErrEventTimestampRequired = errors.New("cloudwatch logs event timestamp is required")
	// ErrEventMessageEmpty reports that the message of an event is empty.
	// CloudWatch Logs does not accept an empty message.
	ErrEventMessageEmpty = errors.New("cloudwatch logs event message is empty")
	// ErrEventsNotSorted reports that events are not in ascending timestamp order.
	ErrEventsNotSorted = errors.New("cloudwatch logs event timestamps must be sorted ascending")
	// ErrTooManyEvents reports that the number of events in one request exceeds
	// MaxPutLogEventsCount.
	ErrTooManyEvents = errors.New("cloudwatch logs put batch has too many events")
	// ErrBatchTooLarge reports that the number of bytes in one request exceeds
	// the CloudWatch Logs limit.
	ErrBatchTooLarge = errors.New("cloudwatch logs put batch is too large")
	// ErrBatchTimeSpanTooLong reports that the difference between the timestamps
	// of the oldest and the newest event in one request exceeds 24 hours.
	ErrBatchTimeSpanTooLong = errors.New("cloudwatch logs put batch time span exceeds 24h")
)

// SplitPutLogEventsBatches splits events in ascending timestamp order into
// consecutive batches that fit the CloudWatch Logs per-request limits on the
// number of events, the number of bytes, and the difference between the
// timestamps of the oldest and the newest event (at most 24 hours). The 24
// hours are not a calendar day but a timestamp difference within one request.
//
// It does not check the period in which AWS accepts events. Events that are
// too old or in the future are sent as well, and the caller classifies them,
// treating rejectedLogEventsInfo in the AWS response as authoritative. It does
// not change the input order and keeps the original order within each batch
// and across batches. It returns an error for an event whose message exceeds
// MaxPutLogEventMessageBytes, because such an event does not fit in one
// request even on its own.
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

// EventTooLargeError reports that the message of one event exceeds the limit
// of what PutLogEvents can send. The event cannot be sent even if the batch is
// split.
type EventTooLargeError struct {
	// Index is the position in the input events.
	Index int
	// MessageBytes is the size of the message as it is sent: its UTF-8 bytes,
	// with each invalid UTF-8 byte counted as the three bytes of the U+FFFD
	// that JSON encoding substitutes for it. MaxBytes is the limit allowed for
	// one message.
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
