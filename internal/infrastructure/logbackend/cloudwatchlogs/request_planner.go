package cloudwatchlogs

import (
	"errors"
	"fmt"
	"sort"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// PlanPutLogEventsRequestsへ渡したInputIndexが契約に反することを表すerrorの種類です。
var (
	// ErrNegativeEventInputIndexは、InputIndexが負であることを表します。
	ErrNegativeEventInputIndex = errors.New("cloudwatch logs indexed event has negative input index")
	// ErrDuplicateEventInputIndexは、同じInputIndexが二度以上現れたことを表します。
	ErrDuplicateEventInputIndex = errors.New("cloudwatch logs indexed event input index is duplicated")
)

// EventInputIndexErrorは、InputIndexの契約違反と、その値・入力内の位置を表します。
// Kindは、ErrNegativeEventInputIndexまたはErrDuplicateEventInputIndexで、errors.Isで判定できます。
type EventInputIndexError struct {
	Kind       error
	Position   int
	InputIndex int
}

func (e *EventInputIndexError) Error() string {
	if e.Kind == ErrNegativeEventInputIndex {
		return fmt.Sprintf(
			"cloudwatch logs indexed event at position %d has negative input index %d",
			e.Position,
			e.InputIndex,
		)
	}
	return fmt.Sprintf("cloudwatch logs indexed event input index %d is duplicated", e.InputIndex)
}

func (e *EventInputIndexError) Unwrap() error { return e.Kind }

// IndexedEventは、CloudWatch Logsへ送るeventと、分割前の入力内での位置を保持します。
// InputIndexはrequest内での並び順ではなく、plannerの呼び出し元が渡した入力を識別する値です。
type IndexedEvent struct {
	InputIndex int
	Event      pkgcloudwatchlogs.Event
}

// PlannedPutLogEventsRequestは、一回のPutLogEventsで送信できるevent群です。
// Eventsは同じLogStreamNameに属し、timestamp昇順かつAWSのrequest上限内です。
// InputIndexは並べ替えと分割の前の値を保持します。
type PlannedPutLogEventsRequest struct {
	LogStreamName string
	Events        []IndexedEvent
}

// InvalidIndexedEventは、単独でもPutLogEventsへ送れないeventと、その原因を保持します。
// 他の正常なeventの計画は継続できるため、planner全体のerrorとは分けて返します。
type InvalidIndexedEvent struct {
	IndexedEvent
	Err error
}

// PlanPutLogEventsRequestsは、event発生日時から実log streamを解決し、streamごとに
// timestamp昇順へ安定sortした後、CloudWatch Logsの件数、byte数、timestamp差の制限に
// 収まる物理PutLogEvents requestへ分割します。
//
// 同じmillisecondのeventは入力順を維持します。stream間は最古eventのtimestamp順、
// 同値ならstream名順に並べるため、mapの走査順に依存しません。単独でも送れないeventは
// InvalidIndexedEventとして返し、それ以外のrequest計画は継続します。
// InputIndexは0以上かつ入力内で一意でなければならず、違反は呼び出し側の契約errorです。
func PlanPutLogEventsRequests(
	resolver *LogStreamNameResolver,
	events []IndexedEvent,
) ([]PlannedPutLogEventsRequest, []InvalidIndexedEvent, error) {
	if len(events) == 0 {
		return nil, nil, nil
	}
	if resolver == nil {
		return nil, nil, fmt.Errorf("cloudwatch logs stream name resolver is nil")
	}

	eventsByStream := make(map[string][]IndexedEvent)
	invalid := make([]InvalidIndexedEvent, 0)
	seenInputIndexes := make(map[int]struct{}, len(events))
	for position, indexedEvent := range events {
		if indexedEvent.InputIndex < 0 {
			return nil, nil, &EventInputIndexError{
				Kind:       ErrNegativeEventInputIndex,
				Position:   position,
				InputIndex: indexedEvent.InputIndex,
			}
		}
		if _, exists := seenInputIndexes[indexedEvent.InputIndex]; exists {
			return nil, nil, &EventInputIndexError{
				Kind:       ErrDuplicateEventInputIndex,
				Position:   position,
				InputIndex: indexedEvent.InputIndex,
			}
		}
		seenInputIndexes[indexedEvent.InputIndex] = struct{}{}

		if _, err := pkgcloudwatchlogs.SplitPutLogEventsBatches(
			[]pkgcloudwatchlogs.Event{indexedEvent.Event},
		); err != nil {
			invalid = append(invalid, InvalidIndexedEvent{
				IndexedEvent: indexedEvent,
				Err:          err,
			})
			continue
		}

		streamName, err := resolver.ResolveLogStreamNameForEvent(indexedEvent.Event.Timestamp)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"resolve cloudwatch logs stream for input index %d: %w",
				indexedEvent.InputIndex,
				err,
			)
		}
		eventsByStream[streamName] = append(eventsByStream[streamName], indexedEvent)
	}

	type streamEventGroup struct {
		streamName string
		events     []IndexedEvent
	}
	groups := make([]streamEventGroup, 0, len(eventsByStream))
	for streamName, streamEvents := range eventsByStream {
		sort.SliceStable(streamEvents, func(left, right int) bool {
			return streamEvents[left].Event.Timestamp.UnixMilli() <
				streamEvents[right].Event.Timestamp.UnixMilli()
		})
		groups = append(groups, streamEventGroup{
			streamName: streamName,
			events:     streamEvents,
		})
	}
	sort.Slice(groups, func(left, right int) bool {
		leftTimestamp := groups[left].events[0].Event.Timestamp.UnixMilli()
		rightTimestamp := groups[right].events[0].Event.Timestamp.UnixMilli()
		if leftTimestamp != rightTimestamp {
			return leftTimestamp < rightTimestamp
		}
		return groups[left].streamName < groups[right].streamName
	})

	requests := make([]PlannedPutLogEventsRequest, 0, len(groups))
	for _, group := range groups {
		groupEvents := make([]pkgcloudwatchlogs.Event, len(group.events))
		for index, indexedEvent := range group.events {
			groupEvents[index] = indexedEvent.Event
		}

		batches, err := pkgcloudwatchlogs.SplitPutLogEventsBatches(groupEvents)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"split cloudwatch logs events for stream %q: %w",
				group.streamName,
				err,
			)
		}

		nextEvent := 0
		for _, batch := range batches {
			end := nextEvent + len(batch)
			requests = append(requests, PlannedPutLogEventsRequest{
				LogStreamName: group.streamName,
				Events:        append([]IndexedEvent(nil), group.events[nextEvent:end]...),
			})
			nextEvent = end
		}
	}

	return requests, invalid, nil
}
