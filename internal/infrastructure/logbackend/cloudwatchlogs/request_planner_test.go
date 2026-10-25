package cloudwatchlogs

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestPlanPutLogEventsRequestsRoutesSortsAndKeepsInputIndexes(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	streamATime := base.Add(100 * time.Nanosecond)
	streamBTime := base.Add(time.Hour + 200*time.Nanosecond)
	resolver := newRequestPlannerResolver(t, "stream", streamrouting.Hourly)
	input := []IndexedEvent{
		{InputIndex: 7, Event: pkgcloudwatchlogs.Event{Timestamp: streamBTime, Message: "b-first"}},
		{InputIndex: 3, Event: pkgcloudwatchlogs.Event{Timestamp: streamATime, Message: "a-first"}},
		{InputIndex: 9, Event: pkgcloudwatchlogs.Event{Timestamp: streamBTime, Message: "b-second"}},
		{InputIndex: 2, Event: pkgcloudwatchlogs.Event{Timestamp: streamATime, Message: "a-second"}},
	}

	requests, invalid, err := PlanPutLogEventsRequests(resolver, input)
	if err != nil {
		t.Fatalf("PlanPutLogEventsRequests() error = %v", err)
	}
	if len(invalid) != 0 {
		t.Fatalf("invalid events = %#v, want empty", invalid)
	}
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if requests[0].LogStreamName != "stream/2026/08/14/12" ||
		requests[1].LogStreamName != "stream/2026/08/14/13" {
		t.Fatalf(
			"stream order = [%q, %q], want hourly stream names",
			requests[0].LogStreamName,
			requests[1].LogStreamName,
		)
	}
	if got, want := plannedInputIndexes(requests[0]), []int{3, 2}; !slices.Equal(got, want) {
		t.Fatalf("stream-a input indexes = %v, want %v", got, want)
	}
	if got, want := plannedInputIndexes(requests[1]), []int{7, 9}; !slices.Equal(got, want) {
		t.Fatalf("stream-b input indexes = %v, want %v", got, want)
	}
	if got, want := []int{input[0].InputIndex, input[1].InputIndex, input[2].InputIndex, input[3].InputIndex},
		[]int{7, 3, 9, 2}; !slices.Equal(got, want) {
		t.Fatalf("input order was mutated: got %v, want %v", got, want)
	}
}

func TestPlanPutLogEventsRequestsSplitsPhysicalRequestLimits(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.August, 14, 0, 0, 0, 0, time.UTC)
	tooManyEvents := make([]IndexedEvent, pkgcloudwatchlogs.MaxPutLogEventsCount+1)
	for index := range tooManyEvents {
		tooManyEvents[index] = IndexedEvent{
			InputIndex: index,
			Event: pkgcloudwatchlogs.Event{
				Timestamp: base.Add(time.Duration(index) * time.Millisecond),
				Message:   "count-limited",
			},
		}
	}

	tests := []struct {
		name   string
		events []IndexedEvent
		want   []int
	}{
		{
			name:   "event count",
			events: tooManyEvents,
			want:   []int{pkgcloudwatchlogs.MaxPutLogEventsCount, 1},
		},
		{
			name: "request bytes",
			events: []IndexedEvent{
				{
					InputIndex: 0,
					Event: pkgcloudwatchlogs.Event{
						Timestamp: base,
						Message:   strings.Repeat("a", 600_000),
					},
				},
				{
					InputIndex: 1,
					Event: pkgcloudwatchlogs.Event{
						Timestamp: base.Add(time.Second),
						Message:   strings.Repeat("b", 600_000),
					},
				},
			},
			want: []int{1, 1},
		},
		{
			name: "timestamp span",
			events: []IndexedEvent{
				{InputIndex: 2, Event: pkgcloudwatchlogs.Event{Timestamp: base.Add(24*time.Hour + time.Millisecond), Message: "next"}},
				{InputIndex: 0, Event: pkgcloudwatchlogs.Event{Timestamp: base, Message: "first"}},
				{InputIndex: 1, Event: pkgcloudwatchlogs.Event{Timestamp: base.Add(24 * time.Hour), Message: "boundary"}},
			},
			want: []int{2, 1},
		},
	}

	resolver := newRequestPlannerResolver(t, "stream-a", streamrouting.None)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			requests, invalid, err := PlanPutLogEventsRequests(resolver, tt.events)
			if err != nil {
				t.Fatalf("PlanPutLogEventsRequests() error = %v", err)
			}
			if len(invalid) != 0 {
				t.Fatalf("invalid events = %#v, want empty", invalid)
			}
			if len(requests) != len(tt.want) {
				t.Fatalf("request count = %d, want %d", len(requests), len(tt.want))
			}
			for index, wantCount := range tt.want {
				if got := len(requests[index].Events); got != wantCount {
					t.Fatalf("request[%d] event count = %d, want %d", index, got, wantCount)
				}
				if requests[index].LogStreamName != "stream-a" {
					t.Fatalf("request[%d] stream = %q, want stream-a", index, requests[index].LogStreamName)
				}
			}
		})
	}
}

func TestPlanPutLogEventsRequestsReturnsSingleEventFailuresByInputIndex(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	resolver := newRequestPlannerResolver(t, "stream-a", streamrouting.None)
	input := []IndexedEvent{
		{InputIndex: 10, Event: pkgcloudwatchlogs.Event{Timestamp: base, Message: "valid"}},
		{InputIndex: 20, Event: pkgcloudwatchlogs.Event{Timestamp: base, Message: ""}},
		{
			InputIndex: 30,
			Event: pkgcloudwatchlogs.Event{
				Timestamp: base,
				Message:   strings.Repeat("x", pkgcloudwatchlogs.MaxPutLogEventMessageBytes+1),
			},
		},
		{InputIndex: 40, Event: pkgcloudwatchlogs.Event{Message: "timestamp is missing"}},
	}

	requests, invalid, err := PlanPutLogEventsRequests(resolver, input)
	if err != nil {
		t.Fatalf("PlanPutLogEventsRequests() error = %v", err)
	}
	if len(requests) != 1 || !slices.Equal(plannedInputIndexes(requests[0]), []int{10}) {
		t.Fatalf("planned requests = %#v, want only input index 10", requests)
	}
	if len(invalid) != 3 {
		t.Fatalf("invalid count = %d, want 3", len(invalid))
	}
	if got, want := []int{invalid[0].InputIndex, invalid[1].InputIndex, invalid[2].InputIndex},
		[]int{20, 30, 40}; !slices.Equal(got, want) {
		t.Fatalf("invalid input indexes = %v, want %v", got, want)
	}
	for index, invalidEvent := range invalid {
		if invalidEvent.Err == nil {
			t.Fatalf("invalid[%d] Err = nil", index)
		}
	}
}

func TestPlanPutLogEventsRequestsAgreesWithEventMessageClassifierAtSizeLimit(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	resolver := newRequestPlannerResolver(t, "stream-a", streamrouting.None)
	for _, tc := range []struct {
		name         string
		messageBytes int
		wantPlanned  bool
	}{
		{name: "largest message", messageBytes: pkgcloudwatchlogs.MaxPutLogEventMessageBytes, wantPlanned: true},
		{name: "one byte over", messageBytes: pkgcloudwatchlogs.MaxPutLogEventMessageBytes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := pkgcloudwatchlogs.Event{Timestamp: base, Message: strings.Repeat("x", tc.messageBytes)}
			classifyErr := ClassifyEventMessage(event.Message)
			requests, invalid, err := PlanPutLogEventsRequests(resolver, []IndexedEvent{{InputIndex: 1, Event: event}})
			if err != nil {
				t.Fatalf("PlanPutLogEventsRequests() error = %v", err)
			}
			// classifierが受け付けるeventは、plannerも単独のrequestとして計画します。
			planned := len(requests) == 1 && slices.Equal(plannedInputIndexes(requests[0]), []int{1})
			if planned != tc.wantPlanned || (classifyErr == nil) != tc.wantPlanned {
				t.Fatalf("planned=%t classifier error=%v, want planned=%t", planned, classifyErr, tc.wantPlanned)
			}
			if !tc.wantPlanned && (len(invalid) != 1 || invalid[0].InputIndex != 1 || invalid[0].Err == nil) {
				t.Fatalf("invalid events = %#v, want input index 1 rejected", invalid)
			}
		})
	}
}

func TestPlanPutLogEventsRequestsRejectsInvalidInputIndexes(t *testing.T) {
	t.Parallel()

	resolver := newRequestPlannerResolver(t, "stream-a", streamrouting.None)
	validEvent := pkgcloudwatchlogs.Event{
		Timestamp: time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC),
		Message:   "valid",
	}
	tests := []struct {
		name   string
		events []IndexedEvent
		want   EventInputIndexError
	}{
		{
			name:   "negative",
			events: []IndexedEvent{{InputIndex: -1, Event: validEvent}},
			want:   EventInputIndexError{Kind: ErrNegativeEventInputIndex, Position: 0, InputIndex: -1},
		},
		{
			name: "duplicate",
			events: []IndexedEvent{
				{InputIndex: 4, Event: validEvent},
				{InputIndex: 4, Event: validEvent},
			},
			want: EventInputIndexError{Kind: ErrDuplicateEventInputIndex, Position: 1, InputIndex: 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			requests, invalid, err := PlanPutLogEventsRequests(resolver, tt.events)
			var indexErr *EventInputIndexError
			if !errors.As(err, &indexErr) || *indexErr != tt.want {
				t.Fatalf("PlanPutLogEventsRequests() error = %#v, want %#v", err, tt.want)
			}
			if requests != nil || invalid != nil {
				t.Fatalf("outputs = requests:%#v invalid:%#v, want nil on contract error", requests, invalid)
			}
		})
	}
}

func TestPlanPutLogEventsRequestsHandlesEmptyInputBeforeResolverValidation(t *testing.T) {
	t.Parallel()

	requests, invalid, err := PlanPutLogEventsRequests(nil, nil)
	if err != nil {
		t.Fatalf("PlanPutLogEventsRequests() error = %v", err)
	}
	if requests != nil || invalid != nil {
		t.Fatalf("outputs = requests:%#v invalid:%#v, want nil", requests, invalid)
	}
}

func newRequestPlannerResolver(
	t *testing.T,
	baseName string,
	granularity streamrouting.Granularity,
) *LogStreamNameResolver {
	t.Helper()

	resolver, err := NewLogStreamNameResolver(
		baseName,
		granularity,
	)
	if err != nil {
		t.Fatalf("NewLogStreamNameResolver() error = %v", err)
	}
	return resolver
}

func plannedInputIndexes(request PlannedPutLogEventsRequest) []int {
	indexes := make([]int, len(request.Events))
	for index, event := range request.Events {
		indexes[index] = event.InputIndex
	}
	return indexes
}
