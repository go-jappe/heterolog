package cloudwatchlogs

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSplitPutLogEventsBatchesAppliesRequestLimitsWithoutAgeCutoff(t *testing.T) {
	now := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	tooManyForOneRequest := make([]Event, MaxPutLogEventsCount+1)
	for index := range tooManyForOneRequest {
		tooManyForOneRequest[index] = Event{
			Timestamp: now.Add(time.Duration(index) * time.Millisecond),
			Message:   "event-" + strconv.Itoa(index),
		}
	}
	tests := []struct {
		name   string
		events []Event
		want   []int
	}{
		{
			name: "old event is retained",
			events: []Event{
				{Timestamp: now.AddDate(0, 0, -30), Message: "old"},
			},
			want: []int{1},
		},
		{
			name: "time span",
			events: []Event{
				{Timestamp: now, Message: "first"},
				{Timestamp: now.Add(24 * time.Hour), Message: "boundary"},
				{Timestamp: now.Add(24*time.Hour + time.Millisecond), Message: "next"},
			},
			want: []int{2, 1},
		},
		{
			name:   "event count",
			events: tooManyForOneRequest,
			want:   []int{MaxPutLogEventsCount, 1},
		},
		{
			name: "request bytes",
			events: []Event{
				{Timestamp: now, Message: strings.Repeat("a", 600_000)},
				{Timestamp: now.Add(time.Second), Message: strings.Repeat("b", 600_000)},
			},
			want: []int{1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantEvents := append([]Event(nil), tt.events...)
			batches, err := SplitPutLogEventsBatches(tt.events)
			if err != nil {
				t.Fatalf("SplitPutLogEventsBatches() error = %v", err)
			}
			if len(batches) != len(tt.want) {
				t.Fatalf("batch count = %d, want %d", len(batches), len(tt.want))
			}
			eventIndex := 0
			for index, wantCount := range tt.want {
				if got := len(batches[index]); got != wantCount {
					t.Fatalf("batch[%d] event count = %d, want %d", index, got, wantCount)
				}
				for offset, got := range batches[index] {
					want := wantEvents[eventIndex]
					if !got.Timestamp.Equal(want.Timestamp) {
						t.Errorf("batch[%d][%d] timestamp = %v, want input[%d] timestamp %v", index, offset, got.Timestamp, eventIndex, want.Timestamp)
					}
					if got.Message != want.Message {
						t.Errorf("batch[%d][%d] message differs from input[%d] (got %d bytes, want %d bytes)", index, offset, eventIndex, len(got.Message), len(want.Message))
					}
					eventIndex++
				}
			}
			if eventIndex != len(wantEvents) {
				t.Errorf("total events = %d, want %d", eventIndex, len(wantEvents))
			}
		})
	}
}

func TestSplitPutLogEventsBatchesRejectsUnsortedEvents(t *testing.T) {
	now := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	_, err := SplitPutLogEventsBatches([]Event{
		{Timestamp: now.Add(time.Second), Message: "later"},
		{Timestamp: now, Message: "earlier"},
	})
	if !errors.Is(err, ErrEventsNotSorted) {
		t.Fatalf("SplitPutLogEventsBatches() error = %v, want %v", err, ErrEventsNotSorted)
	}
}

func TestPutLogEventMessageLimitFitsOneEventRequest(t *testing.T) {
	// AWSはrequest sizeをmessageのUTF-8 byte数とevent当たり26 byteの合計で数えます。
	if MaxPutLogEventMessageBytes != 1_048_576-26 {
		t.Fatalf("MaxPutLogEventMessageBytes = %d, want 1,048,576 - 26", MaxPutLogEventMessageBytes)
	}
	now := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)

	largest := []Event{{Timestamp: now, Message: strings.Repeat("x", MaxPutLogEventMessageBytes)}}
	batches, err := SplitPutLogEventsBatches(largest)
	if err != nil || len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("SplitPutLogEventsBatches(limit) = %d batches, %v; want one single-event batch", len(batches), err)
	}
	if _, err := preparePutLogEvents(largest, now); err != nil {
		t.Fatalf("preparePutLogEvents(limit) error = %v", err)
	}

	tooLarge := []Event{{Timestamp: now, Message: strings.Repeat("x", MaxPutLogEventMessageBytes+1)}}
	want := EventTooLargeError{Index: 0, MessageBytes: 1_048_551, MaxBytes: 1_048_550}
	_, splitErr := SplitPutLogEventsBatches(tooLarge)
	_, prepareErr := preparePutLogEvents(tooLarge, now)
	for name, err := range map[string]error{
		"SplitPutLogEventsBatches": splitErr,
		"preparePutLogEvents":      prepareErr,
	} {
		var tooLargeErr *EventTooLargeError
		if !errors.As(err, &tooLargeErr) || *tooLargeErr != want {
			t.Fatalf("%s(limit+1) error = %v, want %+v", name, err, want)
		}
	}
}
