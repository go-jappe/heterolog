package eventsink

import "context"

type EventSink interface {
	Append(ctx context.Context, e Event) error
}

type Event struct{}
