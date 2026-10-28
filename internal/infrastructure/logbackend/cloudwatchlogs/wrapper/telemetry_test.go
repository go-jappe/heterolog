package wrapper

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// retriedOncerecordingClientは、最初の試行だけ一時的なnetwork errorで失敗します。
func retriedOncerecordingClient() *recordingClient {
	client := &recordingClient{}
	calls := 0
	client.send = func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		calls++
		if calls == 1 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		return &cloudwatchlogs.PutLogEventsOutput{}, nil
	}
	return client
}

func TestRetryWrappersTraceEachAttemptAndCountRetries(t *testing.T) {
	for _, wrapper := range []struct {
		name  string
		build func(next cloudwatchlogs.Client, tracer *observertest.Tracer, meter *observertest.Meter) cloudwatchlogs.Client
	}{
		{"retrying", func(next cloudwatchlogs.Client, tracer *observertest.Tracer, meter *observertest.Meter) cloudwatchlogs.Client {
			return newRetryingClient(next, fastRetryConfig(2), tracer, meter)
		}},
		{"retry policy", func(next cloudwatchlogs.Client, tracer *observertest.Tracer, meter *observertest.Meter) cloudwatchlogs.Client {
			return newRetryPolicyClient(next, fastRetryConfig(2), tracer, meter)
		}},
	} {
		t.Run(wrapper.name, func(t *testing.T) {
			tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
			client := wrapper.build(retriedOncerecordingClient(), tracer, meter)

			if _, err := client.PutLogEvents(context.Background(), cloudwatchlogs.PutLogEventsRequest{}); err != nil {
				t.Fatalf("call error = %v, want success after one retry", err)
			}

			attempts := tracer.Spans(spanNamePutLogEvents)
			if len(attempts) != 2 {
				t.Fatalf("attempt spans = %d, want 2", len(attempts))
			}
			for i, span := range attempts {
				if v, _ := span.Attr(telemetry.SpanAttrAttempt); v != i+1 || span.Ends() != 1 {
					t.Errorf("attempt span %d: attempt = %v, ends = %d", i, v, span.Ends())
				}
			}
			if errs := attempts[0].Errors(); len(errs) != 1 {
				t.Errorf("first attempt errors = %v, want the network error", errs)
			}
			if errs := attempts[1].Errors(); len(errs) != 0 {
				t.Errorf("second attempt errors = %v, want none", errs)
			}
			retries := int64(0)
			for key, count := range meter.Counters() {
				if strings.HasPrefix(key, telemetry.BackendRetries+"{backend=cloudwatchlogs,") {
					retries += count
				}
			}
			if retries != 1 {
				t.Errorf("retries = %d, want 1; counters = %v", retries, meter.Counters())
			}
		})
	}
}
