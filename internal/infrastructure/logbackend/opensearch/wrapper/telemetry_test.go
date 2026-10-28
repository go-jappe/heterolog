package wrapper

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

// retriedOncerecordingOpenSearchClientは、最初の試行だけ一時的なnetwork errorで失敗します。
func retriedOncerecordingOpenSearchClient() *recordingOpenSearchClient {
	client := &recordingOpenSearchClient{}
	calls := 0
	client.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		calls++
		if calls == 1 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		return &opensearch.IndexResponse{}, nil
	}
	return client
}

func TestRetryWrappersTraceEachAttemptAndCountRetries(t *testing.T) {
	for _, wrapper := range []struct {
		name  string
		build func(next opensearch.Client, tracer *observertest.Tracer, meter *observertest.Meter) opensearch.Client
	}{
		{"retrying", func(next opensearch.Client, tracer *observertest.Tracer, meter *observertest.Meter) opensearch.Client {
			return newRetryingClient(next, fastRetryConfig(2), tracer, meter)
		}},
		{"retry policy", func(next opensearch.Client, tracer *observertest.Tracer, meter *observertest.Meter) opensearch.Client {
			return newRetryPolicyClient(next, fastRetryConfig(2), tracer, meter)
		}},
	} {
		t.Run(wrapper.name, func(t *testing.T) {
			tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
			client := wrapper.build(retriedOncerecordingOpenSearchClient(), tracer, meter)

			if _, err := client.IndexDocument(context.Background(), opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}); err != nil {
				t.Fatalf("call error = %v, want success after one retry", err)
			}

			attempts := tracer.Spans(spanNameIndexDocument)
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
				if strings.HasPrefix(key, telemetry.BackendRetries+"{backend=opensearch,") {
					retries += count
				}
			}
			if retries != 1 {
				t.Errorf("retries = %d, want 1; counters = %v", retries, meter.Counters())
			}
		})
	}
}
