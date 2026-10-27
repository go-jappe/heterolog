package opensearch

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/stretchr/testify/require"
)

type cancellationClient struct {
	single func(context.Context, pkgopensearch.DocumentWriteRequest) (*pkgopensearch.IndexResponse, error)
	bulk   func(context.Context, []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error)
}

func (c cancellationClient) IndexDocument(ctx context.Context, req pkgopensearch.DocumentWriteRequest) (*pkgopensearch.IndexResponse, error) {
	return c.single(ctx, req)
}
func (c cancellationClient) IndexDocuments(ctx context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
	return c.bulk(ctx, reqs)
}

type cancellationContextKey struct{}
type cancellationObservation struct {
	result      loggerfailure.SendResult
	contextErr  error
	hasDeadline bool
	value       any
}
type cancellationFailureHandler struct {
	observations chan cancellationObservation
	err          error
}

func (h *cancellationFailureHandler) HandleFailure(ctx context.Context, result loggerfailure.SendResult) error {
	_, deadline := ctx.Deadline()
	h.observations <- cancellationObservation{result: result, contextErr: ctx.Err(), hasDeadline: deadline, value: ctx.Value(cancellationContextKey{})}
	return h.err
}
func (h *cancellationFailureHandler) HandleFailureWithReport(ctx context.Context, result loggerfailure.SendResult) (pkglogger.DeliveryReport, error) {
	err := h.HandleFailure(ctx, result)
	status := pkglogger.DurableSucceeded
	if err != nil {
		status = pkglogger.DurableFailed
	}
	return pkglogger.DeliveryReport{Durables: []pkglogger.DurableResult{{Backend: "test-store", Kind: pkglogger.DurableArchive, Status: status, Err: err}}}, err
}

func TestOpenSearchCanceledSendRunsIndependentDurableHandling(t *testing.T) {
	for _, mode := range []string{"index", "report", "single flush", "bulk flush"} {
		for _, failure := range []bool{false, true} {
			name := mode + "/saved"
			if failure {
				name = mode + "/storage error"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.WithValue(context.Background(), cancellationContextKey{}, "correlation"))
				defer cancel()
				var sent []pkgopensearch.DocumentWriteRequest
				client := cancellationClient{
					single: func(ctx context.Context, req pkgopensearch.DocumentWriteRequest) (*pkgopensearch.IndexResponse, error) {
						sent = append(sent, req)
						cancel()
						return nil, ctx.Err()
					},
					bulk: func(ctx context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
						sent = append(sent, reqs...)
						cancel()
						return nil, ctx.Err()
					},
				}
				handler := &cancellationFailureHandler{observations: make(chan cancellationObservation, 1)}
				if failure {
					handler.err = errors.New("storage unavailable")
				}
				fm := newReportingFlushManagerForDeliveryTest(handler)
				fm.osClient = client
				entry := newFlushManagerDeliveryTestEntry()
				var err error
				var report pkglogger.DeliveryReport
				switch mode {
				case "index":
					err = fm.Index(ctx, entry)
				case "report":
					report, err = fm.IndexWithReport(ctx, entry)
				case "single flush":
					err = fm.Flush(ctx, []*logging.LogEntry{entry})
				case "bulk flush":
					err = fm.Flush(ctx, []*logging.LogEntry{entry, entry})
				}
				require.ErrorIs(t, err, context.Canceled)
				if failure {
					require.ErrorIs(t, err, handler.err)
				}
				require.NoError(t, fm.Close(context.Background()))
				var observed cancellationObservation
				select {
				case observed = <-handler.observations:
				default:
					t.Fatal("accepted snapshot never reached durable handling")
				}
				require.NoError(t, observed.contextErr)
				require.True(t, observed.hasDeadline, "durable storage must have its own bounded deadline")
				require.Equal(t, "correlation", observed.value)
				require.NotEmpty(t, sent)
				require.Len(t, observed.result.QuarantineRecords, len(sent))
				require.Empty(t, observed.result.RetryableRecords)
				require.Empty(t, observed.result.DeadRecords)
				for i, record := range observed.result.QuarantineRecords {
					require.Equal(t, pkglogger.DeliveryUnconfirmed, record.DeliveryStatus)
					payload := record.Payload
					require.Equal(t, sent[i].ID, payload["id"])
					require.Equal(t, sent[i].Doc, payload["doc"])
				}
				if mode == "report" {
					require.Len(t, report.Targets, 1)
					require.Equal(t, pkglogger.DeliveryUnconfirmed, report.Targets[0].Status)
					require.Len(t, report.Durables, 1)
					want := pkglogger.DurableSucceeded
					if failure {
						want = pkglogger.DurableFailed
					}
					require.Equal(t, want, report.Durables[0].Status)
				}
			})
		}
	}
}

func TestOpenSearchCancellationPreservesKnownSingleOutcomes(t *testing.T) {
	for _, outcome := range []string{"success", "create conflict", "permanent rejection", "temporary rejection", "client-only timeout", "not started"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			client := cancellationClient{single: func(_ context.Context, req pkgopensearch.DocumentWriteRequest) (*pkgopensearch.IndexResponse, error) {
				calls++
				if outcome != "client-only timeout" {
					cancel()
				}
				switch outcome {
				case "success":
					return &pkgopensearch.IndexResponse{Index: req.Index, ID: req.ID, Result: "created", Version: 1, Status: 201}, nil
				case "create conflict":
					return nil, &pkgopensearch.HTTPStatusError{Code: 409}
				case "permanent rejection":
					return nil, &pkgopensearch.HTTPStatusError{Code: 403}
				case "temporary rejection":
					return nil, &pkgopensearch.HTTPStatusError{Code: 503}
				default:
					return nil, context.DeadlineExceeded
				}
			}}
			if outcome == "not started" {
				cancel()
			}
			fm := newTestFlushManager(client, NewMockOpenSearchFlushManagerConfig(), observer.NopTracer{}, observer.NopMeter{})
			doc := map[string]any{"message": "retained"}
			result, err := fm.sendDocumentToOpenSearch(ctx, doc)
			switch outcome {
			case "success", "create conflict":
				require.NoError(t, err)
				require.False(t, result.HasSendFailure())
				require.Equal(t, 1, result.SuccessCount())
			case "permanent rejection":
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, result.DeadRecords, 1)
				require.Equal(t, pkglogger.DeliveryFailed, result.DeadRecords[0].DeliveryStatus)
			case "temporary rejection":
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, result.RetryableRecords, 1)
				require.Equal(t, pkglogger.DeliveryFailed, result.RetryableRecords[0].DeliveryStatus)
			case "client-only timeout":
				require.NoError(t, err)
				require.Len(t, result.RetryableRecords, 1)
				require.Equal(t, pkglogger.DeliveryUnconfirmed, result.RetryableRecords[0].DeliveryStatus)
			case "not started":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, calls)
				require.Len(t, result.RetryableRecords, 1)
				require.Equal(t, pkglogger.DeliveryRejected, result.RetryableRecords[0].DeliveryStatus)
				require.Equal(t, doc, result.RetryableRecords[0].Payload["doc"])
			}
		})
	}
}

func TestOpenSearchCancellationPreservesPlannedRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	var sent []pkgopensearch.DocumentWriteRequest
	client.bulkHandler = func(_ context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		require.Len(t, reqs, 1)
		sent = append(sent, reqs...)
		if len(sent) == 1 {
			return successBulkResponse(reqs), nil
		}
		cancel()
		return nil, context.Canceled
	}
	cfg := NewMockOpenSearchFlushManagerConfig()
	cfg.maxBulkRequestBytes = 300 // 150 byteの各messageは単独なら収まりますが、どの二件の組も収まりません。
	fm := newTestFlushManager(client, cfg, observer.NopTracer{}, observer.NopMeter{})
	docs := []map[string]any{{"message": "first" + strings.Repeat("x", 150)}, {"message": "second" + strings.Repeat("x", 150)}, {"message": "third" + strings.Repeat("x", 150)}}
	result, err := fm.sendDocumentsToOpenSearch(ctx, docs)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, sent, 2)
	require.Equal(t, 1, result.SuccessCount())
	require.Len(t, result.QuarantineRecords, 1)
	require.Len(t, result.RetryableRecords, 1)
	require.Empty(t, result.DeadRecords)
	unknown := result.QuarantineRecords[0]
	require.Equal(t, sent[1].ID, unknown.Payload["id"])
	require.Equal(t, docs[1], unknown.Payload["doc"])
	require.Equal(t, pkglogger.DeliveryUnconfirmed, unknown.DeliveryStatus)
	unstarted := result.RetryableRecords[0]
	require.Equal(t, docs[2], unstarted.Payload["doc"])
	require.NotEmpty(t, unstarted.Payload["id"])
	require.Equal(t, pkglogger.DeliveryRejected, unstarted.DeliveryStatus)
}

func TestOpenSearchCanceledBulkRetryRetainsKnownItemResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	client.bulkHandler = func(_ context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		response := successBulkResponse(reqs)
		for i, code := range []int{201, 403, 503} {
			if i == 0 {
				continue
			}
			item := response.Items[i]["create"]
			item.Status = code
			item.Result = ""
			item.Error = &pkgopensearch.ErrorDetail{Type: "test_rejection", Reason: http.StatusText(code)}
			response.Items[i]["create"] = item
		}
		response.Errors = true
		cancel()
		return response, nil
	}
	cfg := NewMockOpenSearchFlushManagerConfig()
	cfg.retryWaitTime = time.Hour
	fm := newTestFlushManager(client, cfg, observer.NopTracer{}, observer.NopMeter{})
	docs := []map[string]any{{"message": "accepted"}, {"message": "forbidden"}, {"message": "unavailable"}}
	result, err := fm.sendDocumentsToOpenSearch(ctx, docs)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, client.IndexDocumentsCallCount)
	require.Equal(t, 1, result.SuccessCount())
	require.Len(t, result.DeadRecords, 1)
	require.Len(t, result.RetryableRecords, 1)
	require.Empty(t, result.QuarantineRecords)
	for i, record := range []loggerfailure.FailedRecord{result.DeadRecords[0], result.RetryableRecords[0]} {
		require.Equal(t, []int{403, 503}[i], record.StatusCode)
		require.Equal(t, client.IndexDocumentsArgs[i+1].ID, record.Payload["id"])
		require.Equal(t, docs[i+1], record.Payload["doc"])
		require.Equal(t, pkglogger.DeliveryFailed, record.DeliveryStatus)
	}
}

func TestOpenSearchCanceledSplitRetainsUnstartedHalf(t *testing.T) {
	for _, timing := range []string{"after rejection", "during left split"} {
		t.Run(timing, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
			client.bulkHandler = func(_ context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
				if len(reqs) == 2 {
					if timing == "after rejection" {
						cancel()
					}
					return nil, &pkgopensearch.HTTPStatusError{Code: 413}
				}
				cancel()
				return nil, context.Canceled
			}
			fm := newTestFlushManager(client, NewMockOpenSearchFlushManagerConfig(), observer.NopTracer{}, observer.NopMeter{})
			result, err := fm.sendDocumentsToOpenSearch(ctx, []map[string]any{{"message": "left"}, {"message": "right"}})
			require.ErrorIs(t, err, context.Canceled)
			if timing == "after rejection" {
				require.Equal(t, 1, client.IndexDocumentsCallCount)
				require.Empty(t, result.QuarantineRecords)
				require.Len(t, result.RetryableRecords, 2)
			} else {
				require.Equal(t, 2, client.IndexDocumentsCallCount)
				require.Len(t, result.QuarantineRecords, 1)
				require.Len(t, result.RetryableRecords, 1)
				require.Equal(t, pkglogger.DeliveryUnconfirmed, result.QuarantineRecords[0].DeliveryStatus)
			}
			for _, record := range result.RetryableRecords {
				require.Equal(t, 413, record.StatusCode)
				require.Equal(t, pkglogger.DeliveryFailed, record.DeliveryStatus, "the original 413 is a confirmed rejection even when the split has not started")
			}
			records := append(result.QuarantineRecords, result.RetryableRecords...)
			for i, record := range records {
				require.Equal(t, client.IndexDocumentsCalls[0][i].ID, record.Payload["id"])
				require.Equal(t, client.IndexDocumentsCalls[0][i].Doc, record.Payload["doc"])
			}
		})
	}
}

var _ flushfailure.FailureHandlerWithReport = (*cancellationFailureHandler)(nil)
