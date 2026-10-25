package opensearch

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func mustNewOpenSearchFailureComposite(
	handlers []flushfailure.FailureHandler,
) *flushfailure.CompositeFailureHandler {
	composite, err := flushfailure.NewCompositeFailureHandler(handlers, 1)
	if err != nil {
		panic(err)
	}
	return composite
}

func mustNewOpenSearchReportingFailureComposite(
	handlers []flushfailure.FailureHandlerWithReport,
) *flushfailure.ReportingCompositeFailureHandler {
	composite, err := flushfailure.NewReportingCompositeFailureHandler(handlers, 1)
	if err != nil {
		panic(err)
	}
	return composite
}

func TestOpenSearchFlushManagerFlushSingleEntryWaitsForFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := newBlockingFailureHandler()
		handleErr := errors.New("durable handling failed")
		handler.err = handleErr
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(handler.releaseHandling)
		fm := newFlushManagerForDeliveryTest(handler)

		done := make(chan error, 1)
		go func() {
			done <- fm.Flush(context.Background(), []*logging.LogEntry{newFlushManagerDeliveryTestEntry()})
		}()

		select {
		case <-handler.started:
		case <-time.After(time.Second):
			t.Fatal("failure handler was not called")
		}

		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Flush returned before failure handler completed: %v", err)
		default:
		}

		handler.releaseHandling()
		if err := <-done; !errors.Is(err, handleErr) {
			t.Fatalf("Flush error = %v, want durable handling error", err)
		}
	})
}

func TestOpenSearchFlushManagerBulkFlushReturnsDurableHandlingError(t *testing.T) {
	handleErr := errors.New("bulk durable handling failed")
	fm := newFlushManagerForDeliveryTest(&reportingFailureHandler{err: handleErr})

	err := fm.Flush(context.Background(), []*logging.LogEntry{
		newFlushManagerDeliveryTestEntry(),
		newFlushManagerDeliveryTestEntry(),
	})
	if !errors.Is(err, handleErr) {
		t.Fatalf("Flush() error = %v, want durable handling error", err)
	}
}

func TestOpenSearchFlushManagerCloseWaitsForAcceptedDurableFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := newBlockingFailureHandler()
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(handler.releaseHandling)
		fm := newFlushManagerForDeliveryTest(handler)

		flushDone := make(chan error, 1)
		go func() {
			flushDone <- fm.Flush(
				context.Background(),
				[]*logging.LogEntry{newFlushManagerDeliveryTestEntry()},
			)
		}()

		select {
		case <-handler.started:
		case <-time.After(time.Second):
			t.Fatal("failure handler was not dispatched")
		}

		// Flushはdurable failure処理が確定するまで戻りません。
		synctest.Wait()
		select {
		case err := <-flushDone:
			t.Fatalf("Flush returned before durable failure handling completed: %v", err)
		default:
		}

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- fm.Close(context.Background())
		}()

		// Closeは、すでに受理したdurable failure処理が完了するまで戻りません。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before durable failure handling completed: %v", err)
		default:
		}

		handler.releaseHandling()
		if err := <-flushDone; err != nil {
			t.Fatalf("Flush returned error: %v", err)
		}

		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close returned error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not return after durable failure handling completed")
		}
	})
}

func TestOpenSearchFlushManagerIndexWaitsForFailureHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := newBlockingFailureHandler()
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(handler.releaseHandling)
		fm := newFlushManagerForDeliveryTest(handler)

		done := make(chan error, 1)
		go func() {
			done <- fm.Index(context.Background(), newFlushManagerDeliveryTestEntry())
		}()

		select {
		case <-handler.started:
		case <-time.After(time.Second):
			t.Fatal("failure handler was not called")
		}

		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Index returned before failure handler completed: %v", err)
		default:
		}

		handler.releaseHandling()

		if err := <-done; err != nil {
			t.Fatalf("Index returned error: %v", err)
		}
	})
}

func TestOpenSearchFlushManagerIndexWithReportWaitsAndMergesFailureReport(t *testing.T) {
	durableArchiveErr := errors.New("durable archive failed")
	handler := &reportingFailureHandler{err: durableArchiveErr}
	fm := newReportingFlushManagerForDeliveryTest(handler)

	report, err := fm.IndexWithReport(context.Background(), newFlushManagerDeliveryTestEntry())
	if err == nil {
		t.Fatal("IndexWithReport must return the unconfirmed target error")
	}
	if !errors.Is(err, durableArchiveErr) {
		t.Fatalf("IndexWithReport error = %v, want durable report error", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("IndexWithReport targets = %d, want 1", len(report.Targets))
	}
	if report.Targets[0].Status != pkglogger.DeliveryUnconfirmed {
		t.Fatalf("IndexWithReport status = %s, want %s", report.Targets[0].Status, pkglogger.DeliveryUnconfirmed)
	}
	if len(report.Durables) != 1 {
		t.Fatalf("IndexWithReport durables = %d, want 1", len(report.Durables))
	}
	if report.Durables[0].Status != pkglogger.DurableFailed {
		t.Fatalf("IndexWithReport durable status = %s, want %s", report.Durables[0].Status, pkglogger.DurableFailed)
	}
	if got := atomic.LoadInt32(&handler.reportCalls); got != 1 {
		t.Fatalf("HandleFailureWithReport called %d times, want 1", got)
	}
}

func TestOpenSearchFlushManagerIndexWithoutFailureHandlerReturnsError(t *testing.T) {
	fm := newFlushManagerForDeliveryTest(nil)

	err := fm.Index(context.Background(), newFlushManagerDeliveryTestEntry())
	if !errors.Is(err, flushfailure.ErrNoFailureHandler) {
		t.Fatalf("Index error = %v, want ErrNoFailureHandler", err)
	}
}

// durable処理を完了できない場合は、failure handlerのerrorだけでなくOpenSearchの拒否そのものを
// errors.Asで判別できるように返します。durable処理が完了した場合は従来どおり成功とします。
func TestOpenSearchFlushManagerKeepsBackendCauseWhenDurableHandlingFails(t *testing.T) {
	rejection := &pkgopensearch.HTTPStatusError{Code: http.StatusForbidden, Status: "403 Forbidden"}
	handlerErr := errors.New("durable handling failed")
	for _, mode := range []string{"index", "single flush", "bulk flush"} {
		for _, test := range []struct {
			name           string
			handler        flushfailure.FailureHandler
			wantHandlerErr error
		}{
			{name: "without failure handler", wantHandlerErr: flushfailure.ErrNoFailureHandler},
			{name: "failure handler error", handler: &reportingFailureHandler{err: handlerErr}, wantHandlerErr: handlerErr},
			{name: "durable handling succeeded", handler: &reportingFailureHandler{}},
		} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				fm := newFlushManagerForDeliveryTest(test.handler)
				fm.osClient = &MockFlushManagerClient{indexErr: rejection, bulkErr: rejection}

				var err error
				switch mode {
				case "index":
					err = fm.Index(context.Background(), newFlushManagerDeliveryTestEntry())
				case "single flush":
					err = fm.Flush(context.Background(), []*logging.LogEntry{newFlushManagerDeliveryTestEntry()})
				case "bulk flush":
					err = fm.Flush(context.Background(), []*logging.LogEntry{
						newFlushManagerDeliveryTestEntry(),
						newFlushManagerDeliveryTestEntry(),
					})
				}

				if test.wantHandlerErr == nil {
					if err != nil {
						t.Fatalf("error after completed durable handling = %v, want nil", err)
					}
					return
				}
				if !errors.Is(err, test.wantHandlerErr) {
					t.Fatalf("error = %v, want durable handling error %v", err, test.wantHandlerErr)
				}
				var statusErr *pkgopensearch.HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
					t.Fatalf("error = %v, want the OpenSearch HTTP 403 rejection", err)
				}
			})
		}
	}
}

// 一部のbulk requestが受理された場合はTerminalErrorを設定しませんが、退避処理を完了できなかった
// ときも、拒否されたrequestの元のerrorを返してerrors.Asで判別できることを確認します。
func TestOpenSearchFlushManagerKeepsBackendCauseForPartiallyAcceptedBulk(t *testing.T) {
	rejection := &pkgopensearch.HTTPStatusError{Code: http.StatusForbidden, Status: "403 Forbidden"}
	client := &MockFlushManagerClient{}
	client.bulkHandler = func(_ context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		if client.IndexDocumentsCallCount == 2 {
			return nil, rejection
		}
		return successBulkResponse(reqs), nil
	}
	handlerErr := errors.New("durable handling failed")
	handler := &resultRecordingFailureHandler{err: handlerErr}
	cfg := NewMockOpenSearchConfigProvider()
	fm := newFlushManagerForDeliveryTest(handler)
	fm.osClient = client
	entries := []*logging.LogEntry{
		newFlushManagerDeliveryTestEntry(),
		newFlushManagerDeliveryTestEntry(),
	}
	// 各documentは単独で上限内に収まり、二件をまとめると上限を超えるようにします。
	total, largest := 0, 0
	for _, entry := range entries {
		size, err := pkgopensearch.BulkRequestSize([]pkgopensearch.DocumentWriteRequest{
			fm.convertDocumentToCreateRequest(fm.LogEntryToDocumentConverter().convertLogEntryToDocument(entry)),
		})
		if err != nil {
			t.Fatalf("measure bulk record: %v", err)
		}
		total += size
		largest = max(largest, size)
	}
	if largest > total-1 {
		t.Fatalf("test precondition: a single document (%d bytes) must fit in the limit %d", largest, total-1)
	}
	cfg.maxBulkRequestBytes = total - 1
	fm.LoggerBase.config = cfg

	err := fm.Flush(context.Background(), entries)

	if client.IndexDocumentsCallCount != 2 {
		t.Fatalf("bulk calls = %d, want one request per document", client.IndexDocumentsCallCount)
	}
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Flush() error = %v, want durable handling error %v", err, handlerErr)
	}
	var statusErr *pkgopensearch.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
		t.Fatalf("Flush() error = %v, want the OpenSearch HTTP 403 rejection", err)
	}
	results := handler.snapshot()
	if len(results) != 1 {
		t.Fatalf("failure handler calls = %d, want 1", len(results))
	}
	// 受理済みの一件目は退避対象に含めず、部分的な受理結果も消しません。
	if res := results[0]; res.TerminalError != nil || res.FailedCount() != 1 || res.SuccessCount() != 1 {
		t.Fatalf("SendResult terminal=%v failed=%d success=%d, want only the rejected document failed",
			res.TerminalError, res.FailedCount(), res.SuccessCount())
	}
}

// Bulk応答のitemで一部のdocumentだけが拒否され、退避処理を完了できなかった場合も、
// itemの拒否理由を返してerrors.Asで判別できることを確認します。
func TestOpenSearchFlushManagerKeepsBulkItemRejectionForPartiallyAcceptedBulk(t *testing.T) {
	client := &MockFlushManagerClient{}
	client.bulkHandler = func(_ context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		if len(reqs) != 2 {
			t.Errorf("bulk documents = %d, want both documents in one request", len(reqs))
			return successBulkResponse(reqs), nil
		}
		return &pkgopensearch.BulkResponse{
			Errors: true,
			Items: []map[string]pkgopensearch.BulkItemResult{
				{"create": {Index: reqs[0].Index, ID: reqs[0].ID, Status: http.StatusCreated, Result: "created"}},
				{"create": {
					Index:  reqs[1].Index,
					ID:     reqs[1].ID,
					Status: http.StatusBadRequest,
					Error: &pkgopensearch.ErrorDetail{
						Type:   pkgopensearch.ErrorTypeMapperParsing,
						Reason: "record mapping failed",
					},
				}},
			},
		}, nil
	}
	handlerErr := errors.New("durable handling failed")
	handler := &resultRecordingFailureHandler{err: handlerErr}
	fm := newFlushManagerForDeliveryTest(handler)
	fm.osClient = client

	err := fm.Flush(context.Background(), []*logging.LogEntry{
		newFlushManagerDeliveryTestEntry(),
		newFlushManagerDeliveryTestEntry(),
	})

	if !errors.Is(err, handlerErr) {
		t.Fatalf("Flush() error = %v, want durable handling error %v", err, handlerErr)
	}
	var itemErr *bulkItemRejectionError
	if !errors.As(err, &itemErr) || itemErr.Status != http.StatusBadRequest || itemErr.Type != pkgopensearch.ErrorTypeMapperParsing {
		t.Fatalf("Flush() error = %v, want the rejected bulk item", err)
	}
	results := handler.snapshot()
	if len(results) != 1 {
		t.Fatalf("failure handler calls = %d, want 1", len(results))
	}
	// 受理済みの一件目は退避対象に含めず、部分的な受理結果も消しません。
	if res := results[0]; res.TerminalError != nil || len(res.QuarantineRecords) != 1 || res.SuccessCount() != 1 {
		t.Fatalf("SendResult terminal=%v quarantine=%d success=%d, want only the rejected document quarantined",
			res.TerminalError, len(res.QuarantineRecords), res.SuccessCount())
	}
	target := results[0].TargetDeliveryResult("opensearch")
	var targetItemErr *bulkItemRejectionError
	if target.Status != pkglogger.DeliveryFailed || !errors.As(target.Err, &targetItemErr) {
		t.Fatalf("target delivery result = %+v, want the rejected bulk item as a failed delivery", target)
	}
}

// resultRecordingFailureHandlerは受け取ったSendResultを記録し、固定のerrorを返します。
type resultRecordingFailureHandler struct {
	mu      sync.Mutex
	err     error
	results []loggerfailure.SendResult
}

func (h *resultRecordingFailureHandler) HandleFailure(_ context.Context, result loggerfailure.SendResult) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.results = append(h.results, result)
	return h.err
}

func (h *resultRecordingFailureHandler) snapshot() []loggerfailure.SendResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]loggerfailure.SendResult(nil), h.results...)
}

func TestOpenSearchFlushManagerFlushWithoutFailureHandlerReturnsError(t *testing.T) {
	fm := newFlushManagerForDeliveryTest(nil)

	if err := fm.Flush(context.Background(), []*logging.LogEntry{newFlushManagerDeliveryTestEntry()}); !errors.Is(err, flushfailure.ErrNoFailureHandler) {
		t.Fatalf("Flush error = %v, want ErrNoFailureHandler", err)
	}
}

func TestOpenSearchFlushManagerIndexWithReportWithoutFailureHandlerReturnsTargetFailure(t *testing.T) {
	fm := newFlushManagerForDeliveryTest(nil)

	report, err := fm.IndexWithReport(context.Background(), newFlushManagerDeliveryTestEntry())
	if err == nil {
		t.Fatal("IndexWithReport must return the unconfirmed target error")
	}
	if !errors.Is(err, flushfailure.ErrNoFailureHandler) {
		t.Fatalf("IndexWithReport error = %v, want ErrNoFailureHandler", err)
	}
	if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryUnconfirmed {
		t.Fatalf("IndexWithReport targets = %+v, want one unconfirmed target", report.Targets)
	}
}

func newFlushManagerForDeliveryTest(handler flushfailure.FailureHandler) *OpenSearchFlushManager {
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	res := flushManagerDeliveryTestResource{}
	cfg := NewMockOpenSearchConfigProvider()
	var composite *flushfailure.CompositeFailureHandler
	if handler != nil {
		composite = mustNewOpenSearchFailureComposite([]flushfailure.FailureHandler{handler})
	}

	manager, err := NewOpenSearchFlushManager(
		&LoggerBase{
			Clock:              clock.NewUTCClock(),
			TimestampFormatter: timestamp.RFC3339Formatter(),
			config:             cfg,
		},
		&nilIndexResponseClient{}, cfg, NewPlainConverter(timestamp.RFC3339Formatter()),
		composite, res, tracer, meter,
	)
	if err != nil {
		panic(err)
	}
	return manager
}

func newReportingFlushManagerForDeliveryTest(
	handler flushfailure.FailureHandlerWithReport,
) *OpenSearchFlushManager {
	fm := newFlushManagerForDeliveryTest(nil)
	if handler == nil {
		return fm
	}

	reporting := mustNewOpenSearchReportingFailureComposite(
		[]flushfailure.FailureHandlerWithReport{handler},
	)
	fm.FailureHandler = reporting.CompositeFailureHandler
	fm.FailureReportHandler = reporting
	return fm
}

func newFlushManagerDeliveryTestEntry() *logging.LogEntry {
	return &logging.LogEntry{
		OccurredAt: time.Now().UTC(),
		Level:      pkglogger.LogLevelInfo,
		Message:    "delivery test",
	}
}

type nilIndexResponseClient struct{}

func (c *nilIndexResponseClient) IndexDocument(
	ctx context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	return nil, nil
}

func (c *nilIndexResponseClient) IndexDocuments(
	ctx context.Context,
	docs []pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.BulkResponse, error) {
	return nil, errors.New("bulk should not be used in this test")
}

type blockingFailureHandler struct {
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	err         error
}

// releaseHandlingは、待機中のHandleFailureを一度だけ解放します。
func (h *blockingFailureHandler) releaseHandling() {
	h.releaseOnce.Do(func() { close(h.release) })
}

func newBlockingFailureHandler() *blockingFailureHandler {
	return &blockingFailureHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (h *blockingFailureHandler) HandleFailure(ctx context.Context, r loggerfailure.SendResult) error {
	h.once.Do(func() {
		close(h.started)
	})

	select {
	case <-h.release:
		return h.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type reportingFailureHandler struct {
	err         error
	reportCalls int32
}

func (h *reportingFailureHandler) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	return h.err
}

func (h *reportingFailureHandler) HandleFailureWithReport(
	context.Context,
	loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	atomic.AddInt32(&h.reportCalls, 1)
	return pkglogger.DeliveryReport{
		Durables: []pkglogger.DurableResult{{
			Backend: "opensearch_failure_archive",
			Kind:    pkglogger.DurableArchive,
			Status:  pkglogger.DurableFailed,
			Err:     h.err,
		}},
	}, nil
}

type flushManagerDeliveryTestResource struct{}

var _ resource.ResourceProvider = flushManagerDeliveryTestResource{}

func (flushManagerDeliveryTestResource) ServiceName() string    { return "test-service" }
func (flushManagerDeliveryTestResource) ServiceVersion() string { return "test-version" }
func (flushManagerDeliveryTestResource) HostName() string       { return "test-host" }
func (flushManagerDeliveryTestResource) OSType() string         { return "linux" }
func (flushManagerDeliveryTestResource) Environment() string    { return "test" }
func (flushManagerDeliveryTestResource) RuntimeType() string    { return "local" }
func (flushManagerDeliveryTestResource) CloudProvider() string  { return "" }
func (flushManagerDeliveryTestResource) CloudProviderRegion() string {
	return ""
}
func (flushManagerDeliveryTestResource) RuntimeInstanceID() string { return "test-instance" }
