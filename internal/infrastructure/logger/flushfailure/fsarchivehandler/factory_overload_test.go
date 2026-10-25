package fsarchivehandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// degrade policyでは、空き容量が閾値を下回ったrecordをarchiveへ保存せず、設定したstdoutへ退避します。
// 同期APIはnilを返し、DeliveryReportは退避の成功（Spilled）を返します。
func TestFailureArchiveDegradePolicySpillsOverloadedRecordsToStdout(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	var spilled bytes.Buffer
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable
		cfg.overload.overloadPolicy = pkgdurable.OverloadPolicyDegrade
		cfg.overload.spillDestination = pkgdurable.SpillDestinationStdout
		cfg.spillStdout = &spilled
	})
	minimum := pipeline.handler.Config.OverloadPolicyConfig().MinAvailableDiskSpace()
	pipeline.handler.availableDiskSpace = func(string) (uint64, error) { return minimum - 1, nil }
	activePath, err := pipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	if err != nil {
		t.Fatalf("ActiveArchivePath() error = %v", err)
	}

	result := retryableArchiveResult(now, "spilled-by-handle-failure")
	result.Meta.Backend = "opensearch"
	result.Meta.RequestID = "request-handle-failure"
	if err := pipeline.HandleFailure(context.Background(), result); err != nil {
		t.Fatalf("HandleFailure() error = %v, want nil after spilling to stdout", err)
	}
	requireArchiveSpilledRecord(t, &spilled, "opensearch", "request-handle-failure", "spilled-by-handle-failure")

	spilled.Reset()
	result = retryableArchiveResult(now, "spilled-by-report")
	result.Meta.Backend = "loki"
	result.Meta.RequestID = "request-report"
	report, err := pipeline.HandleFailureWithReport(context.Background(), result)
	if err != nil {
		t.Fatalf("HandleFailureWithReport() error = %v, want nil after spilling to stdout", err)
	}
	wantEvacuations := []pkglogger.EvacuationResult{{Kind: pkglogger.EvacuationSpilled, Status: pkglogger.EvacuationSucceeded}}
	if !reflect.DeepEqual(report.Evacuations, wantEvacuations) || len(report.Durables) != 0 {
		t.Fatalf("report = %+v, want only %+v", report, wantEvacuations)
	}
	requireArchiveSpilledRecord(t, &spilled, "loki", "request-report", "spilled-by-report")

	// 退避したrecordはarchiveへ保存しません。
	if records := readArchiveRecords(t, activePath); len(records) != 0 {
		t.Fatalf("archive records = %+v, want none while overloaded", records)
	}
}

// requireArchiveSpilledRecordは、一回の退避がmeta行と一件のretryable recordだけを出力したことを確認します。
func requireArchiveSpilledRecord(t *testing.T, spilled *bytes.Buffer, backend, requestID, id string) {
	t.Helper()
	var rows []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(spilled.Bytes()))
	for decoder.More() {
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			t.Fatalf("decode spilled row: %v; output=%q", err, spilled.String())
		}
		rows = append(rows, row)
	}
	if len(rows) != 2 {
		t.Fatalf("spilled rows = %v, want meta and one record", rows)
	}
	meta, record := rows[0], rows[1]
	if meta["type"] != "meta" || meta["backend"] != backend || meta["requestID"] != requestID ||
		meta["retryableRecordCount"] != float64(1) || meta["failedCount"] != float64(1) {
		t.Fatalf("spilled meta = %v, want backend=%q requestID=%q with one retryable record", meta, backend, requestID)
	}
	payload, _ := record["payload"].(map[string]any)
	if record["type"] != "record" || record["kind"] != "retryable" || record["backend"] != backend ||
		record["requestID"] != requestID || payload["id"] != id {
		t.Fatalf("spilled record = %v, want retryable record %q from %q", record, id, backend)
	}
}

func TestNewFailureArchivePipelineWiresMeterToHandler(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	meter := &archiveFactoryTestMeter{}

	handler, err := NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
		cfg,
		observer.NopTracer{},
		meter,
		InternalLogHandlers{},
	)
	if err != nil {
		t.Fatalf("NewFailureArchivePipeline() error = %v", err)
	}
	t.Cleanup(func() {
		if err := shutdownFailureArchiveForTest(handler); err != nil {
			t.Errorf("FailureArchivePipeline.Shutdown() error = %v", err)
		}
	})

	if handler.handler.Meter != meter {
		t.Fatalf("handler.Meter = %#v, want injected meter", handler.handler.Meter)
	}
}

func TestNewFailureArchivePipelineValidatesEntireArchiveConfigBeforeOpeningFiles(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	cfg.overload.minAvailableDiskSpace = pkgdurable.MinFileArchiveAvailableDiskSpace - 1

	handler, err := NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		InternalLogHandlers{},
	)
	if !errors.Is(err, ErrFileArchiveMinAvailableDiskSpaceTooSmall) {
		t.Fatalf("error = %v, want minimum available disk validation error", err)
	}
	if handler != nil {
		t.Fatalf("handler = %#v, want nil", handler)
	}
}

type archiveFactoryTestMeter struct{}

func (*archiveFactoryTestMeter) Counter(string) observer.Counter {
	return observer.NopCounter{}
}

func (*archiveFactoryTestMeter) Gauge(string) observer.Gauge {
	return observer.NopGauge{}
}

func TestNewFailureArchivePipelineRejectsNilConfig(t *testing.T) {
	handler, err := NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		InternalLogHandlers{},
	)
	if !errors.Is(err, ErrNilFileArchiveConfig) {
		t.Fatalf("error = %v, want nil config error", err)
	}
	if handler != nil {
		t.Fatalf("handler = %#v, want nil", handler)
	}
}

func TestNewFailureArchivePipelineRejectsUnsupportedSpillDestinations(t *testing.T) {
	for _, destination := range []pkgdurable.SpillDestination{
		pkgdurable.SpillDestinationS3,
		pkgdurable.SpillDestinationNFS,
		pkgdurable.SpillDestination("unknown"),
	} {
		t.Run(string(destination), func(t *testing.T) {
			cfg := newValidArchiveHandlerConfig(t)
			cfg.overload.overloadPolicy = pkgdurable.OverloadPolicyDegrade
			cfg.overload.spillDestination = destination

			handler, err := NewFailureArchivePipeline(
				context.Background(),
				archiveFixedClock{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
				cfg,
				observer.NopTracer{},
				observer.NopMeter{},
				InternalLogHandlers{},
			)
			if !errors.Is(err, evacuation.ErrUnsupportedSpillDestination) {
				t.Fatalf("error = %v, want ErrUnsupportedSpillDestination", err)
			}
			if handler != nil {
				t.Fatalf("handler = %#v, want nil", handler)
			}
		})
	}
}

func TestNewFailureArchivePipelineRejectsUnsupportedOverloadPolicy(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	cfg.overload.overloadPolicy = pkgdurable.OverloadPolicy("future-policy")

	handler, err := NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		InternalLogHandlers{},
	)
	if !errors.Is(err, ErrUnsupportedFileArchiveOverloadPolicy) {
		t.Fatalf("error = %v, want ErrUnsupportedFileArchiveOverloadPolicy", err)
	}
	if handler != nil {
		t.Fatalf("handler = %#v, want nil", handler)
	}
}
