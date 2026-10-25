package cloudwatchlogs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestBuildDeliveryFailureHandlerBuildsCloudWatchLogsFileArchive(t *testing.T) {
	t.Parallel()

	fmCfg := newDeliveryCloudWatchLogsFailureHandlerConfig(t.TempDir())
	handler, err := BuildDeliveryFailureHandler(
		context.Background(),
		clock.NewUTCClock(),
		fmCfg,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("BuildDeliveryFailureHandler() error = %v", err)
	}
	t.Cleanup(func() {
		if err := handler.Drain(context.Background()); err != nil {
			t.Errorf("failure handler Drain() error = %v", err)
		}
	})
	if len(handler.Handlers) != 1 {
		t.Fatalf("handler count = %d, want 1", len(handler.Handlers))
	}

	archivePipeline, ok := handler.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	if !ok {
		t.Fatalf("handler type = %T, want *fsarchivehandler.FailureArchivePipeline", handler.Handlers[0])
	}
	archivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	if err != nil {
		t.Fatalf("ActiveArchivePath(retryable): %v", err)
	}

	report, err := handler.HandleFailureWithReport(context.Background(), loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: deliveryBackendCloudWatchLogs, InputCount: 1},
		RetryableRecords: []loggerfailure.FailedRecord{{
			Payload: map[string]any{"message": "delivery cloudwatch logs archive"},
		}},
	})
	if err != nil {
		t.Fatalf("HandleFailureWithReport() error = %v", err)
	}
	if len(report.Targets) != 0 || len(report.Durables) != 1 {
		t.Fatalf("report = %+v, want one durable result and no target", report)
	}
	if durableResult := report.Durables[0]; durableResult.Backend != deliveryBackendCloudWatchLogs ||
		durableResult.Kind != pkglogger.DurableArchive ||
		durableResult.Status != pkglogger.DurableSucceeded {
		t.Fatalf("durable result = %+v, want succeeded CloudWatch Logs archive", durableResult)
	}

	records := waitForCloudWatchLogsArchiveRecords(
		t,
		archivePath,
		1,
	)
	if got := records[0].Backend; got != deliveryBackendCloudWatchLogs {
		t.Fatalf("archive backend = %q, want %q", got, deliveryBackendCloudWatchLogs)
	}
}

func TestBuildDeliveryFailureHandlerRejectsCloudWatchLogsSpool(t *testing.T) {
	t.Parallel()

	handler, err := BuildDeliveryFailureHandler(
		context.Background(),
		clock.NewUTCClock(),
		NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
			FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
			},
		}, streamrouting.None),
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
	)
	var durableTypeErr *pkgdurable.DurableTypeError
	if !errors.As(err, &durableTypeErr) || *durableTypeErr != (pkgdurable.DurableTypeError{
		Scope: "delivery failure store", Type: pkgdurable.DurableTypeSpool, Err: pkgdurable.ErrUnsupportedDurableType,
	}) {
		t.Fatalf("BuildDeliveryFailureHandler() error = %v, want unsupported spool error", err)
	}
	if len(handler.Handlers) != 0 {
		t.Fatalf("handler count = %d, want 0", len(handler.Handlers))
	}
}

func newDeliveryCloudWatchLogsFailureHandlerConfig(root string) DeliveryCloudWatchLogsFlushManagerConfigProvider {
	return NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
		FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
			DurableType:       []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
			FileArchiveConfig: newDeliveryCloudWatchLogsFileArchiveConfig(root),
		},
		LogGroupName:  "/app/delivery",
		LogStreamName: "delivery-stream",
	}, streamrouting.None)
}

func newDeliveryCloudWatchLogsFileArchiveConfig(root string) pkgdurable.DeliveryFileArchiveConfig {
	return pkgdurable.DeliveryFileArchiveConfig{
		Target:                     "delivery-cloudwatchlogs-test",
		MaxFileSize:                1024 * 1024,
		FailureFileArchivePath:     root,
		Dir:                        "failure-archive",
		DirCaseRetryable:           "retryable",
		DirCasePermanent:           "permanent",
		FileNameFormat:             pkgfsstore.FailureArchiveFileNameFormat,
		FileDateFormat:             pkgfsstore.FileDateFormat,
		EnableRotate:               false,
		MaxFileBackups:             2,
		MaxFileAge:                 1,
		MaxDirSize:                 10 * 1024 * 1024,
		UsageCacheTTL:              time.Hour,
		EnableFileCompress:         false,
		CompressFormat:             pkgfsstore.CompressGzip,
		RotationType:               pkgfsstore.FileRotationDaily,
		RotationDayOfWeek:          time.Sunday,
		MaxWriteAttempts:           1,
		FileOperationRetryWaitTime: time.Millisecond,
		OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
			OverloadPolicy:        pkgdurable.OverloadPolicyDrop,
			MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
		},
	}
}
