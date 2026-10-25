package fsarchivehandler

import (
	"context"
	"os"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type failureHandlerContract interface {
	HandleFailure(context.Context, loggerfailure.SendResult) error
}

type failureReportHandlerContract interface {
	HandleFailureWithReport(
		context.Context,
		loggerfailure.SendResult,
	) (pkglogger.DeliveryReport, error)
}

type failureHandlerShutdownContract interface {
	Shutdown(context.Context) error
}

var (
	_ failureHandlerContract         = (*FailureArchivePipeline)(nil)
	_ failureReportHandlerContract   = (*FailureArchivePipeline)(nil)
	_ failureHandlerShutdownContract = (*FailureArchivePipeline)(nil)
)

// Pipelineだけが外部からのfailure受付と終了順序を所有し、書き込み担当のHandlerを
// Compositeへ直接配線できないことを固定します。
func TestFsArchiveHandlerDoesNotImplementPipelineContracts(t *testing.T) {
	if _, ok := any(&FsArchiveHandler{}).(failureHandlerContract); ok {
		t.Fatal("*FsArchiveHandler implements HandleFailure, want Pipeline-only acceptance")
	}
	if _, ok := any(&FsArchiveHandler{}).(failureReportHandlerContract); ok {
		t.Fatal("*FsArchiveHandler implements HandleFailureWithReport, want Pipeline-only acceptance")
	}
	if _, ok := any(&FsArchiveHandler{}).(failureHandlerShutdownContract); ok {
		t.Fatal("*FsArchiveHandler implements Shutdown(context.Context) error, want Pipeline-only ownership")
	}
}

func TestFailureArchivePipelineActiveArchivePathReturnsSelectedPath(t *testing.T) {
	retryableFile, err := os.CreateTemp(t.TempDir(), "retryable-*.jsonl")
	if err != nil {
		t.Fatalf("create retryable file: %v", err)
	}
	t.Cleanup(func() { _ = retryableFile.Close() })
	permanentFile, err := os.CreateTemp(t.TempDir(), "permanent-*.jsonl")
	if err != nil {
		t.Fatalf("create permanent file: %v", err)
	}
	t.Cleanup(func() { _ = permanentFile.Close() })

	pipeline := &FailureArchivePipeline{
		handler: &FsArchiveHandler{
			Fsstore: &failurearchive.FileStore{
				ActiveFilePathRetryable: retryableFile.Name(),
				ActiveFilePathPermanent: permanentFile.Name(),
				FileRetryable:           retryableFile,
				FilePermanent:           permanentFile,
			},
		},
	}

	retryablePath, err := pipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	if err != nil {
		t.Fatalf("ActiveArchivePath(retryable) error = %v", err)
	}
	if retryablePath != retryableFile.Name() {
		t.Fatalf("ActiveArchivePath(retryable) = %q", retryablePath)
	}

	permanentPath, err := pipeline.ActiveArchivePath(failurearchive.FailureCategoryPermanent)
	if err != nil {
		t.Fatalf("ActiveArchivePath(permanent) error = %v", err)
	}
	if permanentPath != permanentFile.Name() {
		t.Fatalf("ActiveArchivePath(permanent) = %q", permanentPath)
	}

	if _, err := pipeline.ActiveArchivePath(failurearchive.FailureCategory("unknown")); err == nil {
		t.Fatal("ActiveArchivePath(unknown) error = nil")
	}
}
