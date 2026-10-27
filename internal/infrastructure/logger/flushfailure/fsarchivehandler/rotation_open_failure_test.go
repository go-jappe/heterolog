package fsarchivehandler

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFailureArchiveRotationOpenFailureReportsAndWritesCurrentActiveFile(t *testing.T) {
	tests := []struct {
		name            string
		failureCategory failurearchive.FailureCategory
		withReport      bool
	}{
		{
			name:            "retryable HandleFailure",
			failureCategory: failurearchive.FailureCategoryRetryable,
		},
		{
			name:            "permanent HandleFailureWithReport",
			failureCategory: failurearchive.FailureCategoryPermanent,
			withReport:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
				cfg.enableRotate = true
				cfg.maxFileSize = infrafsstore.MinAllowedFileSizeBytes
			})

			oldPath, oldFile, originalFileName := failureArchiveActiveState(handler, tt.failureCategory)
			// JSONLとして有効な内容でsize rotationの閾値まで成長させる。
			filler := bytes.Repeat(
				[]byte("{}\n"),
				int(infrafsstore.MinAllowedFileSizeBytes/3)+1,
			)
			if _, err := oldFile.Write(filler); err != nil {
				t.Fatalf("grow active failure archive: %v", err)
			}

			candidateFileName := originalFileName
			candidateFileName.Index++
			date, err := logpattern.ResolveDateStringForFileNameAt(
				handler.handler.Config,
				handler.handler.Clock.Now(),
			)
			if err != nil {
				t.Fatalf("resolve rotation date: %v", err)
			}
			candidatePath := logpattern.BuildFileFullPath(date, candidateFileName, handler.handler.Config)
			// rotation先と同名のdirectoryを置き、os.OpenFileを確実に失敗させる。
			if err := os.Mkdir(candidatePath, 0o750); err != nil {
				t.Fatalf("create rotation blocker: %v", err)
			}

			firstMessage := "record-written-after-deferred-rotation-" + string(tt.failureCategory)
			var delegatedErrors []error
			var notificationReadErr error
			notificationObservedOldWrite := false
			handler.handler.OnError = func(
				_ context.Context,
				err error,
				_ *logging.LogEntry,
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				delegatedErrors = append(delegatedErrors, err)
				oldContent, readErr := os.ReadFile(oldPath)
				if readErr != nil {
					notificationReadErr = readErr
					return
				}
				notificationObservedOldWrite = bytes.Contains(oldContent, []byte(firstMessage))
			}

			firstResult := failureArchiveResultForFailureCategory(tt.failureCategory, firstMessage)
			if tt.withReport {
				report, err := handler.HandleFailureWithReport(context.Background(), firstResult)
				if err != nil {
					t.Fatalf("HandleFailureWithReport after deferred rotation: %v", err)
				}
				if len(report.Durables) != 1 || report.Durables[0].Status != pkglogger.DurableSucceeded {
					t.Fatalf("durable report = %+v, want one succeeded archive result", report.Durables)
				}
			} else if err := handler.HandleFailure(context.Background(), firstResult); err != nil {
				t.Fatalf("HandleFailure after deferred rotation: %v", err)
			}

			activePath, activeFile, currentFileName := failureArchiveActiveState(handler, tt.failureCategory)
			if activePath != oldPath {
				t.Fatalf("active path = %q, want old path %q", activePath, oldPath)
			}
			if activeFile != oldFile {
				t.Fatal("active file handle changed after rotation target open failure")
			}
			if currentFileName != originalFileName {
				t.Fatalf("file name state = %+v, want unchanged %+v", currentFileName, originalFileName)
			}
			assertArchivePathProtected(t, handler.handler.directoryForFailureCategory(tt.failureCategory).guard, oldPath, true)

			oldContent, err := os.ReadFile(oldPath)
			if err != nil {
				t.Fatalf("read old active archive: %v", err)
			}
			if !bytes.Contains(oldContent, []byte(firstMessage)) {
				t.Fatalf("old active archive does not contain %q", firstMessage)
			}
			if len(delegatedErrors) != 1 ||
				!errors.Is(delegatedErrors[0], ErrFailureArchiveRotationDeferred) {
				t.Fatalf("delegated errors = %v, want one deferred rotation error", delegatedErrors)
			}
			if notificationReadErr != nil {
				t.Fatalf("read old archive from OnError: %v", notificationReadErr)
			}
			if !notificationObservedOldWrite {
				t.Fatal("OnError ran before the failure record was written to the old active file")
			}

			// 障害を取り除くと、次回のfailure処理で同じrotation先を再試行して成功する。
			if err := os.Remove(candidatePath); err != nil {
				t.Fatalf("remove rotation blocker: %v", err)
			}
			secondMessage := "record-written-after-rotation-recovery-" + string(tt.failureCategory)
			secondResult := failureArchiveResultForFailureCategory(tt.failureCategory, secondMessage)
			if tt.withReport {
				if _, err := handler.HandleFailureWithReport(context.Background(), secondResult); err != nil {
					t.Fatalf("HandleFailureWithReport after rotation recovery: %v", err)
				}
			} else if err := handler.HandleFailure(context.Background(), secondResult); err != nil {
				t.Fatalf("HandleFailure after rotation recovery: %v", err)
			}

			recoveredPath, _, _ := failureArchiveActiveState(handler, tt.failureCategory)
			if recoveredPath != candidatePath {
				t.Fatalf("recovered active path = %q, want %q", recoveredPath, candidatePath)
			}
			recoveredContent, err := os.ReadFile(recoveredPath)
			if err != nil {
				t.Fatalf("read recovered active archive: %v", err)
			}
			if !bytes.Contains(recoveredContent, []byte(secondMessage)) {
				t.Fatalf("recovered active archive does not contain %q", secondMessage)
			}
		})
	}
}

func failureArchiveActiveState(
	pipeline *FailureArchivePipeline,
	failureCategory failurearchive.FailureCategory,
) (string, *os.File, logpattern.FileNamePattern) {
	if failureCategory == failurearchive.FailureCategoryPermanent {
		return pipeline.handler.Fsstore.ActiveFilePathPermanent,
			pipeline.handler.Fsstore.FilePermanent,
			pipeline.handler.Fsstore.FileNamePermanent
	}
	return pipeline.handler.Fsstore.ActiveFilePathRetryable,
		pipeline.handler.Fsstore.FileRetryable,
		pipeline.handler.Fsstore.FileNameRetryable
}

func failureArchiveResultForFailureCategory(
	failureCategory failurearchive.FailureCategory,
	message string,
) loggerfailure.SendResult {
	failed := archiveFailedRecord(message, time.Now(), 500, "test_failure")
	result := loggerfailure.SendResult{Meta: loggerfailure.Meta{Backend: "test-backend"}}
	if failureCategory == failurearchive.FailureCategoryPermanent {
		result.DeadRecords = []loggerfailure.FailedRecord{failed}
	} else {
		result.RetryableRecords = []loggerfailure.FailedRecord{failed}
	}
	return result
}
