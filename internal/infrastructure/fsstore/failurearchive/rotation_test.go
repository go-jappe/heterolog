package failurearchive

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

func TestRotateCommitsNewActiveFileWhenPreviousCloseFails(t *testing.T) {
	tests := []FailureCategory{FailureCategoryRetryable, FailureCategoryPermanent}

	for _, failureCategory := range tests {
		t.Run(string(failureCategory), func(t *testing.T) {
			cfg := newValidTestFileArchiveConfig(t)
			cfg.maxFileSize = 1
			fileName := newTestFileNamePattern(cfg)
			fileName.Dir = t.TempDir()
			rotationAt := time.Date(2042, time.May, 6, 8, 30, 0, 0, time.UTC)
			date, err := logpattern.ResolveDateStringForFileNameAt(cfg, rotationAt)
			if err != nil {
				t.Fatalf("resolve archive date: %v", err)
			}
			oldPath := logpattern.BuildFileFullPath(date, fileName, cfg)
			oldFile, err := OpenFile(oldPath)
			if err != nil {
				t.Fatalf("open old active file: %v", err)
			}
			t.Cleanup(func() { _ = oldFile.Close() })
			if _, err := oldFile.Write([]byte("x")); err != nil {
				t.Fatalf("grow old active file: %v", err)
			}

			store := &FileStore{Config: cfg}
			switch failureCategory {
			case FailureCategoryRetryable:
				store.FileRetryable = oldFile
				store.ActiveFilePathRetryable = oldPath
				store.FileNameRetryable = fileName
			case FailureCategoryPermanent:
				store.FilePermanent = oldFile
				store.ActiveFilePathPermanent = oldPath
				store.FileNamePermanent = fileName
			}

			closeFailure := errors.New("injected previous file close failure")
			closeCalls := 0
			result, err := store.rotate(
				context.Background(),
				failureCategory,
				rotationAt,
				func(file *os.File) error {
					closeCalls++
					if file != oldFile {
						t.Fatalf("close target = %p, want old active file %p", file, oldFile)
					}
					return closeFailure
				},
			)
			if err != nil {
				t.Fatalf("rotate() error = %v, want successful active transition", err)
			}
			if closeCalls != 1 {
				t.Fatalf("previous file close calls = %d, want 1", closeCalls)
			}
			if !errors.Is(result.PreviousFileCloseErr, closeFailure) {
				t.Fatalf("PreviousFileCloseErr = %v, want %v", result.PreviousFileCloseErr, closeFailure)
			}
			if result.OldFilePath != oldPath {
				t.Fatalf("OldFilePath = %q, want %q", result.OldFilePath, oldPath)
			}

			var newFile *os.File
			var newPath string
			switch failureCategory {
			case FailureCategoryRetryable:
				newFile = store.FileRetryable
				newPath = store.ActiveFilePathRetryable
			case FailureCategoryPermanent:
				newFile = store.FilePermanent
				newPath = store.ActiveFilePathPermanent
			}
			if newFile == nil || newFile == oldFile {
				t.Fatal("new active file was not installed")
			}
			t.Cleanup(func() { _ = newFile.Close() })
			if newPath == oldPath {
				t.Fatalf("new active path remained old path %q", oldPath)
			}
			if _, err := newFile.Write([]byte("new-active\n")); err != nil {
				t.Fatalf("write new active file: %v", err)
			}
		})
	}
}
