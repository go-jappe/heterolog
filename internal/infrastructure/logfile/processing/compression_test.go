package processing

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestProcessCompressionReadyRequiresSourceRetirement(t *testing.T) {
	failure := errors.New("directory sync failed")
	cleanupFailure := errors.Join(appendlog.ErrCompressionSourceCleanup, failure)
	for _, test := range []struct {
		name  string
		err   error
		ready bool
	}{
		{name: "completed", ready: true},
		{name: "retirement not confirmed", err: failure},
		{name: "pending source cleanup failed", err: cleanupFailure, ready: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			compressed := false
			result, err := ProcessCompression(context.Background(), CompressionRequest{
				SourcePath: "app_20261011_1.log", CompressedPath: "app_20261011_1.log.gz", ExistingFileObserved: true,
			}, CompressionOperations{
				VerifyExisting: func(string, string) (bool, error) { return true, nil },
				Compress: func(context.Context, string) (string, error) {
					compressed = true
					return "", nil
				},
				RemoveSource: func(string, string) error { return test.err },
			})
			if err != test.err || result.ReadyForUpload != test.ready || !result.ExistingFileVerified || !result.FilesMayHaveChanged || compressed {
				t.Fatalf("result = %+v, err = %v, compressed = %v", result, err, compressed)
			}
		})
	}
}

func TestProcessCompressionPreservesPublishedPathOnFailure(t *testing.T) {
	failure := errors.New("source rename failed")
	for _, test := range []struct {
		name                string
		path                string
		err                 error
		ready               bool
		filesMayHaveChanged bool
	}{
		{name: "before publication", err: failure},
		{name: "after publication before retirement", path: "app.jsonl.gz", err: failure, filesMayHaveChanged: true},
		{name: "source cleanup pending", path: "app.jsonl.gz", err: appendlog.ErrCompressionSourceCleanup, ready: true, filesMayHaveChanged: true},
		{name: "completed", path: "app.jsonl.gz", ready: true, filesMayHaveChanged: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ProcessCompression(context.Background(), CompressionRequest{SourcePath: "app.jsonl"}, CompressionOperations{
				Compress: func(context.Context, string) (string, error) { return test.path, test.err },
			})
			if err != test.err || result.CompressedPath != test.path || result.ReadyForUpload != test.ready || result.FilesMayHaveChanged != test.filesMayHaveChanged || result.ExistingFileVerified {
				t.Fatalf("result = %+v, err = %v", result, err)
			}
		})
	}
}

func TestProcessCompressionInvalidPeerDoesNotRetireOrRecompressSource(t *testing.T) {
	inspectionFailure := errors.New("cannot inspect peer")
	for _, test := range []struct {
		name                    string
		existing                bool
		verifyErr               error
		removeErr               error
		wantRemove              bool
		wantFilesMayHaveChanged bool
		cleanup                 bool
	}{
		{name: "observed peer disappeared"},
		{name: "codec unavailable", existing: true, verifyErr: fsstore.ErrUnsupportedCompressionFormat},
		{name: "inspection failed", verifyErr: inspectionFailure},
		{name: "rotation leaves corrupt peer for full scan", existing: true, verifyErr: pkgfsstore.ErrCorruptCompressedFile},
		{name: "corrupt peer removed", existing: true, verifyErr: pkgfsstore.ErrCorruptCompressedFile, wantRemove: true, wantFilesMayHaveChanged: true, cleanup: true},
		{name: "corrupt peer removal failed", existing: true, verifyErr: pkgfsstore.ErrCorruptCompressedFile, removeErr: inspectionFailure, wantRemove: true, cleanup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			removed, retired, compressed := false, false, false
			result, err := ProcessCompression(context.Background(), CompressionRequest{
				SourcePath: "app.log", CompressedPath: "app.log.custom", ExistingFileObserved: true, RemoveCorruptPeer: test.cleanup,
			}, CompressionOperations{
				VerifyExisting: func(string, string) (bool, error) { return test.existing, test.verifyErr },
				RemoveInvalid:  func(string, string) error { removed = true; return test.removeErr },
				RemoveSource:   func(string, string) error { retired = true; return nil },
				Compress:       func(context.Context, string) (string, error) { compressed = true; return "app.log.gz", nil },
			})
			if err == nil || result.ReadyForUpload || result.FilesMayHaveChanged != test.wantFilesMayHaveChanged || removed != test.wantRemove || retired || compressed {
				t.Fatalf("result = %+v, err = %v, removed = %v, retired = %v, compressed = %v", result, err, removed, retired, compressed)
			}
			if test.removeErr != nil && !errors.Is(err, test.removeErr) {
				t.Fatalf("removal error was lost: %v", err)
			}
		})
	}
}

func TestProcessCompressionRejectsMissingOutputOnSuccess(t *testing.T) {
	result, err := ProcessCompression(context.Background(), CompressionRequest{SourcePath: "app.log"}, CompressionOperations{
		Compress: func(context.Context, string) (string, error) { return "", nil },
	})
	if !errors.Is(err, ErrEmptyCompressedPath) || result.ReadyForUpload {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestProcessCompressionCreatesUnoccupiedPlannedPath(t *testing.T) {
	verified, compressed := false, false
	result, err := ProcessCompression(context.Background(), CompressionRequest{
		SourcePath: "app.log", CompressedPath: "app.log.gz",
	}, CompressionOperations{
		VerifyExisting: func(string, string) (bool, error) { verified = true; return false, nil },
		Compress: func(context.Context, string) (string, error) {
			if !verified {
				t.Fatal("compression started before checking the planned destination")
			}
			compressed = true
			return "app.log.gz", nil
		},
	})
	if err != nil || !compressed || !result.ReadyForUpload || result.ExistingFileVerified {
		t.Fatalf("result = %+v, err = %v, compressed = %v", result, err, compressed)
	}
}
