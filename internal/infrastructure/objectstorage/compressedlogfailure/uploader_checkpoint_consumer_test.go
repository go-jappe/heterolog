package compressedlogfailure

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// minimalUploadCheckpointStoreは、ManagedUploaderが使うcheckpoint操作だけを
// 意図的に実装します。
type minimalUploadCheckpointStore struct {
	loadCalls         int
	progressLoadCalls int
	saveCalls         int
	progress          checkpoint.UploadProgress
	hasProgress       bool
}

func (s *minimalUploadCheckpointStore) Load(_ context.Context, key checkpoint.CheckpointKey) (checkpoint.Checkpoint, error) {
	s.loadCalls++
	return checkpoint.Checkpoint{Key: key}, nil
}

func (s *minimalUploadCheckpointStore) LoadUploadProgress(_ context.Context, key checkpoint.CheckpointKey, destination objectstorage.DestinationID) (checkpoint.UploadProgress, bool, error) {
	s.progressLoadCalls++
	if s.hasProgress && s.progress.Key == key && s.progress.DestinationID == destination {
		return s.progress, true, nil
	}
	return checkpoint.UploadProgress{}, false, nil
}

func (s *minimalUploadCheckpointStore) SaveUploadProgress(_ context.Context, progress checkpoint.UploadProgress) error {
	s.saveCalls++
	s.progress = progress
	s.hasProgress = true
	return nil
}

func TestManagedUploaderAcceptsMinimalUploadCheckpointStore(t *testing.T) {
	base := t.TempDir()
	source, _ := writeCompressedLog(t, base, "app_20260915_1.log.gz")
	store := &minimalUploadCheckpointStore{}
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		CheckpointStore:                     store,
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)

	request := uploadertest.Request(t, source)
	for range 2 {
		if err := managed.Upload(context.Background(), request); err != nil {
			t.Fatalf("Upload() error = %v", err)
		}
	}
	if got := remote.callCount(); got != 1 {
		t.Fatalf("direct upload calls = %d, want 1", got)
	}
	if store.loadCalls != 2 || store.progressLoadCalls != 2 || store.saveCalls != 1 {
		t.Fatalf("checkpoint calls: Load=%d LoadUploadProgress=%d SaveUploadProgress=%d, want 2, 2, 1",
			store.loadCalls, store.progressLoadCalls, store.saveCalls)
	}
	if want := (checkpoint.CheckpointKey{Generation: "20260915-1", Compressed: true}); !store.hasProgress || store.progress.Key != want || store.progress.DestinationID != "destination-a" || store.progress.UploadResult != checkpoint.UploadResultSuccess {
		t.Fatalf("saved upload progress = %+v, want successful upload for %v", store.progress, want)
	}
}
