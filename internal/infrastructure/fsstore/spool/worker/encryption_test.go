package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type encryptedProcessingTestConfig struct {
	processingTestConfig
	keyFile string
}

func (c encryptedProcessingTestConfig) Encryption() pkgfsstore.SpoolEncryptionConfig {
	return pkgfsstore.SpoolEncryptionConfig{Enabled: true, KeyFile: c.keyFile}
}

// 暗号化したactive fileは、鍵を持つStateStoreがclaimして読め、分割して書く結果も暗号化
// されます。鍵のないStateStoreは、暗号化したfileをdecodeできないfileとして扱います。
func TestStateStoreReadsAndWritesEncryptedSpoolFiles(t *testing.T) {
	t.Parallel()
	key, err := cipherfile.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "spool.key")
	if err := cipherfile.WriteKeyFile(keyFile, key); err != nil {
		t.Fatal(err)
	}
	spoolPath := t.TempDir()
	store, err := NewStateStore(encryptedProcessingTestConfig{processingTestConfig{spoolPath: spoolPath}, keyFile})
	if err != nil {
		t.Fatal(err)
	}
	records := []pkgspool.SpoolRecord{
		{OccurredAt: time.Date(2026, 8, 20, 12, 34, 56, 0, time.UTC), RequestID: "first", Payload: json.RawMessage(`{"n":1}`)},
		{OccurredAt: time.Date(2026, 8, 20, 12, 34, 57, 0, time.UTC), RequestID: "second", Payload: json.RawMessage(`{"n":2}`)},
	}
	body, err := buildSpoolNDJSON(records)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := cipherfile.SealSymmetric(key, body)
	if err != nil {
		t.Fatal(err)
	}
	rel := filepath.Join("2026", "08", "20", "12", "34", "56789_encrypted"+spooldir.SpoolExt)
	activePath := filepath.Join(store.RootDir.ActiveDir().Path(), rel)
	if err := os.MkdirAll(filepath.Dir(activePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activePath, sealed, 0o600); err != nil {
		t.Fatal(err)
	}

	processingPath, ok, err := store.ClaimOldestActiveFile(context.Background())
	if err != nil || !ok {
		t.Fatalf("ClaimOldestActiveFile = %q, %t, %v", processingPath, ok, err)
	}
	got, err := store.ReadProcessingRecords(context.Background(), processingPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].RequestID != "first" || got[1].RequestID != "second" {
		t.Fatalf("decrypted records %+v", got)
	}

	// 分割した結果は暗号化して書き、同じ内容をもう一度書いても既存のfileと照合して受け入れます。
	for range 2 {
		if err := store.StoreProcessingRecordsToState(context.Background(), processingPath, pkgspool.StateDone, got[:1]); err != nil {
			t.Fatal(err)
		}
	}
	donePath := filepath.Join(store.RootDir.DoneDir().Path(), rel)
	contents, err := os.ReadFile(donePath)
	if err != nil {
		t.Fatal(err)
	}
	if !cipherfile.IsSymmetric(contents) {
		t.Fatalf("the done file is not encrypted: %q", contents)
	}
	plain, err := cipherfile.OpenSymmetric(key, contents)
	if err != nil {
		t.Fatal(err)
	}
	done, err := decodeSpoolRecords(plain)
	if err != nil || len(done) != 1 || done[0].RequestID != "first" || done[0].FinalizedAt.IsZero() {
		t.Fatalf("done records %+v, %v", done, err)
	}

	// 鍵のないStateStoreは、暗号化したfileを読めません。
	plainStore, err := NewStateStore(processingTestConfig{spoolPath: spoolPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plainStore.ReadProcessingRecords(context.Background(), processingPath); !errors.Is(err, pkgspool.ErrDecodeProcessingSpoolFile) || !errors.Is(err, cipherfile.ErrSpoolKeyRequired) {
		t.Fatalf("ReadProcessingRecords without the key = %v", err)
	}
	// 鍵のあるStateStoreは、暗号化前の平文のfileも読めます。
	plainPath := filepath.Join(store.RootDir.ProcessingDir().Path(), "2026", "08", "20", "12", "35", "00000_plain"+spooldir.SpoolExt)
	os.MkdirAll(filepath.Dir(plainPath), 0o755)
	if err := os.WriteFile(plainPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ReadProcessingRecords(context.Background(), plainPath); err != nil || len(got) != 2 {
		t.Fatalf("ReadProcessingRecords of a plain file = %d records, %v", len(got), err)
	}
}
