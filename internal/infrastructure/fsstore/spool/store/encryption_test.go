package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// encryptedSpoolStoreConfigは、spoolの暗号化を有効にした設定です。
type encryptedSpoolStoreConfig struct {
	testSpoolStoreConfig
	keyFile string
}

func (c encryptedSpoolStoreConfig) Encryption() pkgfsstore.SpoolEncryptionConfig {
	return pkgfsstore.SpoolEncryptionConfig{Enabled: true, KeyFile: c.keyFile}
}

func writeSpoolKeyFile(t *testing.T) (string, *cipherfile.Key) {
	t.Helper()
	key, err := cipherfile.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "spool.key")
	if err := cipherfile.WriteKeyFile(path, key); err != nil {
		t.Fatal(err)
	}
	return path, key
}

// 暗号化を有効にしたstoreは、spool fileを対称鍵で暗号化して書きます。鍵のfileがなければ
// 構築に失敗します。
func TestStoreByStateEncryptsSpoolFiles(t *testing.T) {
	t.Parallel()
	keyFile, key := writeSpoolKeyFile(t)
	cfg := encryptedSpoolStoreConfig{
		testSpoolStoreConfig: testSpoolStoreConfig{spoolPath: resolvedTempDirForTest(t), target: "test-app", dir: "opensearch"},
		keyFile:              keyFile,
	}
	now := time.Date(2026, 6, 11, 0, 5, 6, 0, time.UTC)
	store, err := NewSpoolStore(fixedClock{now: now}, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := pkgspool.SpoolRecord{OccurredAt: now, RequestID: "request-encrypted", Payload: json.RawMessage(`{"secret":"value"}`)}
	if err := store.StoreByState(context.Background(), pkgspool.StateActive, []pkgspool.SpoolRecord{record}); err != nil {
		t.Fatal(err)
	}
	files := listSpoolFiles(t, store.RootDir.ActiveDir().Path())
	if len(files) != 1 {
		t.Fatalf("spool files = %v", files)
	}
	contents, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !cipherfile.IsSymmetric(contents) || bytes.Contains(contents, []byte("request-encrypted")) {
		t.Fatalf("the spool file is not encrypted: %q", contents)
	}
	plain, err := cipherfile.OpenSymmetric(key, contents)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pkgspool.SpoolRecord
	if err := json.Unmarshal(bytes.TrimSpace(plain), &decoded); err != nil || decoded.RequestID != record.RequestID {
		t.Fatalf("decrypted spool file %q: %v", plain, err)
	}

	missing := cfg
	missing.keyFile = filepath.Join(t.TempDir(), "missing.key")
	if _, err := NewSpoolStore(fixedClock{now: now}, missing, nil, nil); err == nil {
		t.Fatal("NewSpoolStore accepted a missing key file")
	}
	missing.keyFile = ""
	if _, err := NewSpoolStore(fixedClock{now: now}, missing, nil, nil); !errors.Is(err, cipherfile.ErrKeyFileRequired) {
		t.Fatalf("NewSpoolStore without a key file = %v", err)
	}
}
