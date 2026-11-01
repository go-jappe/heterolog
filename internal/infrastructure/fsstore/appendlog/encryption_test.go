package appendlog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// 暗号化したFileStoreは、openのたびにsegmentを始め、行ごとに暗号化して書きます。
// 再開（2回目のopen）の後も秘密鍵なしで追記でき、復号すると全行が順に戻ります。
func TestFileStoreEncryptsEachLineAndAppendsAcrossReopen(t *testing.T) {
	identity, err := cipherfile.GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "app.log")
	store := &FileStore{Recipient: identity.Recipient(), Tracer: observer.NopTracer{}}
	retry := NewRetryConfigAdapter(pkgfsstore.RetryConfig{MaxWriteRetries: 1})
	write := func(msg string) uint64 {
		t.Helper()
		written, err := store.WriteStringWithRetry(context.Background(), msg, nil, nil, nil, "write failed", nil, retry)
		if err != nil {
			t.Fatal(err)
		}
		return written
	}
	for round, lines := range [][]string{{"one", "two"}, {"three"}} {
		file, err := store.OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		store.File = file
		for _, line := range lines {
			if written := write(line); written <= uint64(len(line)+1) {
				t.Fatalf("round %d: written %d bytes, want more than the plaintext %d", round, written, len(line)+1)
			}
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store.File = nil
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(contents, []byte("one")) || !bytes.HasPrefix(contents, []byte(cipherfile.Magic)) {
		t.Fatalf("the file is not encrypted: %q", contents)
	}
	var plain bytes.Buffer
	report, err := cipherfile.Decrypt(bytes.NewReader(contents), []*cipherfile.Identity{identity}, &plain)
	if err != nil {
		t.Fatal(err)
	}
	if plain.String() != "one\ntwo\nthree\n" || report.Segments != 2 || !report.Complete() {
		t.Fatalf("decrypted %q with report %+v", plain.String(), report)
	}

	// Recipientがなければ、同じstoreは平文で書きます。
	store.Recipient = nil
	plainPath := filepath.Join(t.TempDir(), "plain.log")
	file, err := store.OpenFile(plainPath)
	if err != nil {
		t.Fatal(err)
	}
	store.File = file
	write("plain")
	store.Close()
	if contents, _ := os.ReadFile(plainPath); string(contents) != "plain\n" {
		t.Fatalf("plain store wrote %q", contents)
	}
}

// 暗号化を有効にした設定では、圧縮の設定は無効になり、無視したことが分かります。
func TestConfigAdapterSuppressesCompressionWhenEncrypting(t *testing.T) {
	cfg := pkgfsstore.Config{
		Lifecycle:  pkgfsstore.FileLifecycleConfig{EnableFileCompress: true, CompressFormat: pkgfsstore.CompressGzip},
		Encryption: pkgfsstore.EncryptionConfig{Enabled: true, Recipient: "hlog-pub1-x"},
	}
	adapter := NewConfigAdapter(cfg)
	if adapter.Lifecycle().EnableFileCompress() || !adapter.Lifecycle().CompressionSuppressedByEncryption() {
		t.Fatal("compression is not suppressed by encryption")
	}
	if adapter.Encryption() != cfg.Encryption {
		t.Fatalf("Encryption() = %+v", adapter.Encryption())
	}
	cfg.Encryption.Enabled = false
	adapter = NewConfigAdapter(cfg)
	if !adapter.Lifecycle().EnableFileCompress() || adapter.Lifecycle().CompressionSuppressedByEncryption() {
		t.Fatal("compression is suppressed without encryption")
	}
	delivery := NewDeliveryFileLifecycleStoreConfigAdapter(pkgfsstore.DeliveryFileLifecycleStoreConfig{
		Lifecycle:  pkgfsstore.DeliveryFileLifecycleConfig{EnableFileCompress: true},
		Encryption: pkgfsstore.EncryptionConfig{Enabled: true},
	})
	if delivery.Lifecycle().EnableFileCompress() || !delivery.Lifecycle().CompressionSuppressedByEncryption() {
		t.Fatal("delivery compression is not suppressed by encryption")
	}
}
