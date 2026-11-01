package cipherfile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestSymmetricRoundTripAndKeyRotation(t *testing.T) {
	first, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	second, _ := GenerateKey()
	parsed, err := ParseKey(" " + first.String() + "\n")
	if err != nil || parsed.String() != first.String() || parsed.KeyID() != first.KeyID() {
		t.Fatalf("ParseKey round trip failed: %v", err)
	}
	for _, text := range []string{"", "hlog-key1-", "hlog-key1-AAAA", "hlog-pub1-AAAA"} {
		if _, err := ParseKey(text); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ParseKey(%q) = %v, want ErrInvalidKey", text, err)
		}
	}

	body := []byte("{\"a\":1}\n{\"b\":2}\n")
	writer, _ := NewSpoolCipher([]*Key{first})
	sealed, err := writer.Seal(body)
	if err != nil {
		t.Fatal(err)
	}
	if !IsSymmetric(sealed) || bytes.Contains(sealed, []byte("\"a\":1")) || KeyIDOfSymmetric(sealed) != first.KeyID() {
		t.Fatalf("sealed body %q", sealed)
	}
	// 同じ鍵で読めます。違う鍵しかなければ、鍵のIDが合わないと分かります。
	if plain, err := writer.Open(sealed); err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("Open = %q, %v", plain, err)
	}
	other, _ := NewSpoolCipher([]*Key{second})
	if _, err := other.Open(sealed); !errors.Is(err, ErrUnknownSpoolKey) {
		t.Fatalf("Open with another key = %v, want ErrUnknownSpoolKey", err)
	}
	// 鍵の切り替え: 新しい鍵を先頭に置けば、新しい鍵で書き、古い鍵でも読めます。
	rotated, _ := NewSpoolCipher([]*Key{second, first})
	if plain, err := rotated.Open(sealed); err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("Open after rotation = %q, %v", plain, err)
	}
	resealed, _ := rotated.Seal(body)
	if KeyIDOfSymmetric(resealed) != second.KeyID() {
		t.Fatal("the rotated cipher does not write with the first key")
	}
	// 改変と短すぎるfileは、壊れたfileです。
	damaged := append([]byte(nil), sealed...)
	damaged[len(damaged)-1] ^= 0x01
	if _, err := writer.Open(damaged); !errors.Is(err, ErrSpoolFileDamaged) {
		t.Fatalf("Open of a damaged body = %v", err)
	}
	if _, err := writer.Open(sealed[:symmetricHeaderSize+3]); !errors.Is(err, ErrSpoolFileDamaged) {
		t.Fatalf("Open of a short body = %v", err)
	}
}

func TestNilSpoolCipherPassesPlaintextAndRejectsEncrypted(t *testing.T) {
	var none *SpoolCipher
	body := []byte("plain\n")
	if sealed, err := none.Seal(body); err != nil || !bytes.Equal(sealed, body) || none.Enabled() {
		t.Fatalf("nil cipher Seal = %q, %v", sealed, err)
	}
	if plain, err := none.Open(body); err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("nil cipher Open = %q, %v", plain, err)
	}
	key, _ := GenerateKey()
	sealed, _ := SealSymmetric(key, body)
	if _, err := none.Open(sealed); !errors.Is(err, ErrSpoolKeyRequired) {
		t.Fatalf("nil cipher Open of an encrypted body = %v", err)
	}
	// 鍵のあるcipherは、暗号化前の平文のfileもそのまま読みます。
	with, _ := NewSpoolCipher([]*Key{key})
	if plain, err := with.Open(body); err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("Open of a plain body = %q, %v", plain, err)
	}
}

func TestKeyFileAndResolveSpoolCipher(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.key")
	key, _ := GenerateKey()
	if err := WriteKeyFile(path, key); err != nil {
		t.Fatal(err)
	}
	if err := WriteKeyFile(path, key); err == nil {
		t.Fatal("WriteKeyFile overwrote an existing file")
	}
	if info, _ := os.Stat(path); info.Mode().Perm()&0o077 != 0 && !isWindows() {
		t.Fatalf("key file permissions %o", info.Mode().Perm())
	}
	keys, err := LoadKeyFile(path)
	if err != nil || len(keys) != 1 || keys[0].String() != key.String() {
		t.Fatalf("LoadKeyFile = %d keys, %v", len(keys), err)
	}

	if c, err := ResolveSpoolCipher(fsstore.SpoolEncryptionConfig{}); c != nil || err != nil {
		t.Fatalf("disabled config = %v, %v", c, err)
	}
	if _, err := ResolveSpoolCipher(fsstore.SpoolEncryptionConfig{Enabled: true}); !errors.Is(err, ErrKeyFileRequired) {
		t.Fatalf("enabled without a key file = %v", err)
	}
	if _, err := ResolveSpoolCipher(fsstore.SpoolEncryptionConfig{Enabled: true, KeyFile: filepath.Join(dir, "missing")}); err == nil {
		t.Fatal("missing key file was accepted")
	}
	empty := filepath.Join(dir, "empty.key")
	os.WriteFile(empty, []byte("# nothing\n"), 0o600)
	if _, err := ResolveSpoolCipher(fsstore.SpoolEncryptionConfig{Enabled: true, KeyFile: empty}); !errors.Is(err, ErrNoKeyInFile) {
		t.Fatalf("empty key file = %v", err)
	}
	c, err := ResolveSpoolCipher(fsstore.SpoolEncryptionConfig{Enabled: true, KeyFile: path})
	if err != nil || !c.Enabled() {
		t.Fatalf("ResolveSpoolCipher = %v, %v", c, err)
	}
	sealed, _ := c.Seal([]byte("x\n"))
	if plain, err := c.Open(sealed); err != nil || string(plain) != "x\n" {
		t.Fatalf("round trip through the resolved cipher = %q, %v", plain, err)
	}
}
