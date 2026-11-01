package cipherfile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeSegmentは、recipientへの1つのsegment（headerとrecordごとのchunk）を返します。
func writeSegment(t *testing.T, recipient *Recipient, records ...string) []byte {
	t.Helper()
	header, sealer, err := NewSealer(recipient)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), header...)
	for _, record := range records {
		chunk, err := sealer.Seal([]byte(record))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, chunk...)
	}
	return out
}

func decrypt(t *testing.T, file []byte, identities ...*Identity) (string, Report, error) {
	t.Helper()
	var out bytes.Buffer
	report, err := Decrypt(bytes.NewReader(file), identities, &out)
	return out.String(), report, err
}

func TestKeysRoundTrip(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := ParseRecipient(" " + identity.Recipient().String() + "\n")
	if err != nil {
		t.Fatal(err)
	}
	if recipient.String() != identity.Recipient().String() || recipient.KeyID() != identity.KeyID() {
		t.Fatalf("parsed recipient differs: %s vs %s", recipient, identity.Recipient())
	}
	parsed, err := ParseIdentity(identity.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.String() != identity.String() || parsed.KeyID() != identity.KeyID() {
		t.Fatal("parsed identity differs")
	}
	if len(identity.KeyID()) != KeyIDSize*2 {
		t.Fatalf("key id %q has an unexpected length", identity.KeyID())
	}
	for _, text := range []string{"", "hlog-pub1-", "hlog-pub1-!!!", "hlog-pub1-AAAA", identity.String()} {
		if _, err := ParseRecipient(text); !errors.Is(err, ErrInvalidRecipient) {
			t.Errorf("ParseRecipient(%q) = %v, want ErrInvalidRecipient", text, err)
		}
	}
	for _, text := range []string{"", "hlog-sec1-AAAA", identity.Recipient().String()} {
		if _, err := ParseIdentity(text); !errors.Is(err, ErrInvalidIdentity) {
			t.Errorf("ParseIdentity(%q) = %v, want ErrInvalidIdentity", text, err)
		}
	}
}

func TestDecryptAppendedSegments(t *testing.T) {
	identity, _ := GenerateIdentity()
	recipient := identity.Recipient()
	file := writeSegment(t, recipient, "one\n", "two\n")
	// process再起動の後の追記は、秘密鍵なしで新しいsegmentを足します。
	file = append(file, writeSegment(t, recipient, "three\n")...)
	file = append(file, writeSegment(t, recipient)...) // recordのない開閉
	file = append(file, writeSegment(t, recipient, "four\n")...)

	got, report, err := decrypt(t, file, identity)
	if err != nil {
		t.Fatal(err)
	}
	if want := "one\ntwo\nthree\nfour\n"; got != want {
		t.Fatalf("decrypted %q, want %q", got, want)
	}
	if report.Segments != 4 || report.Records != 4 || report.PlaintextBytes != int64(len(got)) || !report.Complete() {
		t.Fatalf("unexpected report %+v", report)
	}
	if !bytes.HasPrefix(file, []byte(Magic)) {
		t.Fatal("the file does not start with the magic")
	}
}

func TestDecryptWrongAndSeveralIdentities(t *testing.T) {
	first, _ := GenerateIdentity()
	second, _ := GenerateIdentity()
	other, _ := GenerateIdentity()
	file := writeSegment(t, first.Recipient(), "a\n")
	file = append(file, writeSegment(t, second.Recipient(), "b\n")...)

	got, report, err := decrypt(t, file, other, second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "b\n" {
		t.Fatalf("decrypted %q, want only the second segment", got)
	}
	if len(report.UnknownKeyIDs) != 1 || report.UnknownKeyIDs[0] != first.KeyID() {
		t.Fatalf("unknown key ids %v, want %s", report.UnknownKeyIDs, first.KeyID())
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Reason != SkipUnknownKey || report.Skipped[0].Offset != 0 {
		t.Fatalf("unexpected skips %+v", report.Skipped)
	}
	// 鍵の切り替えでは、古い鍵も渡せば全部読めます。
	got, report, _ = decrypt(t, file, second, first)
	if got != "a\nb\n" || !report.Complete() {
		t.Fatalf("decrypted %q with report %+v", got, report)
	}
}

func TestDecryptSkipsTornTail(t *testing.T) {
	identity, _ := GenerateIdentity()
	recipient := identity.Recipient()
	first := writeSegment(t, recipient, "one\n", "two\n")
	// 2件目のchunkが途中で切れたまま止まり、次の起動が新しいsegmentを足した場合です。
	torn := first[:len(first)-3]
	file := append(append([]byte(nil), torn...), writeSegment(t, recipient, "three\n")...)

	got, report, err := decrypt(t, file, identity)
	if err != nil {
		t.Fatal(err)
	}
	if got != "one\nthree\n" {
		t.Fatalf("decrypted %q", got)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Reason != SkipDamagedChunk {
		t.Fatalf("unexpected skips %+v", report.Skipped)
	}
	skip := report.Skipped[0]
	if int(skip.Offset+skip.Length) != len(torn) {
		t.Fatalf("skip %+v does not end at the torn tail %d", skip, len(torn))
	}
	// 末尾が切れたままのfileも、読める分だけ復号します。
	got, report, _ = decrypt(t, torn, identity)
	if got != "one\n" || len(report.Skipped) != 1 {
		t.Fatalf("decrypted %q with report %+v", got, report)
	}
}

func TestDecryptSkipsCorruptedChunk(t *testing.T) {
	identity, _ := GenerateIdentity()
	file := writeSegment(t, identity.Recipient(), "one\n", "two\n")
	file = append(file, writeSegment(t, identity.Recipient(), "three\n")...)
	header, _, _ := NewSealer(identity.Recipient())
	// 1件目の暗号文の1 bitを変えます。同じsegmentの残りはnonceが合わなくなり、次のsegmentから
	// 読めます。
	file[len(header)+chunkLengthSize+1] ^= 0x01

	got, report, err := decrypt(t, file, identity)
	if err != nil {
		t.Fatal(err)
	}
	if got != "three\n" {
		t.Fatalf("decrypted %q", got)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Reason != SkipUnreadableChunk || report.Skipped[0].Offset != int64(len(header)) {
		t.Fatalf("unexpected skips %+v", report.Skipped)
	}
}

func TestDecryptPlainPrefixAndNotEncrypted(t *testing.T) {
	identity, _ := GenerateIdentity()
	plain := []byte("plain line before encryption was enabled\n")
	file := append(append([]byte(nil), plain...), writeSegment(t, identity.Recipient(), "one\n")...)

	got, report, err := decrypt(t, file, identity)
	if err != nil {
		t.Fatal(err)
	}
	if got != "one\n" {
		t.Fatalf("decrypted %q", got)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Reason != SkipNotEncrypted || report.Skipped[0].Length != int64(len(plain)) {
		t.Fatalf("unexpected skips %+v", report.Skipped)
	}
	if _, _, err := decrypt(t, plain, identity); !errors.Is(err, ErrNotEncrypted) {
		t.Fatalf("err = %v, want ErrNotEncrypted", err)
	}
	if got, report, err := decrypt(t, nil, identity); err != nil || got != "" || report.Segments != 0 {
		t.Fatalf("empty file: %q %+v %v", got, report, err)
	}
}

func TestWriterSealsEachWrite(t *testing.T) {
	identity, _ := GenerateIdentity()
	header, sealer, err := NewSealer(identity.Recipient())
	if err != nil {
		t.Fatal(err)
	}
	var file bytes.Buffer
	file.Write(header)
	w := NewWriter(&file, sealer)
	for _, line := range []string{"{\"a\":1}\n", "{\"b\":2}\n"} {
		n, err := w.Write([]byte(line))
		if err != nil || n != len(line) {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	got, report, _ := decrypt(t, file.Bytes(), identity)
	if got != "{\"a\":1}\n{\"b\":2}\n" || report.Records != 2 {
		t.Fatalf("decrypted %q with %+v", got, report)
	}
	if _, err := sealer.Seal(make([]byte, MaxChunkLength)); !errors.Is(err, ErrChunkTooLong) {
		t.Fatalf("Seal of a huge record = %v", err)
	}
}

func TestIdentityFile(t *testing.T) {
	identity, _ := GenerateIdentity()
	path := filepath.Join(t.TempDir(), "identity")
	if err := WriteIdentityFile(path, identity); err != nil {
		t.Fatal(err)
	}
	if err := WriteIdentityFile(path, identity); err == nil {
		t.Fatal("WriteIdentityFile overwrote an existing file")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 && !isWindows() {
		t.Fatalf("identity file permissions %o", perm)
	}
	contents, _ := os.ReadFile(path)
	if !strings.Contains(string(contents), identity.Recipient().String()) || !strings.Contains(string(contents), identity.KeyID()) {
		t.Fatalf("identity file does not show the public key and key id:\n%s", contents)
	}
	identities, err := LoadIdentityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 || identities[0].String() != identity.String() {
		t.Fatalf("loaded %d identities", len(identities))
	}
	empty := filepath.Join(t.TempDir(), "empty")
	os.WriteFile(empty, []byte("# only a comment\n"), 0o600)
	if _, err := LoadIdentityFile(empty); !errors.Is(err, ErrNoIdentityInFile) {
		t.Fatalf("LoadIdentityFile(empty) = %v", err)
	}
	bad := filepath.Join(t.TempDir(), "bad")
	os.WriteFile(bad, []byte(identity.String()+"\nnot-a-key\n"), 0o600)
	if _, err := LoadIdentityFile(bad); !errors.Is(err, ErrInvalidIdentity) || !strings.Contains(err.Error(), ":2:") {
		t.Fatalf("LoadIdentityFile(bad) = %v", err)
	}
}
