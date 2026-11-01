package failurearchive

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
)

// 暗号化したFileStoreは、系列ごとに別のsegmentを持ち、Frameが系列の文脈でrecordを暗号化します。
// 平文で切れた末尾の検査は、暗号化したfileでは行いません。
func TestFileStoreEncryptsEachCategory(t *testing.T) {
	identity, err := cipherfile.GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store := &FileStore{Recipient: identity.Recipient()}
	paths := map[FailureCategory]string{
		FailureCategoryRetryable: filepath.Join(dir, "retryable.jsonl"),
		FailureCategoryPermanent: filepath.Join(dir, "permanent.jsonl"),
	}
	for category, path := range paths {
		file, err := store.OpenActiveFile(path, category)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range []string{`{"c":"` + string(category) + `","n":1}` + "\n", `{"c":"` + string(category) + `","n":2}` + "\n"} {
			framed, err := store.Frame(category, []byte(record))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(framed, []byte(record)) {
				t.Fatalf("%s record was not encrypted", category)
			}
			if _, err := file.Write(framed); err != nil {
				t.Fatal(err)
			}
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for category, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var plain bytes.Buffer
		report, err := cipherfile.Decrypt(bytes.NewReader(contents), []*cipherfile.Identity{identity}, &plain)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"c":"` + string(category) + `","n":1}` + "\n" + `{"c":"` + string(category) + `","n":2}` + "\n"
		if plain.String() != want || !report.Complete() {
			t.Fatalf("%s decrypted %q with report %+v", category, plain.String(), report)
		}
	}

	// 切れた末尾の後にも、新しいsegmentとして開けます（平文のOpenFileは拒否します）。
	torn := paths[FailureCategoryRetryable]
	contents, _ := os.ReadFile(torn)
	if err := os.WriteFile(torn, contents[:len(contents)-5], 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(torn); err == nil {
		t.Fatal("plain OpenFile accepted a torn tail")
	}
	file, err := store.OpenActiveFile(torn, FailureCategoryRetryable)
	if err != nil {
		t.Fatalf("encrypted open after a torn tail: %v", err)
	}
	file.Close()
	if _, err := store.Frame("other", nil); err == nil {
		t.Fatal("Frame accepted an unknown category")
	}
}
