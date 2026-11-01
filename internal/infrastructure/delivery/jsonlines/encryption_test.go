package jsonlines

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
)

func marshalLines(items []string) ([]byte, error) {
	return []byte(strings.Join(items, ",")), nil
}

// recipientを渡したfileのExporterは、batchごとに1つのchunkとして暗号化し、再度開いた後も
// 新しいsegmentとして追記します。
func TestOpenFileEncryptsEachBatch(t *testing.T) {
	identity, err := cipherfile.GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "spans.jsonl")
	for _, batches := range [][][]string{{{"a", "b"}, {"c"}}, {{"d"}}} {
		exporter, err := OpenFile(path, marshalLines, identity.Recipient())
		if err != nil {
			t.Fatal(err)
		}
		for _, batch := range batches {
			if err := exporter.Export(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
		}
		if err := exporter.Close(); err != nil {
			t.Fatal(err)
		}
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(contents, []byte("a,b")) {
		t.Fatalf("the file is not encrypted: %q", contents)
	}
	var plain bytes.Buffer
	report, err := cipherfile.Decrypt(bytes.NewReader(contents), []*cipherfile.Identity{identity}, &plain)
	if err != nil {
		t.Fatal(err)
	}
	if plain.String() != "a,b\nc\nd\n" || report.Segments != 2 || report.Records != 3 || !report.Complete() {
		t.Fatalf("decrypted %q with report %+v", plain.String(), report)
	}
}
