package testutil

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 閉じたfileのStatとWriteは、OSごとのerrorでも同じ原因として判定できます。
func TestIsClosedFileErrorRecognizesOperationsOnClosedFile(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "closed"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); err != nil || IsClosedFileError(err) {
		t.Fatalf("Stat on open file = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, statErr := file.Stat()
	_, writeErr := file.Write([]byte("after close"))
	for name, err := range map[string]error{"Stat": statErr, "Write": writeErr, "Close": file.Close()} {
		if !IsClosedFileError(err) {
			t.Fatalf("%s on closed file = %v, want closed-file error", name, err)
		}
	}
	if IsClosedFileError(errors.New("unrelated")) || IsClosedFileError(nil) {
		t.Fatal("unrelated errors must not be treated as closed-file errors")
	}
}
