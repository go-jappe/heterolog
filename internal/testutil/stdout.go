package testutil

import (
	"io"
	"os"
	"testing"
)

// CaptureStdoutは、fnを実行する間だけos.Stdoutを一時fileへ差し替え、書かれた内容を
// 返します。os.Stdoutはprocess全体で共有するため、呼び出し側は並列実行しないでください。
func CaptureStdout(t testing.TB, fn func()) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	original := os.Stdout
	os.Stdout = file
	defer func() { os.Stdout = original }()
	fn()
	os.Stdout = original
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}
