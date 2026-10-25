package configerror

import (
	"errors"
	"io/fs"
	"testing"
)

var errKind = errors.New("kind")

// 表示文はfmt.Errorfと同じまま、種類と%wの原因の両方をerrors.Isで辿れることを確認します。
func TestNewKeepsMessageAndWrapsKindAndCause(t *testing.T) {
	err := New(errKind, "open %q: %w", "path", fs.ErrNotExist)
	if got, want := err.Error(), `open "path": file does not exist`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, errKind) {
		t.Fatalf("errors.Is(err, kind) = false")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("errors.Is(err, cause) = false")
	}
}
