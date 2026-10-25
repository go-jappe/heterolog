package storedvalue

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
)

var (
	errKind  = errors.New("kind")
	errOther = errors.New("other kind")
)

type storedNameError struct{ Name string }

func (e *storedNameError) Error() string { return fmt.Sprintf("invalid name %q", e.Name) }
func (e *storedNameError) Unwrap() error { return errKind }

// WithoutValuesは表示文と一致した種類だけを残し、保存された値を持つ型付きerrorを辿らせません。
func TestWithoutValuesKeepsTextAndMatchedKindsOnly(t *testing.T) {
	const stored = "stored-name"
	original := fmt.Errorf("validate: %w", &storedNameError{Name: stored})

	err := WithoutValues(original, errKind, errOther, nil)

	if err.Error() != original.Error() {
		t.Fatalf("Error() = %q, want unchanged %q", err.Error(), original.Error())
	}
	if !errors.Is(err, errKind) {
		t.Fatalf("errors.Is(err, errKind) = false, want the matched kind: %v", err)
	}
	if errors.Is(err, errOther) {
		t.Fatalf("errors.Is(err, errOther) = true, want only kinds that the original error matched")
	}
	var nameErr *storedNameError
	if errors.As(err, &nameErr) {
		t.Fatalf("errors.As() extracted %#v, want the typed error with the stored value hidden", nameErr)
	}
	if location, found := errortree.ExportedFieldContaining(err, stored); found {
		t.Fatalf("%s keeps the stored value", location)
	}
	if WithoutValues(nil, errKind) != nil {
		t.Fatal("WithoutValues(nil) != nil")
	}
}
