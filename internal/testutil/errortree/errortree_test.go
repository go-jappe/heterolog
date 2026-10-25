package errortree

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

type codeError struct{ code int }

func (e *codeError) Error() string { return fmt.Sprintf("code %d", e.code) }

func TestAllReturnsEveryTypedErrorInDepthFirstOrder(t *testing.T) {
	first := &codeError{code: 1}
	second := &codeError{code: 2}
	third := &codeError{code: 3}
	err := errors.Join(
		fmt.Errorf("wrapped: %w", first),
		errors.Join(second, errors.New("plain")),
		fmt.Errorf("multi: %w and %w", third, errors.New("other")),
	)

	got := All[*codeError](err)
	if len(got) != 3 || got[0] != first || got[1] != second || got[2] != third {
		t.Fatalf("All() = %v, want [%v %v %v]", got, first, second, third)
	}
	if got := All[*codeError](nil); len(got) != 0 {
		t.Fatalf("All(nil) = %v, want empty", got)
	}
}

func TestFindReturnsFirstMatchingTypedError(t *testing.T) {
	first := &codeError{code: 1}
	second := &codeError{code: 2}
	err := errors.Join(first, fmt.Errorf("wrapped: %w", second))

	got, ok := Find(err, func(e *codeError) bool { return e.code == 2 })
	if !ok || got != second {
		t.Fatalf("Find(code 2) = %v, %t, want %v, true", got, ok, second)
	}
	if got, ok := Find(err, func(e *codeError) bool { return e.code == 3 }); ok || got != nil {
		t.Fatalf("Find(code 3) = %v, %t, want nil, false", got, ok)
	}
}

type storedValueDetail struct {
	Value  string
	Raw    []byte
	Labels map[string]string
}

type hiddenDetail struct{ Name string }

type valueError struct {
	Detail  *storedValueDetail
	Details []storedValueDetail
	hiddenDetail
	value   string
	message string
	cause   error
}

func (e *valueError) Error() string { return e.message }
func (e *valueError) Unwrap() error { return e.cause }

func TestExportedFieldContainingFindsValuesReadableThroughErrorsAs(t *testing.T) {
	const marker = "stored-marker"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "pointer field", err: &valueError{Detail: &storedValueDetail{Value: "x-" + marker}}, want: "*errortree.valueError.Detail.Value"},
		{name: "byte slice", err: &valueError{Detail: &storedValueDetail{Raw: []byte(marker)}}, want: "*errortree.valueError.Detail.Raw"},
		{name: "map value", err: &valueError{Details: []storedValueDetail{{Labels: map[string]string{"k": marker}}}}, want: "*errortree.valueError.Details[0].Labels[k]"},
		{name: "map key", err: &valueError{Details: []storedValueDetail{{Labels: map[string]string{marker: "v"}}}}, want: "*errortree.valueError.Details[0].Labels[key]"},
		{name: "promoted field of unexported embedded type", err: &valueError{hiddenDetail: hiddenDetail{Name: marker}}, want: "*errortree.valueError.hiddenDetail.Name"},
		{name: "wrapped cause", err: errors.Join(errors.New("other"), fmt.Errorf("wrapped: %w", &valueError{Detail: &storedValueDetail{Value: marker}})), want: "*errortree.valueError.Detail.Value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExportedFieldContaining(tc.err, marker)
			if !ok || got != tc.want {
				t.Fatalf("ExportedFieldContaining() = %q, %t, want %q, true", got, ok, tc.want)
			}
		})
	}
}

// loopErrorは、公開fieldで自身を指すerrorです。
type loopError struct {
	Self    *loopError
	message string
}

func (e *loopError) Error() string { return e.message }

func TestExportedFieldContainingIgnoresTextAndUnexportedFields(t *testing.T) {
	const marker = "stored-marker"
	cyclic := &loopError{message: "cycle " + marker}
	cyclic.Self = cyclic
	for name, err := range map[string]error{
		"message text":         fmt.Errorf("text only: %v", marker),
		"unexported field":     &valueError{value: marker, message: "contains " + marker},
		"self reference":       cyclic,
		"nil error":            nil,
		"other exported value": &valueError{Detail: &storedValueDetail{Value: "unrelated"}},
	} {
		t.Run(name, func(t *testing.T) {
			if got, ok := ExportedFieldContaining(err, marker); ok {
				t.Fatalf("ExportedFieldContaining() = %q, want no readable value", got)
			}
		})
	}
}

func TestExtractableValueReportsDecodeErrorsAndExportedValues(t *testing.T) {
	const marker = "stored-marker"
	_, parseErr := time.Parse(time.RFC3339Nano, marker)
	var syntaxErr *json.SyntaxError
	if !errors.As(json.Unmarshal([]byte(`{"a":`+marker+`}`), &map[string]any{}), &syntaxErr) {
		t.Fatal("fixture did not produce *json.SyntaxError")
	}
	var typeErr *json.UnmarshalTypeError
	if !errors.As(json.Unmarshal([]byte(`["`+marker+`"]`), &map[string]any{}), &typeErr) {
		t.Fatal("fixture did not produce *json.UnmarshalTypeError")
	}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "time parse error", err: fmt.Errorf("decode: %w", parseErr), want: "*time.ParseError"},
		{name: "json syntax error", err: fmt.Errorf("decode: %w", syntaxErr), want: "*json.SyntaxError"},
		{name: "json type error", err: fmt.Errorf("decode: %w", typeErr), want: "*json.UnmarshalTypeError"},
		{name: "exported field", err: &valueError{Detail: &storedValueDetail{Value: marker}}, want: "*errortree.valueError.Detail.Value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractableValue(tc.err, marker)
			if !ok || got != tc.want {
				t.Fatalf("ExtractableValue() = %q, %t, want %q, true", got, ok, tc.want)
			}
		})
	}
	// 表示文だけに含まれる値は、errors.Asで取り出せないため対象外です。
	if got, ok := ExtractableValue(fmt.Errorf("decode: %v", parseErr), marker); ok {
		t.Fatalf("ExtractableValue(text only) = %q, want not extractable", got)
	}
}
