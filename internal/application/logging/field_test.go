package logging_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
)

type failingJSONMarshaler struct{}

func (failingJSONMarshaler) MarshalJSON() ([]byte, error) {
	return nil, errors.New("marshal failure")
}

type trackingJSONMarshaler struct {
	called *bool
}

func (m trackingJSONMarshaler) MarshalJSON() ([]byte, error) {
	*m.called = true
	return []byte(`true`), nil
}

func TestFieldConstructorsPreserveTypedValues(t *testing.T) {
	mustField := func(field applogging.Field, err error) applogging.Field {
		t.Helper()
		if err != nil {
			t.Fatalf("construct Field: %v", err)
		}
		return field
	}
	floatField, err := applogging.Float64("ratio", 0.5)
	if err != nil {
		t.Fatalf("Float64() error = %v", err)
	}
	anyField, err := applogging.Any("profile", map[string]any{
		"tier":    "premium",
		"enabled": true,
		"count":   2,
	})
	if err != nil {
		t.Fatalf("Any() error = %v", err)
	}

	tests := []struct {
		name      string
		field     applogging.Field
		wantKey   string
		wantValue any
	}{
		{name: "request ID", field: applogging.WithRequestID("request-1"), wantKey: "request_id", wantValue: "request-1"},
		{name: "string", field: mustField(applogging.String("user_id", "hogehoge")), wantKey: "user_id", wantValue: "hogehoge"},
		{name: "bool", field: mustField(applogging.Bool("enabled", true)), wantKey: "enabled", wantValue: true},
		{name: "int", field: mustField(applogging.Int("attempt", 2)), wantKey: "attempt", wantValue: int(2)},
		{name: "int64", field: mustField(applogging.Int64("offset", 3)), wantKey: "offset", wantValue: int64(3)},
		{name: "uint64", field: mustField(applogging.Uint64("size", 4)), wantKey: "size", wantValue: uint64(4)},
		{name: "float64", field: floatField, wantKey: "ratio", wantValue: 0.5},
		{name: "any null", field: mustField(applogging.Any("optional", nil)), wantKey: "optional", wantValue: nil},
		{
			name:    "any object",
			field:   anyField,
			wantKey: "profile",
			wantValue: map[string]any{
				"tier":    "premium",
				"enabled": true,
				"count":   json.Number("2"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !test.field.IsDefined() {
				t.Fatal("IsDefined() = false, want true")
			}
			if got := test.field.Key(); got != test.wantKey {
				t.Fatalf("Key() = %q, want %q", got, test.wantKey)
			}
			if got := test.field.Value(); !reflect.DeepEqual(got, test.wantValue) {
				t.Fatalf("Value() = %#v (%T), want %#v (%T)", got, got, test.wantValue, test.wantValue)
			}
		})
	}
}

func TestFieldConstructorsRejectInvalidKeys(t *testing.T) {
	constructors := []struct {
		name      string
		construct func(string) (applogging.Field, error)
	}{
		{name: "string", construct: func(key string) (applogging.Field, error) {
			return applogging.String(key, "value")
		}},
		{name: "bool", construct: func(key string) (applogging.Field, error) {
			return applogging.Bool(key, true)
		}},
		{name: "int", construct: func(key string) (applogging.Field, error) {
			return applogging.Int(key, 1)
		}},
		{name: "int64", construct: func(key string) (applogging.Field, error) {
			return applogging.Int64(key, 1)
		}},
		{name: "uint64", construct: func(key string) (applogging.Field, error) {
			return applogging.Uint64(key, 1)
		}},
		{name: "float64", construct: func(key string) (applogging.Field, error) {
			return applogging.Float64(key, 0.5)
		}},
		{name: "any", construct: func(key string) (applogging.Field, error) {
			return applogging.Any(key, map[string]any{"value": true})
		}},
	}
	invalidKeys := []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "ASCII space", key: "user id"},
		{name: "non-breaking space", key: "user\u00a0id"},
		{name: "tab", key: "user\tid"},
		{name: "newline", key: "user\nid"},
		{name: "NUL", key: "user\x00id"},
		{name: "invalid UTF-8", key: string([]byte{'u', 's', 'e', 'r', 0xff})},
	}

	for _, constructor := range constructors {
		for _, invalidKey := range invalidKeys {
			t.Run(constructor.name+"/"+invalidKey.name, func(t *testing.T) {
				field, err := constructor.construct(invalidKey.key)
				if !errors.Is(err, applogging.ErrFieldKeyInvalid) {
					t.Fatalf("constructor error = %v, want ErrFieldKeyInvalid", err)
				}
				if field.IsDefined() {
					t.Fatal("rejected Field IsDefined() = true, want false")
				}
			})
		}
	}
}

func TestFieldConstructorsAcceptPortableKeys(t *testing.T) {
	keys := []string{"request_id", "service.name", "http-status", "ユーザーID"}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			field, err := applogging.String(key, "value")
			if err != nil {
				t.Fatalf("String() error = %v", err)
			}
			if got := field.Key(); got != key {
				t.Fatalf("Key() = %q, want unchanged key %q", got, key)
			}
		})
	}
}

func TestFieldKeyCreatesFieldsWithoutRepeatedKeyErrors(t *testing.T) {
	key, err := applogging.NewFieldKey("service.name")
	if err != nil {
		t.Fatalf("NewFieldKey() error = %v", err)
	}

	tests := []struct {
		name      string
		field     applogging.Field
		wantValue any
	}{
		{name: "string", field: key.String("checkout"), wantValue: "checkout"},
		{name: "bool", field: key.Bool(true), wantValue: true},
		{name: "int", field: key.Int(1), wantValue: int(1)},
		{name: "int64", field: key.Int64(2), wantValue: int64(2)},
		{name: "uint64", field: key.Uint64(3), wantValue: uint64(3)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.field.Key(); got != "service.name" {
				t.Fatalf("Key() = %q, want service.name", got)
			}
			if got := test.field.Value(); !reflect.DeepEqual(got, test.wantValue) {
				t.Fatalf("Value() = %#v, want %#v", got, test.wantValue)
			}
		})
	}

	mustField := applogging.MustFieldKey("region").String("ap-northeast-1")
	if got := mustField.Key(); got != "region" {
		t.Fatalf("MustFieldKey Field Key() = %q, want region", got)
	}
}

func TestFieldKeyValueConversionsRetainValueErrors(t *testing.T) {
	key := applogging.MustFieldKey("value")

	field, err := key.Float64(math.NaN())
	if !errors.Is(err, applogging.ErrFieldFloatNotFinite) {
		t.Fatalf("FieldKey.Float64() error = %v, want ErrFieldFloatNotFinite", err)
	}
	if field.IsDefined() {
		t.Fatal("FieldKey.Float64() Field IsDefined() = true, want false")
	}

	field, err = key.Any(make(chan struct{}))
	if !errors.Is(err, applogging.ErrFieldValueNotJSON) {
		t.Fatalf("FieldKey.Any() error = %v, want ErrFieldValueNotJSON", err)
	}
	if field.IsDefined() {
		t.Fatal("FieldKey.Any() Field IsDefined() = true, want false")
	}
}

func TestInvalidFieldKeyConstructionAndZeroValuePanic(t *testing.T) {
	fieldKey, err := applogging.NewFieldKey("invalid key")
	if !errors.Is(err, applogging.ErrFieldKeyInvalid) {
		t.Fatalf("NewFieldKey() error = %v, want ErrFieldKeyInvalid", err)
	}
	assertFieldKeyPanic(t, "NewFieldKey error result", func() {
		_ = fieldKey.String("value")
	})
	assertFieldKeyPanic(t, "MustFieldKey invalid key", func() {
		_ = applogging.MustFieldKey("invalid key")
	})

	var zero applogging.FieldKey
	assertFieldKeyPanic(t, "zero FieldKey", func() {
		_ = zero.Bool(true)
	})
}

func assertFieldKeyPanic(t *testing.T, name string, call func()) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("%s did not panic", name)
		}
		err, ok := recovered.(error)
		if !ok || !errors.Is(err, applogging.ErrFieldKeyInvalid) {
			t.Fatalf("%s panic = %#v, want ErrFieldKeyInvalid", name, recovered)
		}
	}()
	call()
}

func TestFieldKeyValidationPrecedesValueValidation(t *testing.T) {
	marshalCalled := false
	field, err := applogging.Any("invalid key", trackingJSONMarshaler{called: &marshalCalled})
	if !errors.Is(err, applogging.ErrFieldKeyInvalid) {
		t.Fatalf("Any() error = %v, want ErrFieldKeyInvalid", err)
	}
	if marshalCalled {
		t.Fatal("Any() called MarshalJSON for an invalid key")
	}
	if field.IsDefined() {
		t.Fatal("Any() Field IsDefined() = true, want false")
	}

	field, err = applogging.Float64("invalid key", math.NaN())
	if !errors.Is(err, applogging.ErrFieldKeyInvalid) {
		t.Fatalf("Float64() error = %v, want ErrFieldKeyInvalid", err)
	}
	if errors.Is(err, applogging.ErrFieldFloatNotFinite) {
		t.Fatalf("Float64() error = %v, must validate key before value", err)
	}
	if field.IsDefined() {
		t.Fatal("Float64() Field IsDefined() = true, want false")
	}
}

func TestAnySnapshotsSourceAndEachReturnedValue(t *testing.T) {
	source := map[string]any{
		"labels": []string{"before"},
		"nested": map[string]any{"state": "before"},
	}
	field, err := applogging.Any("context", source)
	if err != nil {
		t.Fatalf("Any() error = %v", err)
	}

	source["labels"].([]string)[0] = "source changed"
	source["nested"].(map[string]any)["state"] = "source changed"
	source["new"] = true

	first, ok := field.Value().(map[string]any)
	if !ok {
		t.Fatalf("Value() type = %T, want map[string]any", field.Value())
	}
	first["labels"].([]any)[0] = "returned value changed"
	first["nested"].(map[string]any)["state"] = "returned value changed"
	first["new"] = true

	want := map[string]any{
		"labels": []any{"before"},
		"nested": map[string]any{"state": "before"},
	}
	if got := field.Value(); !reflect.DeepEqual(got, want) {
		t.Fatalf("second Value() = %#v, want independent snapshot %#v", got, want)
	}
}

func TestAnyRejectsValuesThatJSONCannotRepresent(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle

	tests := []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan struct{})},
		{name: "cyclic map", value: cycle},
		{name: "failing JSON marshaler", value: failingJSONMarshaler{}},
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field, err := applogging.Any("invalid", test.value)
			if !errors.Is(err, applogging.ErrFieldValueNotJSON) {
				t.Fatalf("Any() error = %v, want ErrFieldValueNotJSON", err)
			}
			if field.IsDefined() {
				t.Fatal("rejected Field IsDefined() = true, want false")
			}
		})
	}
}

// nestedArrays returns leaf wrapped in depth nested arrays.
func nestedArrays(depth int, leaf any) any {
	value := leaf
	for range depth {
		value = []any{value}
	}
	return value
}

// nestedObjects returns leaf wrapped in depth nested objects.
func nestedObjects(depth int, leaf any) any {
	value := leaf
	for range depth {
		value = map[string]any{"child": value}
	}
	return value
}

func TestAnyAcceptsNestingUpToMaxFieldNestingDepth(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "scalar", value: "preserved leaf"},
		{name: "one array", value: nestedArrays(1, "preserved leaf")},
		{name: "maximum arrays", value: nestedArrays(applogging.MaxFieldNestingDepth, "preserved leaf")},
		{name: "maximum objects", value: nestedObjects(applogging.MaxFieldNestingDepth, "preserved leaf")},
		// 文字列中の括弧は入れ子として数えません。
		{name: "brackets inside string", value: []any{strings.Repeat("[{", applogging.MaxFieldNestingDepth)}},
		// escapeした引用符・backslashの後も文字列の範囲を正しく追い、その後の入れ子を数えます。
		{name: "escaped quote before maximum nesting", value: []any{`quoted " text`, nestedArrays(applogging.MaxFieldNestingDepth-1, "leaf")}},
		{name: "escaped backslash before maximum nesting", value: []any{`trailing backslash \`, nestedArrays(applogging.MaxFieldNestingDepth-1, "leaf")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("fixture is not JSON-encodable: %v", err)
			}
			field, err := applogging.Any("deep", test.value)
			if err != nil {
				t.Fatalf("supported nesting was rejected: %v", err)
			}
			if !field.IsDefined() {
				t.Fatal("supported nesting produced an undefined Field")
			}
			restored, err := json.Marshal(field.Value())
			if err != nil {
				t.Fatalf("marshal restored snapshot: %v", err)
			}
			if !bytes.Equal(restored, encoded) {
				t.Fatalf("accepted snapshot changed: encoded %d bytes, restored %d bytes", len(encoded), len(restored))
			}
		})
	}
}

func TestAnyRejectsNestingBeyondMaxFieldNestingDepth(t *testing.T) {
	// 上限を超えた値はAnyのerrorだけを確認し、JSON encoderの上限や挙動には依存しません。
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "arrays one level too deep", value: nestedArrays(applogging.MaxFieldNestingDepth+1, "leaf")},
		{name: "objects one level too deep", value: nestedObjects(applogging.MaxFieldNestingDepth+1, "leaf")},
		{name: "beyond JSON decoder depth", value: nestedArrays(10001, "leaf")},
		// 文字列中のescapeを読み違えると、その後の入れ子を文字列の一部とみなして見逃します。
		{name: "escaped quote before excessive nesting", value: []any{`quoted " text`, nestedArrays(applogging.MaxFieldNestingDepth, "leaf")}},
		{name: "escaped backslash before excessive nesting", value: []any{`trailing backslash \`, nestedArrays(applogging.MaxFieldNestingDepth, "leaf")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			field, err := applogging.Any("deep", test.value)
			if !errors.Is(err, applogging.ErrFieldValueNotJSON) {
				t.Fatalf("Any() error = %v, want ErrFieldValueNotJSON", err)
			}
			if field.IsDefined() {
				t.Fatal("rejected nesting produced a defined Field")
			}
		})
	}
}

func TestSlogFieldSharesMaxFieldNestingDepth(t *testing.T) {
	field, err := applogging.NewSlogField("deep", nestedArrays(applogging.MaxFieldNestingDepth, "slog leaf"))
	if err != nil || !field.IsDefined() {
		t.Fatalf("NewSlogField() at the maximum depth = (%v, %v), want a defined Field", field.IsDefined(), err)
	}
	field, err = applogging.NewSlogField("deep", nestedArrays(applogging.MaxFieldNestingDepth+1, "slog leaf"))
	if !errors.Is(err, applogging.ErrFieldValueNotJSON) || field.IsDefined() {
		t.Fatalf("NewSlogField() beyond the maximum depth = (%v, %v), want ErrFieldValueNotJSON", field.IsDefined(), err)
	}
}

func TestFloat64RejectsNonFiniteValues(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field, err := applogging.Float64("invalid", test.value)
			if !errors.Is(err, applogging.ErrFieldFloatNotFinite) {
				t.Fatalf("Float64() error = %v, want ErrFieldFloatNotFinite", err)
			}
			if field.IsDefined() {
				t.Fatal("rejected Field IsDefined() = true, want false")
			}
		})
	}
}

func TestFieldZeroValueIsUndefined(t *testing.T) {
	var field applogging.Field
	if field.IsDefined() {
		t.Fatal("zero Field IsDefined() = true, want false")
	}
	if got := field.Key(); got != "" {
		t.Fatalf("zero Field Key() = %q, want empty", got)
	}
	if got := field.Value(); got != nil {
		t.Fatalf("zero Field Value() = %#v, want nil", got)
	}
}

type callerMarshalError struct{}

func (*callerMarshalError) Error() string { return "caller marshal failed" }

type valueWithMarshalError struct{ err error }

func (v valueWithMarshalError) MarshalJSON() ([]byte, error) { return nil, v.err }

// JSON化のerrorは記録しようとした値やMarshalJSONが返したerrorを保持し得るため、Anyと
// NewSlogFieldは原因として辿れる形では返さず、ErrFieldValueNotJSONだけで判定させます。
func TestFieldValueErrorsDoNotExposeCallerValueAsCause(t *testing.T) {
	callerErr := &callerMarshalError{}
	for _, value := range []struct {
		name  string
		value any
	}{
		{name: "MarshalJSON error", value: valueWithMarshalError{err: callerErr}},
		{name: "NaN", value: map[string]any{"value": math.NaN()}},
	} {
		for _, constructor := range []struct {
			name      string
			construct func(any) (applogging.Field, error)
		}{
			{name: "Any", construct: func(v any) (applogging.Field, error) { return applogging.Any("value", v) }},
			{name: "NewSlogField", construct: func(v any) (applogging.Field, error) { return applogging.NewSlogField("value", v) }},
		} {
			t.Run(value.name+"/"+constructor.name, func(t *testing.T) {
				_, err := constructor.construct(value.value)
				if !errors.Is(err, applogging.ErrFieldValueNotJSON) {
					t.Fatalf("%s() error = %v, want ErrFieldValueNotJSON", constructor.name, err)
				}
				if errors.Is(err, callerErr) {
					t.Fatalf("%s() error exposes the error returned by MarshalJSON: %v", constructor.name, err)
				}
				var marshalerErr *json.MarshalerError
				var unsupportedErr *json.UnsupportedValueError
				if errors.As(err, &marshalerErr) || errors.As(err, &unsupportedErr) {
					t.Fatalf("%s() error exposes the JSON encoding error with the caller value: %v", constructor.name, err)
				}
			})
		}
	}
}
