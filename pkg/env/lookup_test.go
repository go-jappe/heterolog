package env

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// mapLookupは、mの値だけを返すLookupです。
func mapLookup(m map[string]string) Lookup {
	return func(key string) (string, bool) {
		value, ok := m[key]
		return value, ok
	}
}

// Lookupのmethodは、process environmentではなくLookupが返す値を、package関数と同じ規則で
// 変換します。process environmentに同じkeyがあっても使いません。
func TestLookupMethodsReadOnlyTheLookup(t *testing.T) {
	for _, key := range []string{"TEST_LOOKUP_STRING", "TEST_LOOKUP_INT", "TEST_LOOKUP_BOOL", "TEST_LOOKUP_DURATION", "TEST_LOOKUP_JSON"} {
		t.Setenv(key, "process-value")
	}
	lookup := mapLookup(map[string]string{
		"TEST_LOOKUP_STRING":   "from-lookup",
		"TEST_LOOKUP_INT":      "42",
		"TEST_LOOKUP_BOOL":     "",
		"TEST_LOOKUP_DURATION": "3",
		"TEST_LOOKUP_JSON":     `{"a":"b"}`,
	})

	if got := lookup.GetEnvString("TEST_LOOKUP_STRING", "default"); got != "from-lookup" {
		t.Errorf("GetEnvString = %q, want the lookup value", got)
	}
	if got, err := lookup.GetEnvInt("TEST_LOOKUP_INT", 1); err != nil || got != 42 {
		t.Errorf("GetEnvInt = %d, %v; want 42, nil", got, err)
	}
	// 空文字は未設定と同じくdefaultです。
	if got, err := lookup.GetEnvBool("TEST_LOOKUP_BOOL", true); err != nil || !got {
		t.Errorf("GetEnvBool(empty) = %t, %v; want the default true", got, err)
	}
	if got, err := lookup.GetEnvDuration("TEST_LOOKUP_DURATION", time.Second); err != nil || got != 3*time.Second {
		t.Errorf("GetEnvDuration = %s, %v; want 3s", got, err)
	}
	if got, err := GetEnvJSONFrom(lookup, "TEST_LOOKUP_JSON", map[string]string(nil), true); err != nil || !reflect.DeepEqual(got, map[string]string{"a": "b"}) {
		t.Errorf("GetEnvJSONFrom = %v, %v; want map[a:b]", got, err)
	}
	if got, err := ParseEnvFrom(lookup, "TEST_LOOKUP_INT", 0, strconv.Atoi); err != nil || got != 42 {
		t.Errorf("ParseEnvFrom = %d, %v; want 42", got, err)
	}
	// Lookupにないkeyは、process environmentに値があっても未設定です。
	if got := mapLookup(nil).GetEnvString("TEST_LOOKUP_STRING", "default"); got != "default" {
		t.Errorf("GetEnvString for a key missing from the lookup = %q, want the default", got)
	}
}

// Lookupの値が不正な場合も、package関数と同じくkeyと値を持つVariableErrorを返します。
func TestLookupMethodsReportInvalidValuesWithTheKey(t *testing.T) {
	lookup := mapLookup(map[string]string{"TEST_LOOKUP_INT": "x"})
	got, err := lookup.GetEnvInt("TEST_LOOKUP_INT", 5)
	var variableErr *VariableError
	if got != 5 || !errors.As(err, &variableErr) || variableErr.Key != "TEST_LOOKUP_INT" || variableErr.Value != "x" || !errors.Is(err, ErrNotInteger) {
		t.Fatalf("GetEnvInt(invalid) = %d, %v; want the default and a VariableError for the key", got, err)
	}
}

// package関数は、ProcessEnvironmentと同じくprocess environmentを読みます。
func TestPackageFunctionsReadTheProcessEnvironment(t *testing.T) {
	t.Setenv("TEST_LOOKUP_PROCESS", "7")
	if got, err := GetEnvInt("TEST_LOOKUP_PROCESS", 1); err != nil || got != 7 {
		t.Errorf("GetEnvInt = %d, %v; want 7", got, err)
	}
	if got, err := ProcessEnvironment().GetEnvInt("TEST_LOOKUP_PROCESS", 1); err != nil || got != 7 {
		t.Errorf("ProcessEnvironment().GetEnvInt = %d, %v; want 7", got, err)
	}
}
