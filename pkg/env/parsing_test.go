package env

import (
	"errors"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 不正な明示値でも、各関数は未設定時と同じdefaultをerrorとともに返します。
func TestGetEnvFunctionsReturnExplicitValueErrors(t *testing.T) {
	t.Setenv("TEST_ENV_INVALID_INT", "not-an-int")
	t.Setenv("TEST_ENV_INVALID_BOOL", "enabled")
	t.Setenv("TEST_ENV_INVALID_DURATION", "forever")
	t.Setenv("TEST_ENV_INVALID_JSON", "{")
	t.Setenv("TEST_ENV_INVALID_FLOAT", "NaN")

	intValue, intErr := GetEnvInt("TEST_ENV_INVALID_INT", 7)
	boolValue, boolErr := GetEnvBool("TEST_ENV_INVALID_BOOL", false)
	durationValue, durationErr := GetEnvDuration("TEST_ENV_INVALID_DURATION", time.Second)
	jsonValue, jsonErr := GetEnvJSON[map[string]string]("TEST_ENV_INVALID_JSON", nil, false)
	floatValue, floatErr := GetEnvFloat64("TEST_ENV_INVALID_FLOAT", 0.5)

	if intValue != 7 || boolValue || durationValue != time.Second || jsonValue != nil || floatValue != 0.5 {
		t.Fatalf(
			"invalid values returned int=%d bool=%t duration=%s json=%v float=%f",
			intValue,
			boolValue,
			durationValue,
			jsonValue,
			floatValue,
		)
	}
	for _, result := range []struct {
		err   error
		key   string
		value string
		kind  error
	}{
		{err: intErr, key: "TEST_ENV_INVALID_INT", value: "not-an-int", kind: ErrNotInteger},
		{err: boolErr, key: "TEST_ENV_INVALID_BOOL", value: "enabled", kind: ErrNotBoolean},
		{err: durationErr, key: "TEST_ENV_INVALID_DURATION", value: "forever", kind: ErrNotInteger},
		{err: jsonErr, key: "TEST_ENV_INVALID_JSON", value: "{", kind: ErrInvalidJSON},
		{err: floatErr, key: "TEST_ENV_INVALID_FLOAT", value: "NaN", kind: ErrNotFinite},
	} {
		assertVariableError(t, result.err, result.key, result.value, result.kind)
	}
	// parseの原因もerror chainに残ります。
	if !errors.Is(intErr, strconv.ErrSyntax) {
		t.Fatalf("GetEnvInt() error = %v, want strconv.ErrSyntax cause", intErr)
	}
}

func TestGetEnvFunctionsUseDefaultsForMissingAndEmptyValues(t *testing.T) {
	t.Setenv("TEST_ENV_EMPTY_INT", "")
	t.Setenv("TEST_ENV_EMPTY_BOOL", "")

	missingInt, missingIntErr := GetEnvInt("TEST_ENV_MISSING_INT", 7)
	emptyInt, emptyIntErr := GetEnvInt("TEST_ENV_EMPTY_INT", 8)
	emptyBool, emptyBoolErr := GetEnvBool("TEST_ENV_EMPTY_BOOL", true)
	if missingInt != 7 || emptyInt != 8 || !emptyBool {
		t.Fatalf("defaults returned missingInt=%d emptyInt=%d emptyBool=%t", missingInt, emptyInt, emptyBool)
	}
	if missingIntErr != nil || emptyIntErr != nil || emptyBoolErr != nil {
		t.Fatalf(
			"default errors = missing:%v emptyInt:%v emptyBool:%v, want nil",
			missingIntErr,
			emptyIntErr,
			emptyBoolErr,
		)
	}
}

func TestGetEnvDurationRejectsOverflow(t *testing.T) {
	t.Setenv("TEST_ENV_DURATION_OVERFLOW", "9223372036854775807")

	_, err := GetEnvDuration("TEST_ENV_DURATION_OVERFLOW", time.Second)
	assertVariableError(t, err, "TEST_ENV_DURATION_OVERFLOW", "9223372036854775807", ErrOutOfRange)
}

func TestGetEnvDurationUsesFunctionSpecificUnit(t *testing.T) {
	const key = "TEST_ENV_DURATION_UNIT"
	t.Setenv(key, "2")

	tests := []struct {
		name string
		load func(string, time.Duration) (time.Duration, error)
		want time.Duration
	}{
		{name: "seconds", load: GetEnvDuration, want: 2 * time.Second},
		{name: "minutes", load: GetEnvDurationMinute, want: 2 * time.Minute},
		{name: "milliseconds", load: GetEnvDurationMillisecond, want: 2 * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.load(key, time.Hour)
			if err != nil {
				t.Fatalf("load duration: %v", err)
			}
			if got != test.want {
				t.Fatalf("duration = %s, want %s", got, test.want)
			}
		})
	}
}

func TestGetEnvDurationSliceRejectsInvalidValue(t *testing.T) {
	defaultValue := []time.Duration{time.Minute}
	t.Setenv("TEST_ENV_DURATION_SLICE", "1,invalid")

	got, err := GetEnvDurationSlice("TEST_ENV_DURATION_SLICE", defaultValue, time.Minute)
	if len(got) != 1 || got[0] != defaultValue[0] {
		t.Fatalf("GetEnvDurationSlice() = %v, want default %v", got, defaultValue)
	}
	assertVariableError(t, err, "TEST_ENV_DURATION_SLICE", "1,invalid", ErrNotInteger)
}

func TestGetEnvDurationSliceAcceptsSignedTrimmedValues(t *testing.T) {
	t.Setenv("TEST_ENV_SIGNED_DURATION_SLICE", " -2, +3 , 0 ")

	got, err := GetEnvDurationSlice("TEST_ENV_SIGNED_DURATION_SLICE", nil, time.Minute)
	want := []time.Duration{-2 * time.Minute, 3 * time.Minute, 0}
	if err != nil {
		t.Fatalf("GetEnvDurationSlice() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("GetEnvDurationSlice() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("GetEnvDurationSlice()[%d] = %s, want %s", index, got[index], want[index])
		}
	}
}

func TestGetEnvUint64ScaledRejectsMultiplicationOverflow(t *testing.T) {
	t.Setenv("TEST_ENV_UINT64_SCALE_OVERFLOW", "18446744073709551615")

	// 明示値の乗算errorでも、未設定時と同じ倍率適用済みのdefaultを返します。
	got, err := GetEnvUint64Scaled("TEST_ENV_UINT64_SCALE_OVERFLOW", 1, 1024)
	if got != 1024 {
		t.Fatalf("GetEnvUint64Scaled() = %d, want scaled default 1024 after overflow", got)
	}
	assertVariableError(t, err, "TEST_ENV_UINT64_SCALE_OVERFLOW", "18446744073709551615", ErrOutOfRange)
}

func TestGetEnvUint64ScaledAcceptsLargestSafeValue(t *testing.T) {
	const multiplier = uint64(1024)
	want := uint64(math.MaxUint64 / multiplier)
	t.Setenv("TEST_ENV_UINT64_SCALE_MAX", "18014398509481983")

	got, err := GetEnvUint64Scaled("TEST_ENV_UINT64_SCALE_MAX", 1, multiplier)
	if err != nil {
		t.Fatalf("GetEnvUint64Scaled() error = %v", err)
	}
	if got != want*multiplier {
		t.Fatalf("GetEnvUint64Scaled() = %d, want %d", got, want*multiplier)
	}
}

func TestGetEnvUint64ScaledUsesScaledDefaultForMissingAndEmptyValues(t *testing.T) {
	for _, tt := range []struct {
		name  string
		empty bool
	}{
		{name: "missing"},
		{name: "empty", empty: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_ENV_UINT64_SCALE_DEFAULT_" + strings.ToUpper(tt.name)
			if tt.empty {
				t.Setenv(key, "")
			} else {
				unsetEnvForTest(t, key)
			}

			got, err := GetEnvUint64Scaled(key, 7, 3)
			if err != nil {
				t.Fatalf("GetEnvUint64Scaled() error = %v", err)
			}
			if got != 21 {
				t.Fatalf("GetEnvUint64Scaled() = %d, want scaled default 21", got)
			}
		})
	}
}

func TestGetEnvUint64ScaledReturnsOnlyParseErrorForInvalidValue(t *testing.T) {
	const key = "TEST_ENV_UINT64_SCALE_INVALID"
	t.Setenv(key, "not-an-unsigned-integer")

	for _, tt := range []struct {
		name         string
		defaultValue uint64
		want         uint64
	}{
		// 値は未設定時と同じ、倍率を適用したdefaultです。
		{name: "representable default", defaultValue: 7, want: 21},
		// defaultの乗算もuint64を超える場合、未設定時と同じく0ですが、errorは明示値の
		// parse errorだけで、defaultの乗算errorは含めません。
		{name: "overflowing default", defaultValue: math.MaxUint64, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetEnvUint64Scaled(key, tt.defaultValue, 3)
			if got != tt.want {
				t.Fatalf("GetEnvUint64Scaled() = %d, want unset value %d", got, tt.want)
			}
			assertVariableError(t, err, key, "not-an-unsigned-integer", ErrNotInteger)
			if errors.Is(err, ErrOutOfRange) {
				t.Fatalf("GetEnvUint64Scaled() error = %v, contains unrelated scale error", err)
			}
		})
	}
}

func TestGetEnvUint64ScaledRejectsOverflowingDefault(t *testing.T) {
	for _, tt := range []struct {
		name  string
		empty bool
	}{
		{name: "missing"},
		{name: "empty", empty: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_ENV_UINT64_SCALE_OVERFLOWING_DEFAULT_" + strings.ToUpper(tt.name)
			if tt.empty {
				t.Setenv(key, "")
			} else {
				unsetEnvForTest(t, key)
			}

			got, err := GetEnvUint64Scaled(key, math.MaxUint64, 2)
			if got != 0 {
				t.Fatalf("GetEnvUint64Scaled() = %d, want 0 for overflowing default", got)
			}
			// defaultの乗算errorは環境変数の値を持ちません。
			assertVariableError(t, err, key, "", ErrOutOfRange)
		})
	}
}

func unsetEnvForTest(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("os.Unsetenv(%q): %v", key, err)
	}
}

// どのGetEnv*とParseEnvも、不正な明示値では未設定時と同じ値をerrorとともに返します。
// 呼び出し側はerrorを設定errorとして扱い、その値を成功値として使いません。
func TestGetEnvFunctionsReturnUnsetValueWithError(t *testing.T) {
	const key = "TEST_ENV_UNSET_VALUE_ON_ERROR"
	tests := []struct {
		name    string
		invalid []string
		load    func() (any, error)
	}{
		{"ParseEnv", []string{"x"}, func() (any, error) { return ParseEnv(key, 7, strconv.Atoi) }},
		{"GetEnvDuration", []string{"forever", "9223372036854775807"}, func() (any, error) {
			return GetEnvDuration(key, time.Second)
		}},
		{"GetEnvDurationMinute", []string{"soon", "9223372036854775807"}, func() (any, error) {
			return GetEnvDurationMinute(key, time.Minute)
		}},
		{"GetEnvDurationMillisecond", []string{"soon", "9223372036855"}, func() (any, error) {
			return GetEnvDurationMillisecond(key, time.Millisecond)
		}},
		{"GetEnvDurationSlice", []string{"1,invalid", ",", "9223372036854775807"}, func() (any, error) {
			return GetEnvDurationSlice(key, []time.Duration{time.Minute}, time.Minute)
		}},
		{"GetEnvBool", []string{"enabled"}, func() (any, error) { return GetEnvBool(key, true) }},
		{"GetEnvInt", []string{"one"}, func() (any, error) { return GetEnvInt(key, 7) }},
		{"GetEnvPositiveInt", []string{"0", "-1", "one"}, func() (any, error) { return GetEnvPositiveInt(key, 7) }},
		{"GetEnvPositiveMilliseconds", []string{"0", "-1", "soon", "9223372036855"}, func() (any, error) {
			return GetEnvPositiveMilliseconds(key, time.Second)
		}},
		{"GetEnvUint64", []string{"-1"}, func() (any, error) { return GetEnvUint64(key, 7) }},
		{"GetEnvUint64Scaled", []string{"x", "18446744073709551615"}, func() (any, error) {
			return GetEnvUint64Scaled(key, 7, 1024)
		}},
		{"GetEnvJSON", []string{"{"}, func() (any, error) {
			return GetEnvJSON(key, map[string]string{"source": "default"}, false)
		}},
		{"GetEnvJSON strict", []string{`{"Unknown":1}`, `{} {}`}, func() (any, error) {
			return GetEnvJSON(key, struct{ Known int }{Known: 7}, true)
		}},
		{"GetEnvFloat64", []string{"NaN", "abc"}, func() (any, error) { return GetEnvFloat64(key, 0.5) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetEnvForTest(t, key)
			unset, err := tt.load()
			if err != nil {
				t.Fatalf("unset load error = %v", err)
			}
			for _, value := range tt.invalid {
				t.Setenv(key, value)
				got, err := tt.load()
				if err == nil {
					t.Fatalf("load(%q) error = nil, want configuration error", value)
				}
				if !reflect.DeepEqual(got, unset) {
					t.Fatalf("load(%q) value = %#v, want unset value %#v", value, got, unset)
				}
			}
		})
	}
}
