// Package env reads the values of environment variables as typed configuration
// values.
//
// The GetEnv* functions that return an error, and ParseEnv, return the value
// for the unset case (the default) when a variable is unset or empty. When an
// explicit value is invalid, they also return the same value as in the unset
// case, together with an error. Callers should treat the error as a
// configuration error and not continue processing with the returned value.
//
// The package functions read the process environment. To read from somewhere
// other than the process environment, such as the values of an env file, use
// the methods of Lookup and ParseEnvFrom and GetEnvJSONFrom, which follow the
// same rules.
package env

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Lookup is a function that returns the value of key and whether key is set.
// It has the same shape as os.LookupEnv. Its methods convert the values that
// the Lookup returns to typed configuration values by the same rules as the
// package functions.
type Lookup func(key string) (value string, ok bool)

// ProcessEnvironment returns a Lookup that reads the process environment.
func ProcessEnvironment() Lookup { return os.LookupEnv }

// valueはkeyの値を返します。未設定の場合は空文字です。
func (l Lookup) value(key string) string {
	value, _ := l(key)
	return value
}

// ParseEnv converts an explicitly set environment value with parse. If the
// variable is unset or empty, it returns defaultValue. On error it also returns
// defaultValue, as when the variable is unset, but callers should treat the
// error as a configuration error.
func ParseEnv[T any](key string, defaultValue T, parse func(string) (T, error)) (T, error) {
	return ParseEnvFrom(ProcessEnvironment(), key, defaultValue, parse)
}

// ParseEnvFrom converts the value that lookup returns by the same rules as
// ParseEnv.
func ParseEnvFrom[T any](lookup Lookup, key string, defaultValue T, parse func(string) (T, error)) (T, error) {
	raw := lookup.value(key)
	if raw == "" {
		return defaultValue, nil
	}

	value, err := parse(raw)
	if err != nil {
		return defaultValue, newVariableError(
			key, raw, ErrInvalidValue, err,
			"environment variable %q has invalid value %q: %v",
			key,
			raw,
			err,
		)
	}
	return value, nil
}

// GetEnvDuration reads an integer value from the environment as seconds. If
// the variable is unset or empty, it returns defaultValue. On error it also
// returns defaultValue, as when the variable is unset, but callers should treat
// the error as a configuration error.
func GetEnvDuration(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvDuration(key, defaultValue)
}

// GetEnvDuration reads the value that l returns by the same rules as the
// package function GetEnvDuration.
func (l Lookup) GetEnvDuration(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return l.getEnvDuration(key, defaultValue, time.Second)
}

// GetEnvDurationMinute reads an integer value from the environment as minutes.
// If the variable is unset or empty, it returns defaultValue. On error it also
// returns defaultValue, as when the variable is unset, but callers should treat
// the error as a configuration error.
func GetEnvDurationMinute(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvDurationMinute(key, defaultValue)
}

// GetEnvDurationMinute reads the value that l returns by the same rules as the
// package function GetEnvDurationMinute.
func (l Lookup) GetEnvDurationMinute(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return l.getEnvDuration(key, defaultValue, time.Minute)
}

// GetEnvDurationMillisecond reads an integer value from the environment as
// milliseconds. If the variable is unset or empty, it returns defaultValue. On
// error it also returns defaultValue, as when the variable is unset, but
// callers should treat the error as a configuration error.
func GetEnvDurationMillisecond(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvDurationMillisecond(key, defaultValue)
}

// GetEnvDurationMillisecond reads the value that l returns by the same rules as
// the package function GetEnvDurationMillisecond.
func (l Lookup) GetEnvDurationMillisecond(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return l.getEnvDuration(key, defaultValue, time.Millisecond)
}

// getEnvDurationは単位別の公開関数で確定したunitを使用し、整数のparse、
// time.Duration範囲の検証、単位変換を共通実装します。
func (l Lookup) getEnvDuration(
	key string,
	defaultValue time.Duration,
	unit time.Duration,
) (time.Duration, error) {
	raw := l.value(key)
	if raw == "" {
		return defaultValue, nil
	}
	if unit <= 0 {
		return defaultValue, newVariableError(
			key, raw, ErrInvalidDurationUnit, nil,
			"duration unit for environment variable %q must be positive: %s",
			key,
			unit,
		)
	}
	unitLabel := durationUnitLabel(unit)

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultValue, newVariableError(
			key, raw, ErrNotInteger, err,
			"environment variable %q must be an integer number of %s: %q: %v",
			key,
			unitLabel,
			raw,
			err,
		)
	}
	const (
		maxDuration = time.Duration(1<<63 - 1)
		minDuration = -maxDuration - 1
	)
	if value > int64(maxDuration/unit) || value < int64(minDuration/unit) {
		return defaultValue, newVariableError(
			key, raw, ErrOutOfRange, nil,
			"environment variable %q %s exceed time.Duration range: %d",
			key,
			unitLabel,
			value,
		)
	}
	return time.Duration(value) * unit, nil
}

// GetEnvDurationSlice converts comma-separated integers to durations in units
// of unit. If the variable is unset or empty, it returns defaultValue. On error
// it also returns defaultValue, as when the variable is unset, but callers
// should treat the error as a configuration error.
func GetEnvDurationSlice(
	key string,
	defaultValue []time.Duration,
	unit time.Duration,
) ([]time.Duration, error) {
	return ProcessEnvironment().GetEnvDurationSlice(key, defaultValue, unit)
}

// GetEnvDurationSlice reads the value that l returns by the same rules as the
// package function GetEnvDurationSlice.
func (l Lookup) GetEnvDurationSlice(
	key string,
	defaultValue []time.Duration,
	unit time.Duration,
) ([]time.Duration, error) {
	raw := l.value(key)
	if raw == "" {
		return defaultValue, nil
	}
	if unit <= 0 {
		return defaultValue, newVariableError(
			key, raw, ErrInvalidDurationUnit, nil,
			"duration unit for environment variable %q must be positive: %s",
			key,
			unit,
		)
	}
	unitLabel := durationUnitLabel(unit)

	values, err := parseCommaSeparatedInts(raw)
	if err != nil {
		return defaultValue, newVariableError(
			key, raw, ErrNotInteger, err,
			"environment variable %q must contain comma-separated integer %s: %q: %v",
			key,
			unitLabel,
			raw,
			err,
		)
	}

	durations := make([]time.Duration, len(values))
	for index, value := range values {
		const (
			maxDuration = time.Duration(1<<63 - 1)
			minDuration = -maxDuration - 1
		)
		if int64(value) > int64(maxDuration/unit) ||
			int64(value) < int64(minDuration/unit) {
			return defaultValue, newVariableError(
				key, raw, ErrOutOfRange, nil,
				"environment variable %q %s value at index %d exceeds time.Duration range: %d",
				key,
				unitLabel,
				index,
				value,
			)
		}
		durations[index] = time.Duration(value) * unit
	}
	return durations, nil
}

func durationUnitLabel(unit time.Duration) string {
	switch unit {
	case time.Second:
		return "seconds"
	case time.Minute:
		return "minutes"
	case time.Millisecond:
		return "milliseconds"
	default:
		return unit.String() + " units"
	}
}

// GetEnvBool reads a boolean value from the environment. If the variable is
// unset or empty, it returns defaultValue. On error it also returns
// defaultValue, as when the variable is unset, but callers should treat the
// error as a configuration error.
func GetEnvBool(key string, defaultValue bool) (bool, error) {
	return ProcessEnvironment().GetEnvBool(key, defaultValue)
}

// GetEnvBool reads the value that l returns by the same rules as the package
// function GetEnvBool.
func (l Lookup) GetEnvBool(key string, defaultValue bool) (bool, error) {
	val, exists := l(key)
	if !exists || val == "" {
		return defaultValue, nil
	}
	value, err := ParseBool(val)
	if err != nil {
		return defaultValue, newVariableError(key, val, ErrNotBoolean, err, "environment variable %q %v", key, err)
	}
	return value, nil
}

// ParseBool parses a boolean value that has already been read. It does not
// look up environment variables. It accepts true/1/yes/on and false/0/no/off.
// Callers handle defaults and inheritance for an empty string.
func ParseBool(val string) (bool, error) {
	switch val {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, newVariableError("", val, ErrNotBoolean, nil, "must be a boolean value: %q", val)
	}
}

// GetEnvInt reads an integer value from the environment. If the variable is
// unset or empty, it returns defaultValue. On error it also returns
// defaultValue, as when the variable is unset, but callers should treat the
// error as a configuration error.
func GetEnvInt(key string, defaultValue int) (int, error) {
	return ProcessEnvironment().GetEnvInt(key, defaultValue)
}

// GetEnvInt reads the value that l returns by the same rules as the package
// function GetEnvInt.
func (l Lookup) GetEnvInt(key string, defaultValue int) (int, error) {
	envVal := l.value(key)
	if envVal == "" {
		return defaultValue, nil
	}
	val, err := strconv.Atoi(envVal)
	if err != nil {
		return defaultValue, newVariableError(
			key, envVal, ErrNotInteger, err,
			"environment variable %q must be an integer: %q: %v",
			key,
			envVal,
			err,
		)
	}
	return val, nil
}

// GetEnvPositiveInt reads a positive integer from the environment. If key is
// unset or empty, it returns defaultValue. If an explicit value is not an
// integer, or is zero or less, it returns a configuration error instead of
// replacing the value with the default and succeeding. On error it also
// returns defaultValue, as when the variable is unset, but callers should treat
// the error as a configuration error.
func GetEnvPositiveInt(key string, defaultValue int) (int, error) {
	return ProcessEnvironment().GetEnvPositiveInt(key, defaultValue)
}

// GetEnvPositiveInt reads the value that l returns by the same rules as the
// package function GetEnvPositiveInt.
func (l Lookup) GetEnvPositiveInt(key string, defaultValue int) (int, error) {
	raw := l.value(key)
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue, newVariableError(
			key, raw, ErrNotInteger, err,
			"environment variable %q must be an integer: %q: %v",
			key,
			raw,
			err,
		)
	}
	if value <= 0 {
		return defaultValue, newVariableError(
			key, raw, ErrNotPositive, nil,
			"environment variable %q must be greater than zero: %d",
			key,
			value,
		)
	}
	return value, nil
}

// GetEnvPositiveMilliseconds reads a positive integer from the environment as
// milliseconds and converts it to a time.Duration. If key is unset or empty, it
// returns defaultValue. If an explicit value is not an integer, is zero or
// less, or cannot be converted to a time.Duration safely, it returns a
// configuration error instead of replacing the value with the default and
// succeeding. On error it also returns defaultValue, as when the variable is
// unset, but callers should treat the error as a configuration error.
func GetEnvPositiveMilliseconds(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvPositiveMilliseconds(key, defaultValue)
}

// GetEnvPositiveMilliseconds reads the value that l returns by the same rules
// as the package function GetEnvPositiveMilliseconds.
func (l Lookup) GetEnvPositiveMilliseconds(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	raw := l.value(key)
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultValue, newVariableError(
			key, raw, ErrNotInteger, err,
			"environment variable %q must be an integer number of milliseconds: %q: %v",
			key,
			raw,
			err,
		)
	}
	if value <= 0 {
		return defaultValue, newVariableError(
			key, raw, ErrNotPositive, nil,
			"environment variable %q must be greater than zero: %d",
			key,
			value,
		)
	}

	maxMilliseconds := int64(time.Duration(1<<63-1) / time.Millisecond)
	if value > maxMilliseconds {
		return defaultValue, newVariableError(
			key, raw, ErrOutOfRange, nil,
			"environment variable %q milliseconds exceed time.Duration range: %d",
			key,
			value,
		)
	}
	return time.Duration(value) * time.Millisecond, nil
}

// GetEnvUint64 reads an unsigned integer value from the environment. If the
// variable is unset or empty, it returns defaultValue. On error it also returns
// defaultValue, as when the variable is unset, but callers should treat the
// error as a configuration error.
func GetEnvUint64(key string, defaultValue uint64) (uint64, error) {
	return ProcessEnvironment().GetEnvUint64(key, defaultValue)
}

// GetEnvUint64 reads the value that l returns by the same rules as the package
// function GetEnvUint64.
func (l Lookup) GetEnvUint64(key string, defaultValue uint64) (uint64, error) {
	envVal := l.value(key)
	if envVal == "" {
		return defaultValue, nil
	}
	val, err := strconv.ParseUint(envVal, 10, 64)
	if err != nil {
		return defaultValue, newVariableError(
			key, envVal, ErrNotInteger, err,
			"environment variable %q must be an unsigned integer: %q: %v",
			key,
			envVal,
			err,
		)
	}
	return val, nil
}

// GetEnvUint64Scaled reads an unsigned integer value from the environment and
// returns it multiplied by multiplier. If the variable is unset or empty, it
// uses defaultValue, and it returns an error if the multiplication exceeds the
// range of uint64. On error it also returns the same value as when the variable
// is unset, that is, defaultValue multiplied by multiplier. If multiplying the
// default itself exceeds the range of uint64, that value is 0, as when the
// variable is unset. Callers should treat the error as a configuration error.
func GetEnvUint64Scaled(
	key string,
	defaultValue uint64,
	multiplier uint64,
) (uint64, error) {
	return ProcessEnvironment().GetEnvUint64Scaled(key, defaultValue, multiplier)
}

// GetEnvUint64Scaled reads the value that l returns by the same rules as the
// package function GetEnvUint64Scaled.
func (l Lookup) GetEnvUint64Scaled(
	key string,
	defaultValue uint64,
	multiplier uint64,
) (uint64, error) {
	raw := l.value(key)
	if raw == "" {
		return scaleEnvUint64(key, "", defaultValue, multiplier)
	}
	// 明示値のerrorでも未設定時と同じ値を返します。defaultの乗算errorは明示値の
	// errorとは別の原因なので、ここでは返しません。
	unsetValue, _ := scaleEnvUint64(key, "", defaultValue, multiplier)
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return unsetValue, newVariableError(
			key, raw, ErrNotInteger, err,
			"environment variable %q must be an unsigned integer: %q: %v",
			key,
			raw,
			err,
		)
	}
	scaled, err := scaleEnvUint64(key, raw, parsed, multiplier)
	if err != nil {
		return unsetValue, err
	}
	return scaled, nil
}

// scaleEnvUint64はvalueをmultiplier倍します。uint64の範囲を超える場合は0とerrorを返します。
// rawは値の出所となった環境変数の値で、defaultを変換する場合は空です。
func scaleEnvUint64(key, raw string, value uint64, multiplier uint64) (uint64, error) {
	if multiplier != 0 && value > math.MaxUint64/multiplier {
		return 0, newVariableError(
			key, raw, ErrOutOfRange, nil,
			"value %d for environment variable %q multiplied by %d exceeds uint64 range",
			value,
			key,
			multiplier,
		)
	}
	return value * multiplier, nil
}

// GetEnvString returns the value of an environment variable. If the variable
// is unset or empty, it returns defaultValue.
func GetEnvString(key, defaultValue string) string {
	return ProcessEnvironment().GetEnvString(key, defaultValue)
}

// GetEnvString reads the value that l returns by the same rules as the package
// function GetEnvString.
func (l Lookup) GetEnvString(key, defaultValue string) string {
	envVal := l.value(key)
	if envVal == "" {
		return defaultValue
	}
	return envVal
}

// GetEnvSliceString splits an environment variable at commas and returns the
// elements with leading and trailing whitespace removed. If key is unset,
// empty or only whitespace, it returns defaultSlice as is.
//
// Whether empty elements are allowed or rejected depends on the configuration
// contract of the caller, so this function neither removes nor validates them.
func GetEnvSliceString(key string, defaultSlice []string) []string {
	return ProcessEnvironment().GetEnvSliceString(key, defaultSlice)
}

// GetEnvSliceString reads the value that l returns by the same rules as the
// package function GetEnvSliceString.
func (l Lookup) GetEnvSliceString(key string, defaultSlice []string) []string {
	envVal := strings.TrimSpace(l.value(key))
	if envVal == "" {
		return defaultSlice
	}

	data := strings.Split(envVal, ",")
	for index := range data {
		data[index] = strings.TrimSpace(data[index])
	}
	return data
}

func parseCommaSeparatedInts(s string) ([]int, error) {
	if strings.TrimSpace(s) == "" {
		return nil, errors.New("input string is empty")
	}

	parts := strings.Split(s, ",")
	result := make([]int, 0, len(parts))

	for _, rawPart := range parts {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			return nil, errors.New("empty value found between commas")
		}

		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %q to int: %w", part, err)
		}
		result = append(result, n)
	}

	return result, nil
}

// GetEnvJSON converts JSON in the environment to a typed value. If the
// variable is unset or empty, it returns defaultValue. On error it also returns
// defaultValue, as when the variable is unset, but callers should treat the
// error as a configuration error.
func GetEnvJSON[T any](
	key string,
	defaultValue T,
	strictUnknownFields bool,
) (T, error) {
	return GetEnvJSONFrom(ProcessEnvironment(), key, defaultValue, strictUnknownFields)
}

// GetEnvJSONFrom converts the value that lookup returns by the same rules as
// GetEnvJSON.
func GetEnvJSONFrom[T any](
	lookup Lookup,
	key string,
	defaultValue T,
	strictUnknownFields bool,
) (T, error) {
	envVal := lookup.value(key)
	if envVal == "" {
		return defaultValue, nil
	}

	var out T

	if strictUnknownFields {
		dec := json.NewDecoder(strings.NewReader(envVal))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&out); err != nil {
			return defaultValue, newVariableError(
				key, envVal, ErrInvalidJSON, err,
				"environment variable %q must contain valid JSON: %v",
				key,
				err,
			)
		}
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			return defaultValue, newVariableError(
				key, envVal, ErrInvalidJSON, nil,
				"environment variable %q must contain one JSON value without trailing tokens",
				key,
			)
		}
		return out, nil
	}

	if err := json.Unmarshal([]byte(envVal), &out); err != nil {
		return defaultValue, newVariableError(
			key, envVal, ErrInvalidJSON, err,
			"environment variable %q must contain valid JSON: %v",
			key,
			err,
		)
	}

	return out, nil
}

// GetEnvFloat64 reads a finite floating-point value from the environment. If
// the variable is unset or empty, it returns defaultValue. On error it also
// returns defaultValue, as when the variable is unset, but callers should treat
// the error as a configuration error.
func GetEnvFloat64(key string, defaultValue float64) (float64, error) {
	return ProcessEnvironment().GetEnvFloat64(key, defaultValue)
}

// GetEnvFloat64 reads the value that l returns by the same rules as the
// package function GetEnvFloat64.
func (l Lookup) GetEnvFloat64(key string, defaultValue float64) (float64, error) {
	envVal := l.value(key)
	if envVal == "" {
		return defaultValue, nil
	}

	val, err := strconv.ParseFloat(envVal, 64)
	if err != nil {
		return defaultValue, newVariableError(
			key, envVal, ErrNotNumber, err,
			"environment variable %q must be a number: %q: %v",
			key,
			envVal,
			err,
		)
	}
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return defaultValue, newVariableError(
			key, envVal, ErrNotFinite, nil,
			"environment variable %q must be a finite number: %q",
			key,
			envVal,
		)
	}

	return val, nil
}
