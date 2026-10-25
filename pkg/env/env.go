// Package envは、環境変数の値を型付きの設定値として読み込みます。
//
// errorを返すGetEnv*とParseEnvは、未設定または空文字なら未設定時の値（default）を
// 返します。明示値が不正な場合も、未設定時と同じ値をerrorとともに返します。呼び出し側は
// errorを設定errorとして扱い、返された値で処理を続けないでください。
//
// package関数はprocess environmentを読みます。環境設定fileの値などprocess environment
// 以外から読む場合は、同じ規則のLookupのmethodとParseEnvFrom・GetEnvJSONFromを使います。
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

// Lookupは、keyの値と、keyが設定されているかを返す関数です。os.LookupEnvと同じ形です。
// methodはpackage関数と同じ規則で、Lookupが返す値を型付きの設定値へ変換します。
type Lookup func(key string) (value string, ok bool)

// ProcessEnvironmentは、process environmentを読むLookupを返します。
func ProcessEnvironment() Lookup { return os.LookupEnv }

// valueはkeyの値を返します。未設定の場合は空文字です。
func (l Lookup) value(key string) string {
	value, _ := l(key)
	return value
}

// ParseEnvは明示されたenvironment値をparseで変換します。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func ParseEnv[T any](key string, defaultValue T, parse func(string) (T, error)) (T, error) {
	return ParseEnvFrom(ProcessEnvironment(), key, defaultValue, parse)
}

// ParseEnvFromは、lookupが返す値をParseEnvと同じ規則で変換します。
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

// GetEnvDurationはenvironmentの整数値を秒として読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvDuration(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvDuration(key, defaultValue)
}

// GetEnvDurationは、lookupが返す値をpackage関数GetEnvDurationと同じ規則で読み込みます。
func (l Lookup) GetEnvDuration(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return l.getEnvDuration(key, defaultValue, time.Second)
}

// GetEnvDurationMinuteはenvironmentの整数値を分として読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvDurationMinute(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvDurationMinute(key, defaultValue)
}

// GetEnvDurationMinuteは、lookupが返す値をpackage関数GetEnvDurationMinuteと同じ規則で読み込みます。
func (l Lookup) GetEnvDurationMinute(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return l.getEnvDuration(key, defaultValue, time.Minute)
}

// GetEnvDurationMillisecondはenvironmentの整数値をミリ秒として読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvDurationMillisecond(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvDurationMillisecond(key, defaultValue)
}

// GetEnvDurationMillisecondは、lookupが返す値をpackage関数GetEnvDurationMillisecondと同じ規則で読み込みます。
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

// GetEnvDurationSliceはcomma区切りの整数をunit単位のdurationへ変換します。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvDurationSlice(
	key string,
	defaultValue []time.Duration,
	unit time.Duration,
) ([]time.Duration, error) {
	return ProcessEnvironment().GetEnvDurationSlice(key, defaultValue, unit)
}

// GetEnvDurationSliceは、lookupが返す値をpackage関数GetEnvDurationSliceと同じ規則で読み込みます。
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

// GetEnvBoolはenvironmentのboolean値を読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvBool(key string, defaultValue bool) (bool, error) {
	return ProcessEnvironment().GetEnvBool(key, defaultValue)
}

// GetEnvBoolは、lookupが返す値をpackage関数GetEnvBoolと同じ規則で読み込みます。
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

// ParseBoolは読み込み済みのboolean値を解析します。環境変数は参照しません。
// true/1/yes/onとfalse/0/no/offを受け付けます。空文字の既定値や継承は呼び出し側で扱います。
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

// GetEnvIntはenvironmentの整数値を読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvInt(key string, defaultValue int) (int, error) {
	return ProcessEnvironment().GetEnvInt(key, defaultValue)
}

// GetEnvIntは、lookupが返す値をpackage関数GetEnvIntと同じ規則で読み込みます。
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

// GetEnvPositiveIntはenvironmentから正の整数を読み込みます。
// keyが未設定または空の場合はdefaultValueを返します。明示値が整数でない場合、
// または0以下の場合は、defaultで置き換えて成功とはせず設定errorを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvPositiveInt(key string, defaultValue int) (int, error) {
	return ProcessEnvironment().GetEnvPositiveInt(key, defaultValue)
}

// GetEnvPositiveIntは、lookupが返す値をpackage関数GetEnvPositiveIntと同じ規則で読み込みます。
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

// GetEnvPositiveMillisecondsはenvironmentの正の整数をmillisecondとして読み込み、
// time.Durationへ変換します。keyが未設定または空の場合はdefaultValueを返します。
// 明示値が整数でない場合、0以下の場合、time.Durationへ安全に変換できない場合は、
// defaultで置き換えて成功とはせず設定errorを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvPositiveMilliseconds(
	key string,
	defaultValue time.Duration,
) (time.Duration, error) {
	return ProcessEnvironment().GetEnvPositiveMilliseconds(key, defaultValue)
}

// GetEnvPositiveMillisecondsは、lookupが返す値をpackage関数GetEnvPositiveMillisecondsと同じ規則で
// 読み込みます。
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

// GetEnvUint64はenvironmentの符号なし整数値を読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvUint64(key string, defaultValue uint64) (uint64, error) {
	return ProcessEnvironment().GetEnvUint64(key, defaultValue)
}

// GetEnvUint64は、lookupが返す値をpackage関数GetEnvUint64と同じ規則で読み込みます。
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

// GetEnvUint64Scaledはenvironmentの符号なし整数値をmultiplier倍して返します。
// 未設定または空文字ならdefaultValueを使用し、乗算がuint64の範囲を超える場合はerrorを返します。
// error時も未設定時と同じ値、つまりdefaultValueのmultiplier倍を返します。default自体の
// 乗算がuint64の範囲を超える場合、その値は未設定時と同じく0です。errorは設定errorとして
// 扱ってください。
func GetEnvUint64Scaled(
	key string,
	defaultValue uint64,
	multiplier uint64,
) (uint64, error) {
	return ProcessEnvironment().GetEnvUint64Scaled(key, defaultValue, multiplier)
}

// GetEnvUint64Scaledは、lookupが返す値をpackage関数GetEnvUint64Scaledと同じ規則で読み込みます。
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

// GetEnvStringは環境変数の値を返します。未設定または空文字ならdefaultValueを返します。
func GetEnvString(key, defaultValue string) string {
	return ProcessEnvironment().GetEnvString(key, defaultValue)
}

// GetEnvStringは、lookupが返す値をpackage関数GetEnvStringと同じ規則で読み込みます。
func (l Lookup) GetEnvString(key, defaultValue string) string {
	envVal := l.value(key)
	if envVal == "" {
		return defaultValue
	}
	return envVal
}

// GetEnvSliceStringは、環境変数をcommaで分割し、各要素の前後の空白を除いて返します。
// keyが未設定、空文字、空白だけの場合はdefaultSliceをそのまま返します。
//
// 空要素の許可・拒否は利用側の設定契約に依存するため、この関数では除去も検証も
// 行いません。
func GetEnvSliceString(key string, defaultSlice []string) []string {
	return ProcessEnvironment().GetEnvSliceString(key, defaultSlice)
}

// GetEnvSliceStringは、lookupが返す値をpackage関数GetEnvSliceStringと同じ規則で読み込みます。
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

// GetEnvJSONはenvironmentのJSONを型付き値へ変換します。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvJSON[T any](
	key string,
	defaultValue T,
	strictUnknownFields bool,
) (T, error) {
	return GetEnvJSONFrom(ProcessEnvironment(), key, defaultValue, strictUnknownFields)
}

// GetEnvJSONFromは、lookupが返す値をGetEnvJSONと同じ規則で変換します。
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

// GetEnvFloat64はenvironmentの有限な浮動小数点値を読み込みます。
// 未設定または空文字ならdefaultValueを返します。
// error時も未設定時と同じdefaultValueを返しますが、errorは設定errorとして扱ってください。
func GetEnvFloat64(key string, defaultValue float64) (float64, error) {
	return ProcessEnvironment().GetEnvFloat64(key, defaultValue)
}

// GetEnvFloat64は、lookupが返す値をpackage関数GetEnvFloat64と同じ規則で読み込みます。
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
