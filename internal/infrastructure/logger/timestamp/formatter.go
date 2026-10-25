// Package timestampは、loggerが確定済みの発生時刻を各出力先の表示規則へ変換する機能を
// 提供します。現在時刻を取得するClockとは独立して扱います。
package timestamp

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrLayoutRequiredは、ログの時刻表示に使うGo time layout（LogLineDateFormatなど）が空であることを
// 表します。
var ErrLayoutRequired = errors.New("timestamp formatter layout is required")

// Formatterは、確定済みの時刻を一つのtimezoneとGo time layoutで文字列化します。
// 現在時刻は取得せず、各出力先がそれぞれ所有します。zero値はUTCのRFC3339として
// 動作します。
type Formatter struct {
	location *time.Location
	layout   string
}

// NewFormatterは、ログ出力に使用するtimezoneとGo time layoutを検証してFormatterを
// 作ります。
func NewFormatter(location *time.Location, layout string) (Formatter, error) {
	if location == nil {
		return Formatter{}, fmt.Errorf("timestamp formatter location is required")
	}
	if strings.TrimSpace(layout) == "" {
		return Formatter{}, ErrLayoutRequired
	}
	return Formatter{location: location, layout: layout}, nil
}

// RFC3339FormatterはUTCのRFC3339表示を返すFormatterを作ります。
func RFC3339Formatter() Formatter {
	return Formatter{location: time.UTC, layout: time.RFC3339}
}

// Formatはatが表す瞬間を設定済みtimezoneへ変換してから文字列化します。
func (f Formatter) Format(at time.Time) string {
	return at.In(f.Location()).Format(f.Layout())
}

// Locationは表示に使用するtimezoneを返します。
func (f Formatter) Location() *time.Location {
	if f.location == nil {
		return time.UTC
	}
	return f.location
}

// Layoutは表示に使用するGo time layoutを返します。
func (f Formatter) Layout() string {
	if f.layout == "" {
		return time.RFC3339
	}
	return f.layout
}

// IsZeroはconstructorで設定されたFormatterかを返します。logger constructorが外部から
// 渡された設定を尊重しつつ、未設定時だけconfigから補完するために使用します。
func (f Formatter) IsZero() bool {
	return f.location == nil && f.layout == ""
}
