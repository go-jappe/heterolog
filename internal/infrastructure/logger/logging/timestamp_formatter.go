package logging

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
)

// ErrInvalidTimeZoneは、local time表示に指定されたtimezoneを読み込めないことを表します。
var ErrInvalidTimeZone = errors.New("invalid timezone")

// NewTimestampFormatterはlogger設定から、ログ本文の表示だけを担当するformatterを
// 作ります。現在時刻の取得やrotation期間の判定には使用しません。
func NewTimestampFormatter(
	useLocal bool,
	timezone string,
	layout string,
) (timestamp.Formatter, error) {
	location, err := ResolveLocation(useLocal, timezone)
	if err != nil {
		return timestamp.Formatter{}, err
	}

	formatter, err := timestamp.NewFormatter(location, layout)
	if err != nil {
		return timestamp.Formatter{}, fmt.Errorf("create timestamp formatter: %w", err)
	}
	return formatter, nil
}

// ResolveLocationはログ本文の表示用タイムゾーンを設定から解決します。
// 保存先・rotation・purgeの日付判定には使いません。local timeが無効ならUTCです。
func ResolveLocation(useLocal bool, timezone string) (*time.Location, error) {
	if !useLocal {
		return time.UTC, nil
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrInvalidTimeZone, timezone, err)
	}
	return location, nil
}
