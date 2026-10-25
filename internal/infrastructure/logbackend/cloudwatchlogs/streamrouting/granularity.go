// Package streamroutingは、eventを時間単位で分割したCloudWatch Logs streamへ
// 振り分けるapplication側のrouting policyを定義します。
package streamrouting

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidGranularityは、routing granularityが対応値でないことを表します。
// 実際に返すerrorはInvalidGranularityErrorです。
var ErrInvalidGranularity = errors.New("invalid cloudwatch logs stream routing granularity")

// InvalidGranularityErrorは、対応していないrouting granularityを指定したことを表します。
// errors.Is(err, ErrInvalidGranularity)で判定できます。
type InvalidGranularityError struct {
	// Granularityは、指定された値です。
	Granularity Granularity
}

func (e *InvalidGranularityError) Error() string {
	return fmt.Sprintf(
		"%v %q: must be one of none, monthly, daily, hourly",
		ErrInvalidGranularity,
		e.Granularity,
	)
}

func (e *InvalidGranularityError) Unwrap() error { return ErrInvalidGranularity }

// Granularityは、基準CloudWatch Logs stream名を分割する時間粒度です。
// CloudWatch Logs APIのoptionではなく、application側のstream routing policyです。
type Granularity string

const (
	// Noneは、設定された基準stream名をそのまま使用します。
	None Granularity = "none"
	// Monthlyは、基準stream名へYYYY/MM suffixを付けます。
	Monthly Granularity = "monthly"
	// Dailyは、基準stream名へYYYY/MM/DD suffixを付けます。
	Daily Granularity = "daily"
	// Hourlyは、基準stream名へYYYY/MM/DD/HH suffixを付けます。
	Hourly Granularity = "hourly"
)

// Normalizeはrouting granularityの表記を正規化し、ゼロ値をNoneとして扱います。
func Normalize(granularity Granularity) Granularity {
	normalized := Granularity(strings.ToLower(strings.TrimSpace(string(granularity))))
	if normalized == "" {
		return None
	}
	return normalized
}

// Validateはrouting granularityが対応値かを検証します。ゼロ値はNoneとして有効です。
func (g Granularity) Validate() error {
	switch Normalize(g) {
	case None, Monthly, Daily, Hourly:
		return nil
	default:
		return &InvalidGranularityError{Granularity: g}
	}
}

// Suffixはrouting granularityに対応する先頭slashなしの時間pathを返します。
// Noneの場合は空文字列を返し、atのLocationはそのまま使用します。
func Suffix(granularity Granularity, at time.Time) (string, error) {
	if err := granularity.Validate(); err != nil {
		return "", err
	}

	switch Normalize(granularity) {
	case None:
		return "", nil
	case Monthly:
		return at.Format("2006/01"), nil
	case Daily:
		return at.Format("2006/01/02"), nil
	case Hourly:
		return at.Format("2006/01/02/15"), nil
	default:
		// Validateを通過した値だけが到達するため通常は実行されません。
		// 対応値とsuffix実装が将来ずれた場合に備えて防御的に残します。
		return "", fmt.Errorf(
			"cloudwatch logs stream routing granularity %q has no suffix implementation",
			granularity,
		)
	}
}
