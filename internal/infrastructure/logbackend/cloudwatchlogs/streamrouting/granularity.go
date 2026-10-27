// Package streamroutingは、eventを時間単位で分割したCloudWatch Logs streamへ
// 振り分けるapplication側のrouting policyを定義します。
package streamrouting

import (
	"fmt"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// Suffixはrouting granularityに対応する先頭slashなしの時間pathを返します。
// Noneの場合は空文字列を返し、atのLocationはそのまま使用します。
func Suffix(granularity pkgcloudwatchlogs.StreamRouting, at time.Time) (string, error) {
	if err := granularity.Validate(); err != nil {
		return "", err
	}

	switch granularity.Normalize() {
	case pkgcloudwatchlogs.StreamRoutingNone:
		return "", nil
	case pkgcloudwatchlogs.StreamRoutingMonthly:
		return at.Format("2006/01"), nil
	case pkgcloudwatchlogs.StreamRoutingDaily:
		return at.Format("2006/01/02"), nil
	case pkgcloudwatchlogs.StreamRoutingHourly:
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
