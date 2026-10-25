package logging

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/clock"
)

// ResolveOccurredAtは、internal logなどLogEntryをinfra内部で生成する経路の発生時刻を
// Clockから確定します。application logはtop-level loggerが一度確定したOccurredAtを
// 全backendへ渡すため、この関数で再解決しません。
// 呼び出し元は構築時に設定済みのClockを渡してください。Clockの返り値がゼロ時刻でも、
// 実時計の時刻へ置き換えず、その値をUTCで返します。
func ResolveOccurredAt(occurrenceClock clock.Clock) time.Time {
	return occurrenceClock.Now().UTC()
}

// ResolveEntryOccurredAtはLogEntryに確定済みの発生時刻を返します。zeroの場合は、
// Clockを持たない変換・診断経路の最終防御として現在時刻を使用します。
// 返り値はbackendの表示timezoneに依存しないUTCです。
func ResolveEntryOccurredAt(entry *LogEntry) time.Time {
	if entry == nil {
		return time.Now().UTC()
	}
	if !entry.OccurredAt.IsZero() {
		return entry.OccurredAt.UTC()
	}
	return time.Now().UTC()
}

// EnsureEntryOccurredAtは、LogEntryの発生時刻が未確定の場合だけ明示されたClockから確定し、
// 以後のfallback、buffer、backend変換でも同じ時刻を参照できるようentryへ保存します。
func EnsureEntryOccurredAt(entry *LogEntry, occurrenceClock clock.Clock) time.Time {
	if entry != nil && !entry.OccurredAt.IsZero() {
		entry.OccurredAt = entry.OccurredAt.UTC()
		return entry.OccurredAt
	}
	occurredAt := ResolveOccurredAt(occurrenceClock)
	if entry != nil {
		entry.OccurredAt = occurredAt
	}
	return occurredAt
}
