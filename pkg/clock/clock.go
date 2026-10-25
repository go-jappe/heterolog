package clock

import "time"

// Clockは現在時刻をtime.Timeとして返す時刻源です。
// 戻り値のタイムゾーンはClockの実装が決めます。
// 使用する実装や既定値は利用側の構築時に決め、処理中はNowの戻り値をそのまま使います。
type Clock interface {
	Now() time.Time
}

// UTCClockは現在時刻をUTCで返します。
type UTCClock struct{}

func NewUTCClock() Clock {
	return UTCClock{}
}

func (UTCClock) Now() time.Time {
	return time.Now().UTC()
}

// FixedClockは構築時に指定された時刻を返し続けるClockです。
type FixedClock struct {
	at time.Time
}

func NewFixedClock(at time.Time) Clock {
	return FixedClock{at: at}
}

func (c FixedClock) Now() time.Time {
	return c.at
}
