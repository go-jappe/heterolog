package memorypressure

// MeetsCriticalThresholds checks the usage ratio and the remaining amount of
// each reliable observation independently. It returns true if any of them
// reaches its threshold. A usage ratio threshold of 0 or less and a remaining
// amount threshold of 0 are each disabled. An AvailableBytes of 0 in the
// primary observation counts as not obtained, so it never reaches the
// remaining amount threshold. Choosing only the highest usage ratio would miss
// a shortage of remaining memory under another, smaller limit.
func MeetsCriticalThresholds(p MemoryPressure, criticalRatio float64, minAvailableBytes uint64) bool {
	if criticalRatio > 0 && IsCriticalOverload(p, criticalRatio) {
		return true
	}
	if minAvailableBytes == 0 {
		return false
	}
	// 既存の全体観測ではAvailableBytes==0に未取得も含まれるため、従来どおり除外します。
	if p.Reliable && p.AvailableBytes > 0 && p.AvailableBytes <= minAvailableBytes {
		return true
	}
	// 追加制約は上限と使用量の取得成功を明示できるので、残量0も上限到達として扱います。
	limit := p.ProcessLimit
	if limit.Reliable && limit.LimitBytes > 0 &&
		limit.LimitBytes-min(limit.UsedBytes, limit.LimitBytes) <= minAvailableBytes {
		return true
	}
	limit = p.CgroupLimit
	return limit.Reliable && limit.LimitBytes-min(limit.UsedBytes, limit.LimitBytes) <= minAvailableBytes
}

// CriticalPolicy holds the settings for judging memory pressure as critical.
type CriticalPolicy struct {
	// CriticalRatio is the threshold at or above which the usage ratio or the
	// pressure ratio makes a reading a critical candidate.
	//
	// For example:
	//   0.90 makes 90% or more a critical candidate
	//
	// When it is 0 or less, this condition is disabled.
	CriticalRatio float64

	// MinAvailableBytes is the threshold at or below which the available
	// memory makes a reading a critical candidate.
	//
	// For example:
	//   512 * 1024 * 1024 makes available memory of 512MiB or less a critical
	//   candidate
	//
	// When it is 0, this condition is disabled. It applies to the primary
	// observation and to the remaining amount of each reliable additional
	// limit. An AvailableBytes of 0 in the primary observation counts as not
	// obtained and never reaches it, while a remaining amount of 0 under an
	// additional limit does; see MeetsCriticalThresholds.
	MinAvailableBytes uint64

	// ConsecutiveCriticalHits specifies how many critical candidates in a row
	// are needed before the state is really considered critical.
	//
	// For example:
	//   1: critical as soon as the conditions are met even once
	//   3: critical when the conditions are met 3 times in a row
	//
	// It keeps momentary spikes from causing an overreaction. CriticalState
	// treats 0 or less as 1.
	ConsecutiveCriticalHits int
}

// CriticalState holds the state for critical evaluation.
//
// Because ConsecutiveCriticalHits is used, it needs to remember how many times
// in a row the readings up to the previous one were critical candidates.
type CriticalState struct {
	// ConsecutiveHits is the number of critical candidates that occurred in a
	// row.
	ConsecutiveHits int
}

// Reset resets the count of consecutive critical candidates.
func (s *CriticalState) Reset() {
	s.ConsecutiveHits = 0
}

// Evaluate judges whether the current MemoryPressure is in a critical state
// according to policy.
//
// Return value:
//
//	true  = judged critical
//	false = not critical
//
// If no reliable observation is a critical candidate, it also resets the
// consecutive count. Even when several of the primary observation and the
// additional constraints are in danger, it counts only once per reading.
func (s *CriticalState) Evaluate(p MemoryPressure, policy CriticalPolicy) bool {
	// 今回 critical 候補でなければ、連続回数をリセットする。
	if !MeetsCriticalThresholds(p, policy.CriticalRatio, policy.MinAvailableBytes) {
		s.ConsecutiveHits = 0
		return false
	}

	// 今回 critical 候補だったので連続回数を加算する。
	s.ConsecutiveHits++

	// 連続回数の指定が 1 以下なら、1回の検出で即 critical。
	if policy.ConsecutiveCriticalHits <= 1 {
		return true
	}

	// 指定回数以上、連続して critical 候補になった場合のみ critical。
	return s.ConsecutiveHits >= policy.ConsecutiveCriticalHits
}
