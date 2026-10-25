package memorypressure

// MeetsCriticalThresholdsは信頼できる各観測の使用率・残量を独立して確認します。
// 一つでも閾値に達すればtrueです。使用率0以下・残量0の閾値はそれぞれ無効です。
// 最大使用率だけを選ぶと、別の小さい上限で残量が不足している状況を見落とします。
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

// CriticalPolicy は、メモリ逼迫を critical と判定するための設定です。
type CriticalPolicy struct {
	// CriticalRatio は、使用率・逼迫率がこの値以上なら critical 候補とする閾値です。
	//
	// 例:
	//   0.90 なら 90%以上で critical 候補
	//
	// 0 以下の場合、この条件は無効です。
	CriticalRatio float64

	// MinAvailableBytes は、利用可能メモリがこの値以下なら critical 候補とする閾値です。
	//
	// 例:
	//   512 * 1024 * 1024 なら available が 512MiB 以下で critical 候補
	//
	// 0 の場合、この条件は無効です。
	MinAvailableBytes uint64

	// ConsecutiveCriticalHits は、critical 候補が何回連続したら
	// 本当に critical とみなすかを指定します。
	//
	// 例:
	//   1: 1回でも条件を満たしたら即 critical
	//   3: 3回連続で条件を満たしたら critical
	//
	// 瞬間的なスパイクで過剰反応しないための設定です。
	ConsecutiveCriticalHits int
}

// CriticalState は、critical 判定のための状態を保持します。
//
// ConsecutiveCriticalHits を使うため、前回までに何回連続で
// critical 候補になったかを覚えておく必要があります。
type CriticalState struct {
	// ConsecutiveHits は、critical 候補が連続して発生した回数です。
	ConsecutiveHits int
}

// Reset は、連続 critical 候補回数をリセットします。
func (s *CriticalState) Reset() {
	s.ConsecutiveHits = 0
}

// Evaluate は、現在の MemoryPressure が policy に照らして
// critical 状態かどうかを判定します。
//
// 戻り値:
//
//	true  = critical と判定
//	false = critical ではない
//
// 信頼できる観測にcritical候補がなければ、連続回数もリセットします。
// 主観測と追加制約の複数が危険でも、一観測につき一回だけ数えます。
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
