package pressure

import (
	"errors"
	"fmt"
	"math"

	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// validateCriticalPolicy は、critical を一度も判定できない policy や、
// 閾値として意味を持たない値を拒否します。
//
// CriticalRatio と MinAvailableBytes の無効値（0 以下・0）は CriticalPolicy の
// 文書どおりに扱いますが、両方が無効だと critical にならないため拒否します。
// 使用率は 0 から 1 の範囲なので、1 を超える CriticalRatio と NaN も拒否します。
func validateCriticalPolicy(policy memorypressure.CriticalPolicy) error {
	if math.IsNaN(policy.CriticalRatio) || policy.CriticalRatio > 1 {
		return fmt.Errorf("memory critical ratio must be at most 1, or 0 or less to disable it: %v", policy.CriticalRatio)
	}
	if policy.CriticalRatio <= 0 && policy.MinAvailableBytes == 0 {
		return errors.New("memory critical policy must enable CriticalRatio or MinAvailableBytes")
	}
	if policy.ConsecutiveCriticalHits < 0 {
		return fmt.Errorf("memory critical consecutive hits must not be negative: %d", policy.ConsecutiveCriticalHits)
	}
	return nil
}
