package timeoutbudget

import (
	"errors"
	"strings"

	"github.com/go-jappe/heterolog/internal/config/configerror"
)

// ErrInvalidTimeoutBudgetPolicyは、timeout budget policyがmust_fit・may_cutoff・unboundedの
// いずれでもないことを表します。
var ErrInvalidTimeoutBudgetPolicy = errors.New("invalid timeout budget policy")

type TimeoutBudgetPolicy string

const (
	TimeoutBudgetMustFit   TimeoutBudgetPolicy = "must_fit"
	TimeoutBudgetMayCutoff TimeoutBudgetPolicy = "may_cutoff"
	TimeoutBudgetUnbounded TimeoutBudgetPolicy = "unbounded"

	DefaultTimeoutBudgetPolicy = TimeoutBudgetMustFit
)

func ParseTimeoutBudgetPolicy(value string) (TimeoutBudgetPolicy, error) {
	normalized := TimeoutBudgetPolicy(strings.ToLower(strings.TrimSpace(value)))
	if normalized == "" {
		return DefaultTimeoutBudgetPolicy, nil
	}
	if normalized.IsValid() {
		return normalized, nil
	}
	return "", configerror.New(
		ErrInvalidTimeoutBudgetPolicy,
		"invalid timeout budget policy %q: allowed values are %q, %q, %q",
		value, TimeoutBudgetMustFit, TimeoutBudgetMayCutoff, TimeoutBudgetUnbounded,
	)
}

func NormalizeTimeoutBudgetPolicy(policy TimeoutBudgetPolicy) (TimeoutBudgetPolicy, error) {
	if policy == "" {
		return DefaultTimeoutBudgetPolicy, nil
	}
	if policy.IsValid() {
		return policy, nil
	}
	return "", configerror.New(
		ErrInvalidTimeoutBudgetPolicy,
		"invalid timeout budget policy %q: allowed values are %q, %q, %q",
		policy, TimeoutBudgetMustFit, TimeoutBudgetMayCutoff, TimeoutBudgetUnbounded,
	)
}

func (p TimeoutBudgetPolicy) IsValid() bool {
	switch p {
	case TimeoutBudgetMustFit, TimeoutBudgetMayCutoff, TimeoutBudgetUnbounded:
		return true
	default:
		return false
	}
}
