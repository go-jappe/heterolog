package config

import (
	"fmt"
	"slices"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// TestTimeoutBudgetPolicyKeyAppliesOnlyToItsContractは、HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY・
// HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY・HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICYが、それぞれの
// LoggerContractKindのtimeout予算検証だけに効くことを環境設定から検証します。
// 3契約ともLokiを出力先に選び、logger timeoutを既定のLoki clientの再試行予算（8秒）より
// 短くしたうえで、policyを契約ごとに変えます。must_fitの契約だけがTimeoutHierarchyErrorになり、
// may_cutoff・unboundedの契約は拒否しません。policyのkeyはmapperの変換結果へ届かないため
// （publicEnvKeyMapperExemptions）、検証への効果はこの試験で確認します。
func TestTimeoutBudgetPolicyKeyAppliesOnlyToItsContract(t *testing.T) {
	contracts := []struct {
		prefix string
		kind   pkglogger.LoggerContractKind
	}{
		{"", pkglogger.LoggerContractSync},
		{"ASYNC_", pkglogger.LoggerContractAsyncBestEffort},
		{"DELIVERY_", pkglogger.LoggerContractDelivery},
	}
	for _, own := range contracts {
		for _, tc := range []struct {
			ownPolicy, otherPolicy timeoutbudget.TimeoutBudgetPolicy
		}{
			{timeoutbudget.TimeoutBudgetMustFit, timeoutbudget.TimeoutBudgetUnbounded},
			{timeoutbudget.TimeoutBudgetMustFit, timeoutbudget.TimeoutBudgetMayCutoff},
			{timeoutbudget.TimeoutBudgetUnbounded, timeoutbudget.TimeoutBudgetMustFit},
			{timeoutbudget.TimeoutBudgetMayCutoff, timeoutbudget.TimeoutBudgetMustFit},
		} {
			name := fmt.Sprintf("%s/%s with others %s", own.kind, tc.ownPolicy, tc.otherPolicy)
			t.Run(name, func(t *testing.T) {
				env := minimalNetworkBackendEnvironment([]string{"", "ASYNC_", "DELIVERY_"}, []string{"loki"})
				var want []pkglogger.LoggerContractKind
				for _, contract := range contracts {
					policy := tc.otherPolicy
					if contract.prefix == own.prefix {
						policy = tc.ownPolicy
					}
					env["HETEROLOG_"+contract.prefix+"LOG_TIMEOUT_SECOND"] = "1"
					env["HETEROLOG_"+contract.prefix+"LOG_TIMEOUT_BUDGET_POLICY"] = string(policy)
					if policy == timeoutbudget.TimeoutBudgetMustFit {
						want = append(want, contract.kind)
					}
				}
				err := ValidateApplicationConfig(loadMinimalEnvConfig(t, env))

				var got []pkglogger.LoggerContractKind
				for _, hierarchy := range errortree.All[*TimeoutHierarchyError](err) {
					if hierarchy.Backend != pkglogger.LogOutputLoki {
						t.Errorf("%s TimeoutHierarchyError backend = %s, want %s", hierarchy.Contract, hierarchy.Backend, pkglogger.LogOutputLoki)
					}
					got = append(got, hierarchy.Contract)
				}
				slices.Sort(got)
				slices.Sort(want)
				if !slices.Equal(got, want) {
					t.Fatalf("TimeoutHierarchyError contracts = %v, want %v (error %v)", got, want, err)
				}
				if len(want) == 0 && err != nil {
					t.Fatalf("ValidateApplicationConfig() = %v, want nil", err)
				}
			})
		}
	}
}
