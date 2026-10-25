package fsstore

import (
	"errors"
	"testing"
)

func TestValidateFileOverloadConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy OverloadPolicy
		spill  SpillDestination
		want   error
	}{
		{name: "unspecified defaults to drop"},
		{name: "drop", policy: OverloadPolicyDrop},
		{name: "drop ignores unused destination", policy: OverloadPolicyDrop, spill: "future"},
		{name: "degrade stdout", policy: OverloadPolicyDegrade, spill: SpillDestinationStdout},
		{name: "unknown policy", policy: "future", want: ErrUnsupportedOverloadPolicy},
		// 退避先がstdoutでも、未知の方針をdegradeとして受け付けません。
		{name: "unknown policy with stdout", policy: "future", spill: SpillDestinationStdout, want: ErrUnsupportedOverloadPolicy},
		{name: "degrade missing destination", policy: OverloadPolicyDegrade, want: ErrUnsupportedSpillDestination},
		{name: "degrade s3", policy: OverloadPolicyDegrade, spill: "s3", want: ErrUnsupportedSpillDestination},
		{name: "degrade nfs", policy: OverloadPolicyDegrade, spill: "nfs", want: ErrUnsupportedSpillDestination},
		{name: "degrade unknown destination", policy: OverloadPolicyDegrade, spill: "future", want: ErrUnsupportedSpillDestination},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateFileOverloadConfig(tc.policy, tc.spill)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("ValidateFileOverloadConfig(%q, %q) error = %v, want nil", tc.policy, tc.spill, err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("ValidateFileOverloadConfig(%q, %q) error = %v, want %v", tc.policy, tc.spill, err, tc.want)
			}
		})
	}
}

// 方針と退避先の不正は、運用者が設定を直すための表示文で区別します。表示文の形式そのものが
// 運用者向けの契約なので、種類ごとに一件だけ固定します。
func TestValidateFileOverloadConfigErrorFormat(t *testing.T) {
	for _, tc := range []struct {
		policy OverloadPolicy
		spill  SpillDestination
		want   string
	}{
		{policy: "future", want: `file overload policy "future" is invalid; use drop or degrade`},
		{policy: OverloadPolicyDegrade, spill: "s3", want: `file overload secondary destination "s3" is unsupported; degrade requires stdout`},
	} {
		if err := ValidateFileOverloadConfig(tc.policy, tc.spill); err == nil || err.Error() != tc.want {
			t.Errorf("ValidateFileOverloadConfig(%q, %q) error = %v, want %q", tc.policy, tc.spill, err, tc.want)
		}
	}
}
