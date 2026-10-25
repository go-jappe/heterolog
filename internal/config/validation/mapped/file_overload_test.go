package mappedvalidation

import (
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestMappedFileOverloadValidatesUsedDestinations(t *testing.T) {
	for _, source := range []string{"logger", "async logger"} {
		for _, route := range []string{"normal", "handler base", "internal override", "unused"} {
			for _, tc := range []struct {
				name   string
				policy pkgfsstore.OverloadPolicy
				spill  pkgfsstore.SpillDestination
				// wantErrは拒否の理由です。nilなら受理します。
				wantErr error
			}{
				{name: "drop", policy: pkgfsstore.OverloadPolicyDrop},
				{name: "degrade stdout", policy: pkgfsstore.OverloadPolicyDegrade, spill: pkgfsstore.SpillDestinationStdout},
				{name: "invalid policy", policy: "unsupported", wantErr: pkgfsstore.ErrUnsupportedOverloadPolicy},
				// 退避先がstdoutでも、未知の方針をdegradeとして受け付けません。
				{
					name: "invalid policy with stdout", policy: "unsupported", spill: pkgfsstore.SpillDestinationStdout,
					wantErr: pkgfsstore.ErrUnsupportedOverloadPolicy,
				},
				{
					name: "unsupported destination", policy: pkgfsstore.OverloadPolicyDegrade, spill: "s3",
					wantErr: pkgfsstore.ErrUnsupportedSpillDestination,
				},
			} {
				t.Run(source+"/"+route+"/"+tc.name, func(t *testing.T) {
					cfg := mappedRolePurgeFileConfig()
					// lifecycle無効でも書込みは行うため、overload設定は検証が必要です。
					cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
					cfg.FsstoreConfig.Overload = pkgfsstore.FileArchiveOverloadPolicyConfig{OverloadPolicy: tc.policy, SpillDestination: tc.spill}
					err := validateMappedLogFileNameDestination(source, route, cfg, nil)
					if tc.wantErr != nil && route != "unused" {
						label := source + " file"
						if route == "internal override" {
							label = source + " internal log file"
						}
						requireConfigError(t, err, label, ConfigSectionOverload, tc.wantErr)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
