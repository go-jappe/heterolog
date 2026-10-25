package config

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/stretchr/testify/require"
)

func TestFileOverloadEnvironmentMappingAndInternalInheritance(t *testing.T) {
	for _, tc := range []struct {
		name, policy, destination string
		wantPolicy                durable.OverloadPolicy
		wantDestination           durable.SpillDestination
	}{
		{name: "defaults", wantPolicy: durable.OverloadPolicyDrop, wantDestination: durable.SpillDestinationStdout},
		{name: "degrade", policy: "  DEGRADE  ", destination: " STDOUT ", wantPolicy: durable.OverloadPolicyDegrade, wantDestination: durable.SpillDestinationStdout},
		{name: "invalid values are preserved", policy: "unknown", destination: "s3", wantPolicy: "unknown", wantDestination: "s3"},
	} {
		for _, async := range []bool{false, true} {
			name, prefix := "sync", ""
			if async {
				name, prefix = "async", "ASYNC_"
			}
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				restoreProcessEnvAfterTest(t)
				clearProcessEnvForTest(t)
				if tc.policy != "" {
					t.Setenv("HETEROLOG_"+prefix+"LOG_FILE_OVERLOAD_POLICY", tc.policy)
				}
				if tc.destination != "" {
					t.Setenv("HETEROLOG_"+prefix+"LOG_FILE_SECONDARY_DESTINATION", tc.destination)
				}
				syncConfig, err := loadFileConfig(processEnvironmentForTest())
				require.NoError(t, err)
				asyncConfig, err := loadAsyncFileConfig(processEnvironmentForTest())
				require.NoError(t, err)
				cfg := AppConfig{}
				cfg.Logger.FileConfig = syncConfig
				cfg.AsyncBestEffortMultiLoggerConfig.FileConfig = asyncConfig
				cfg.FsstoreConfig.MinAvailableDiskSpace = 12345
				selected, other := NewFileLoggerConfig(&cfg), NewAsyncFileLoggerConfig(&cfg)
				if async {
					selected, other = other, selected
				}
				want := pkgfsstore.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:        pkgfsstore.OverloadPolicy(tc.wantPolicy),
					SpillDestination:      pkgfsstore.SpillDestination(tc.wantDestination),
					MinAvailableDiskSpace: 12345,
				}
				require.Equal(t, want, selected.FsstoreConfig.Overload)
				require.Equal(t, pkgfsstore.OverloadPolicyDrop, other.FsstoreConfig.Overload.OverloadPolicy)
				require.Equal(t, pkgfsstore.SpillDestinationStdout, other.FsstoreConfig.Overload.SpillDestination)
				internal := newInternalLogDestinations(
					logconfig.InternalLogDestinationConfig{FileLogDir: "internal-logs"},
					func(target pkglogger.LogOutputDestination) bool { return target == pkglogger.LogOutputFile },
					pkglogger.LoggerConfig{File: selected},
				)
				require.NotNil(t, internal)
				require.NotNil(t, internal.File)
				require.Equal(t, want, internal.File.FsstoreConfig.Overload)
			})
		}
	}
}
