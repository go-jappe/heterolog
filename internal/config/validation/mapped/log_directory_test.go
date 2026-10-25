package mappedvalidation

import (
	"testing"

	"github.com/stretchr/testify/require"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestMappedLogDirectoryValidation(t *testing.T) {
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, route := range []string{"normal", "handler base", "internal override"} {
			for _, tc := range []struct {
				name, directory string
				wantError       bool
			}{
				{name: "empty", wantError: true},
				{name: "whitespace", directory: " \t\n", wantError: true},
				{name: "target root", directory: "."},
				{name: "directory", directory: "logs"},
			} {
				t.Run(source+"/"+route+"/"+tc.name, func(t *testing.T) {
					cfg := mappedRolePurgeFileConfig()
					cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
					cfg.FsstoreConfig.Layout.LogDir = tc.directory
					err := validateMappedLogFileNameDestination(source, route, cfg, nil)
					if tc.wantError {
						label := source + " file"
						if route == "internal override" {
							label = source + " internal log file"
						}
						requireConfigError(t, err, label, ConfigSectionDirectory, pkgfsstore.ErrEmptyLogDirectory)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestMappedLogDirectoryIgnoresUnusedBase(t *testing.T) {
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, route := range []string{"unused", "internal override"} {
			t.Run(source+"/"+route, func(t *testing.T) {
				unused := mappedRolePurgeFileConfig()
				unused.FsstoreConfig.Layout.LogDir = " \t "
				require.NoError(t, validateMappedLogFileNameDestination(source, route, mappedRolePurgeFileConfig(), unused))
			})
		}
	}
}
