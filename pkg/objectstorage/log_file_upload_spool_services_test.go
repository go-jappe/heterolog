package objectstorage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

func TestCompressedLogFileUploadSpoolServices(t *testing.T) {
	const other UploadTarget = "other"
	spoolTarget := func(services ...UploadTarget) UploadTargetConfig {
		return UploadTargetConfig{
			Enabled: true,
			Targets: services,
			CompressedLogFileUploadFailureStore: CompressedLogFileUploadFailureStoreConfig{
				DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
			},
		}
	}
	config := func() LogFileUploadConfig {
		return LogFileUploadConfig{
			Enabled: true,
			Default: spoolTarget(UploadTargetS3),
			Error:   spoolTarget(other, UploadTargetS3),
			Debug:   spoolTarget(other),
		}
	}
	for _, test := range []struct {
		name                         string
		separateError, separateDebug bool
		change                       func(*LogFileUploadConfig)
		want                         []UploadTarget
	}{
		{name: "all roles deduplicate", separateError: true, separateDebug: true, want: []UploadTarget{UploadTargetS3, other}},
		{name: "inactive roles", want: []UploadTarget{UploadTargetS3}},
		{name: "debug role", separateDebug: true, want: []UploadTarget{UploadTargetS3, other}},
		{name: "disabled upload", separateError: true, separateDebug: true, change: func(cfg *LogFileUploadConfig) { cfg.Enabled = false }},
		{name: "disabled role", separateError: true, change: func(cfg *LogFileUploadConfig) { cfg.Error.Enabled = false }, want: []UploadTarget{UploadTargetS3}},
		{name: "archive only excluded", separateError: true, change: func(cfg *LogFileUploadConfig) {
			cfg.Error.CompressedLogFileUploadFailureStore.DurableType = []failurestore.DurableType{failurestore.DurableTypeFileArchive}
		}, want: []UploadTarget{UploadTargetS3}},
		{name: "no selected store", change: func(cfg *LogFileUploadConfig) { cfg.Default.CompressedLogFileUploadFailureStore.DurableType = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config()
			if test.change != nil {
				test.change(&cfg)
			}
			require.Equal(t, test.want, cfg.CompressedLogFileUploadSpoolServices(test.separateError, test.separateDebug))
			provider := NewLogFileUploadConfigAdapter(cfg)
			require.Equal(t, test.want, provider.CompressedLogFileUploadSpoolServices(test.separateError, test.separateDebug))
			require.Equal(t, cfg.UsesCompressedLogFileUploadSpool(test.separateError, test.separateDebug),
				provider.UsesCompressedLogFileUploadSpool(test.separateError, test.separateDebug))
		})
	}
}
