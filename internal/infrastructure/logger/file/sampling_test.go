package file

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

// sampling有効時のlevel別rateは、単体constructorとLoggerSetのどちらでも
// ログ保存先を作成する前に検証します。
func TestFileLoggerConstructorsRejectInvalidSamplingRate(t *testing.T) {
	rates := []struct {
		name   string
		field  pkglogger.ConfigField
		mutate func(*pkglogger.SamplingConfig)
	}{
		{name: "debug", field: pkglogger.ConfigFieldSamplingDebugRate, mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingDebugRate = 0 }},
		{name: "info", field: pkglogger.ConfigFieldSamplingInfoRate, mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingInfoRate = 0 }},
		{name: "warn", field: pkglogger.ConfigFieldSamplingWarnRate, mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingWarnRate = 0 }},
		{name: "error", field: pkglogger.ConfigFieldSamplingErrorRate, mutate: func(cfg *pkglogger.SamplingConfig) { cfg.SamplingErrorRate = 0 }},
	}
	constructors := []struct {
		name      string
		construct func(*testing.T, FileLoggerConfigProvider) error
	}{
		{name: "standalone", construct: func(t *testing.T, cfg FileLoggerConfigProvider) error {
			logger, err := newStandaloneFileLogger(context.Background(), cfg, nil, nil)
			if logger != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
			}
			return err
		}},
		{name: "logger set", construct: func(t *testing.T, cfg FileLoggerConfigProvider) error {
			set, err := NewLoggerSet(cfg, nil, nil)
			if set != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			}
			return err
		}},
	}

	for _, rate := range rates {
		for _, constructor := range constructors {
			t.Run(rate.name+"/"+constructor.name, func(t *testing.T) {
				// 対照の設定は、検証対象のrateが1であることだけが異なります。
				// 対照を受理し対象だけを拒否することで、拒否の原因がそのrateであることを確認します。
				valid := newTestFileLoggerConfig(t.TempDir(), "invalid_sampling", "invalid-sampling")
				valid.SamplingConfig.IsSamplingEnabled = true
				require.NoError(t, constructor.construct(t, NewConfigAdapter(valid)))

				root := t.TempDir()
				cfg := newTestFileLoggerConfig(root, "invalid_sampling", "invalid-sampling")
				cfg.SamplingConfig.IsSamplingEnabled = true
				rate.mutate(&cfg.SamplingConfig)

				err := constructor.construct(t, NewConfigAdapter(cfg))
				require.ErrorIs(t, err, pkglogger.ErrInvalidSamplingRate)
				fieldErrs := pkglogger.ConfigFieldErrors(err)
				require.Len(t, fieldErrs, 1)
				require.Equal(t, rate.field, fieldErrs[0].Field)
				require.Equal(t, 0, fieldErrs[0].Value)
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				require.Empty(t, entries)
			})
		}
	}
}

// request_idによるsamplingでは、同じrequestのentryをlevelにかかわらず同じ判定で扱い、
// 選ばれたrequestのentryだけをsampled付きでfileへ記録します。
func TestFileLoggerKeepsRequestSamplingDecisionAcrossEntries(t *testing.T) {
	ctx := context.Background()
	cfg := newTestFileLoggerConfig(t.TempDir(), "request_sampling", "file-request-sampling")
	// 全levelを同じfileへ記録し、requestごとの判定をまとめて確認します。
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	// level別rateを1より大きくし、どのlevelも必須出力として扱われないようにします。
	cfg.SamplingConfig = pkglogger.SamplingConfig{
		IsSamplingEnabled:        true,
		EnableLoggingByRequestID: true,
		LoggingByRequestIDRate:   3,
		SamplingDebugRate:        5,
		SamplingInfoRate:         7,
		SamplingWarnRate:         11,
		SamplingErrorRate:        13,
	}
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	writes := []struct {
		level string
		write func(context.Context, string, ...applogging.Field) error
	}{
		{level: "debug", write: logger.testDebug},
		{level: "info", write: logger.testInfo},
		{level: "warn", write: logger.testWarn},
		{level: "error", write: logger.testError},
	}
	const requests = 64
	for index := range requests {
		requestCtx := requestctx.WithRequestID(ctx, fmt.Sprintf("file-sampling-request-%02d", index))
		for _, write := range writes {
			require.NoError(t, write.write(requestCtx, fmt.Sprintf("file-sampling-%s-record-%02d", write.level, index)))
		}
	}

	content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	selected, skipped := 0, 0
	for index := range requests {
		requestID := fmt.Sprintf("file-sampling-request-%02d", index)
		keep := sampling.ShouldSampleByRequestId(requestID, cfg.SamplingConfig.LoggingByRequestIDRate)
		if keep {
			selected++
		} else {
			skipped++
		}
		for _, write := range writes {
			message := fmt.Sprintf("file-sampling-%s-record-%02d", write.level, index)
			var matched []string
			for _, line := range lines {
				if strings.Contains(line, message) {
					matched = append(matched, line)
				}
			}
			if !keep {
				require.Empty(t, matched, "request %s was not selected", requestID)
				continue
			}
			require.Len(t, matched, 1, "request %s was selected", requestID)
			require.Contains(t, matched[0], " - request_id: "+requestID)
			require.Contains(t, matched[0], " - sampled")
		}
	}
	require.NotZero(t, selected, "fixture must exercise selected requests")
	require.NotZero(t, skipped, "fixture must exercise excluded requests")
}
