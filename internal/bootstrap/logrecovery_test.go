package bootstrap_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/logrecovery"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 環境設定が空の場合、どのlogger契約も標準出力だけを使います。asyncでもSlackなどを
// 暗黙に選ばないため、設定変換は認証情報なしで成功し、復旧対象がないことを報告します。
func TestRecoverLogsWithEmptyEnvironmentReportsNoConfiguredFiles(t *testing.T) {
	root := t.TempDir()
	testutil.UnsetConfigEnvironment(t)
	envPath := filepath.Join(root, "empty.env")
	require.NoError(t, os.WriteFile(envPath, nil, 0o600))
	require.NoError(t, os.Setenv("HETEROLOG_ENV_PATH", envPath))
	cfg, err := config.LoadConfig()
	require.NoError(t, err)
	cfg.RuntimeConfig.Target = "service"
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		t.Run(contract.String(), func(t *testing.T) {
			report, err := bootstrap.RecoverLogs(context.Background(), cfg, logrecovery.Options{LoggerContract: contract})
			require.ErrorIs(t, err, logrecovery.ErrNoConfiguredLogFiles)
			// Report.ErrorsはCLIがJSON・本文として出力する文字列の契約なので、返したerrorの
			// 文言がそのまま記録されることを確認します。errorの種類はerrors.Isで判定済みです。
			require.Equal(t, []string{err.Error()}, report.Errors)
			require.Empty(t, report.Directories)
		})
	}
}

// 設定のtargetが保存先の識別子として使えない場合は、logger設定を変換する前に拒否します。
func TestRecoverLogsRejectsInvalidTarget(t *testing.T) {
	report, err := bootstrap.RecoverLogs(context.Background(), config.AppConfig{}, logrecovery.Options{})
	require.ErrorIs(t, err, logrecovery.ErrInvalidRecoveryTarget)
	require.Empty(t, report.Directories)
}
