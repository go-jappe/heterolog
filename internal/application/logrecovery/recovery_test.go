package logrecovery

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/config"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func loadRecoveryTestConfig(t *testing.T, overrides map[string]string) config.AppConfig {
	t.Helper()
	// 一時directoryはTMPDIR・TMPなどの実行環境から作るため、設定項目を外す前に作ります。
	// 外すのは設定loaderが読む項目だけで、実行環境は保持し、テスト終了時に復元します。
	// 保存先は実際のpathで解決されるため、macOSの/var -> /private/varのような
	// symlinkを含む一時directoryも、期待値と同じ表記へ先に解決します。
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	testutil.UnsetConfigEnvironment(t)
	values := map[string]string{
		"HETEROLOG_TARGET": "service", "HETEROLOG_SYSTEM_LOG_DIR": filepath.Join(root, "logs"),
		"HETEROLOG_APP_STATE_DIR": filepath.Join(root, "state"), "HETEROLOG_SYSTEM_LIB_DIR": filepath.Join(root, "spool"),
		"HETEROLOG_ENABLE_LOGGING": "true", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING": "true", "HETEROLOG_ENABLE_DELIVERY_LOGGING": "true",
		"HETEROLOG_LOG_TYPE": "file", "HETEROLOG_ASYNC_LOG_TYPE": "file", "HETEROLOG_DELIVERY_LOG_TYPE": "file",
		"HETEROLOG_LOG_DIR": "sync", "HETEROLOG_ASYNC_LOG_DIR": "async", "HETEROLOG_DELIVERY_LOG_DIR": "delivery",
	}
	for _, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
		values["HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_ERROR_LOGS"] = "false"
		values["HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_DEBUG_LOGS"] = "false"
		values["HETEROLOG_ENABLE_"+prefix+"LOG_FILE_OBJECT_STORAGE_UPLOAD"] = "false"
		values["HETEROLOG_ENABLE_"+prefix+"LOG_FILE_COMPRESS"] = "true"
		values["HETEROLOG_"+prefix+"LOG_COMPRESS_FORMAT"] = "gzip"
	}
	for key, value := range overrides {
		values[key] = value
	}
	var lines []string
	for key, value := range values {
		lines = append(lines, key+"="+strconv.Quote(value))
	}
	sort.Strings(lines)
	envPath := filepath.Join(root, "recovery.env")
	require.NoError(t, os.WriteFile(envPath, []byte(strings.Join(lines, "\n")), 0o600))
	require.NoError(t, os.Setenv("HETEROLOG_ENV_PATH", envPath))
	cfg, err := config.LoadConfig()
	require.NoError(t, err)
	return cfg
}

func TestRecoverLoadedConfigCompressesLatestWithoutPurgeOrWriter(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, map[string]string{"HETEROLOG_MAX_LOG_FILE_AGE": "1", "HETEROLOG_MAX_LOG_FILE_BACKUPS": "1"})
	cfg.Logger.FileConfig.FileSystemStoreConfig.MaxDirSize = 1
	directory := filepath.Join(cfg.FsstoreConfig.LogPath, "service", "sync")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	payload := []byte("residual log content\n")
	var compressed []string
	for _, period := range []string{"20000101", time.Now().UTC().Format("20060102")} {
		path := filepath.Join(directory, "service_"+period+"_1.log")
		require.NoError(t, os.WriteFile(path, payload, 0o600))
		compressed = append(compressed, path+".gz")
	}
	notes := filepath.Join(directory, "operator-notes.txt")
	require.NoError(t, os.WriteFile(notes, []byte("keep"), 0o600))

	report, err := Recover(context.Background(), cfg, Options{})
	require.NoError(t, err)
	require.Empty(t, report.Errors)
	require.Len(t, report.Directories, 1)
	result := report.Directories[0]
	require.Equal(t, "file-logger", result.Scope)
	require.Equal(t, directory, result.DirectoryPath)
	require.Equal(t, 2, result.ScannedFiles)
	require.ElementsMatch(t, compressed, result.CompressedFiles)
	require.Empty(t, result.AcceptedFiles)
	require.Empty(t, result.RemovedFiles)
	require.Empty(t, result.FailedFiles)
	for _, path := range compressed {
		require.NoFileExists(t, strings.TrimSuffix(path, ".gz"))
		file, err := os.Open(path)
		require.NoError(t, err)
		reader, err := gzip.NewReader(file)
		require.NoError(t, err)
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.NoError(t, file.Close())
		require.Equal(t, payload, got)
	}
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 3, "recovery must not create a new current log or purge retained data")
	require.FileExists(t, notes)
	require.NoDirExists(t, cfg.FsstoreConfig.AppStatePath, "disabled upload must not initialize checkpoints")
	require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.LogPath, "service", "async"))
	require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.LogPath, "service", "delivery"))

	// 続けて明示的なpassを実行しても害はありません。最初の呼び出しは、動作中のwriterを残しません。
	report, err = Recover(context.Background(), cfg, Options{LoggerContract: pkglogger.LoggerContractSync, Scope: RecoveryScopeFileLogger})
	require.NoError(t, err)
	require.Len(t, report.Directories, 1)
	require.Empty(t, report.Directories[0].CompressedFiles)
	for _, path := range compressed {
		require.FileExists(t, path)
	}
}

func TestRecoverCancelledBeforeStartDoesNotCreateStorage(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := Recover(ctx, cfg, Options{})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, report.Directories)
	require.Len(t, report.Errors, 1)
	require.NoDirExists(t, cfg.FsstoreConfig.LogPath)
	require.NoDirExists(t, cfg.FsstoreConfig.AppStatePath)
}

func TestRecoverCustomApplicationIdentifier(t *testing.T) {
	const identifier = "payments-api.v2"
	cfg := loadRecoveryTestConfig(t, map[string]string{"HETEROLOG_TARGET": identifier})
	directory := filepath.Join(cfg.FsstoreConfig.LogPath, identifier, "sync")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	source := filepath.Join(directory, identifier+"_20000101_1.log")
	require.NoError(t, os.WriteFile(source, []byte("custom application log\n"), 0o600))

	report, err := Recover(context.Background(), cfg, Options{})
	require.NoError(t, err)
	require.Len(t, report.Directories, 1)
	require.Equal(t, []string{source + ".gz"}, report.Directories[0].CompressedFiles)
	require.NoFileExists(t, source)
	require.FileExists(t, source+".gz")
}

func TestRecoverInvalidIdentifierDoesNotCreateStorage(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, nil)
	cfg.RuntimeConfig.Target = "../other-app"
	report, err := Recover(context.Background(), cfg, Options{})
	require.ErrorIs(t, err, ErrInvalidRecoveryTarget)
	require.Empty(t, report.Directories)
	require.NoDirExists(t, cfg.FsstoreConfig.LogPath)
	require.NoDirExists(t, cfg.FsstoreConfig.AppStatePath)
}

// 環境設定が空の場合、どのlogger契約も標準出力だけを使います。asyncでもSlackなどを
// 暗黙に選ばないため、設定変換は認証情報なしで成功し、復旧対象がないことを報告します。
func TestRecoverEmptyEnvironmentReportsNoConfiguredFiles(t *testing.T) {
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
			specs, err := recoverySpecs(cfg, Options{LoggerContract: contract, Scope: RecoveryScopeAll})
			require.NoError(t, err)
			require.Empty(t, specs)
			report, err := Recover(context.Background(), cfg, Options{LoggerContract: contract})
			require.ErrorIs(t, err, ErrNoConfiguredLogFiles)
			// Report.ErrorsはCLIがJSON・本文として出力する文字列の契約なので、返したerrorの
			// 文言がそのまま記録されることを確認します。errorの種類はerrors.Isで判定済みです。
			require.Equal(t, []string{err.Error()}, report.Errors)
			require.Empty(t, report.Directories)
		})
	}
}

func TestDirectoryReportPreservesIndividualFailurePaths(t *testing.T) {
	failure := errors.New("compression unavailable")
	result := directoryReport("file-logger", logfilelifecycle.RunReport{
		DirectoryPath: "/logs", ScannedFiles: 2,
		Compressed:     []logfilelifecycle.CompressionReport{{SourcePath: "/logs/good.log", CompressedPath: "/logs/good.log.gz"}},
		RecoveryErrors: []logfilelifecycle.FileRecoveryError{{Path: "/logs/bad.log", Err: failure}},
	}, failure)
	// DirectoryReportのError欄はCLIが利用者へ出力する文字列の契約なので、元のerror文を
	// 保持することを確認します。
	require.Equal(t, []FileFailureReport{{Path: "/logs/bad.log", Error: failure.Error()}}, result.FailedFiles)
	require.Equal(t, []string{"/logs/good.log.gz"}, result.CompressedFiles)
	require.Equal(t, failure.Error(), result.Error)
}
