package bootstrap_test

import (
	"compress/gzip"
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/logrecovery"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 環境設定が空の場合、どのlogger契約も標準出力だけを使います。asyncでもSlackなどを
// 暗黙に選ばないため、設定変換は認証情報なしで成功し、復旧対象がないことを報告します。
// traceとmetricsも既定では無効のため、Telemetryにも復旧対象はありません。
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
		storageowner.Telemetry,
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

// 停止したTelemetryが残したtraceのfailure archiveを、通常の起動と同じ設定で復旧します。
// 送り先に拒否されたspanはfailure archiveへ保存され、復旧が最新のarchiveも圧縮します。
func TestRecoverLogsCompressesTheTraceFailureArchiveOfAStoppedTelemetry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	testutil.UnsetConfigEnvironment(t)
	values := map[string]string{
		"HETEROLOG_TARGET": "service", "HETEROLOG_SYSTEM_LOG_DIR": filepath.Join(root, "logs"),
		"HETEROLOG_APP_STATE_DIR": filepath.Join(root, "state"), "HETEROLOG_SYSTEM_LIB_DIR": filepath.Join(root, "lib"),
		"HETEROLOG_ENABLE_TRACES":                                    "true",
		"HETEROLOG_TRACES_TYPE":                                      "otlp",
		"HETEROLOG_TRACES_OTLP_ENDPOINT":                             server.URL,
		"HETEROLOG_TRACES_OTLP_RETRIES":                              "0",
		"HETEROLOG_TRACES_EXPORT_INTERVAL_MILLISECOND":               "10",
		"HETEROLOG_TRACES_OTLP_DURABLE_TYPES":                        "fileArchive",
		"HETEROLOG_ENABLE_TRACES_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS": "true",
		"HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT": "gzip",
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

	tel, err := bootstrap.NewTelemetry(context.Background(), config.MapTelemetryConfig(cfg),
		loggerruntime.WithTelemetryErrorHandler(func(error) {}))
	require.NoError(t, err)
	_, span := tel.Tracer().StartSpan(context.Background(), "archived-span")
	span.End()
	require.NoError(t, tel.Shutdown(context.Background()))
	// failure archiveは、retryableとpermanentのdirectoryに一つずつ開きます。受信側が拒否した
	// spanは、permanentへ保存します。
	archives := filesWithSuffix(t, root, ".jsonl")
	require.Len(t, archives, 2)
	rejected := filepath.Join(root, "state", "service", "traces_otlp_failure_archives", "permanent")

	report, err := bootstrap.RecoverLogs(context.Background(), cfg, logrecovery.Options{LoggerContract: storageowner.Telemetry})
	require.NoError(t, err)
	require.Empty(t, report.Errors)
	var compressed []string
	for _, directory := range report.Directories {
		require.Equal(t, "telemetry-failure-archive/otlp_traces", directory.Scope)
		compressed = append(compressed, directory.CompressedFiles...)
	}
	require.Len(t, report.Directories, 2)
	var wantCompressed []string
	for _, archive := range archives {
		wantCompressed = append(wantCompressed, archive+".gz")
		require.NoFileExists(t, archive)
	}
	require.ElementsMatch(t, wantCompressed, compressed)
	permanent := filesWithSuffix(t, rejected, ".jsonl.gz")
	require.Len(t, permanent, 1)
	file, err := os.Open(permanent[0])
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(content), "archived-span")
}

// filesWithSuffixは、root以下で名前がsuffixで終わるfileのpathを返します。
func filesWithSuffix(t *testing.T, root, suffix string) []string {
	t.Helper()
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, suffix) {
			paths = append(paths, path)
		}
		return nil
	}))
	return paths
}
