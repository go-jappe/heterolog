package logger

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	spoolstorehandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	spoollifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	spoolsenderclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/clickhouse"
	spoolsenderloki "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/loki"
	spoolsenderopensearch "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStdoutLoggerUsesConfigFixtureAndWritesStdout(t *testing.T) {
	appCfg, envFile := loadConfigFixtureForLoggerTestWithOverrides(t, map[string]string{
		"HETEROLOG_LOG_TYPE": "stdout",
	})
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.Stdout)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_APP_NAME"), cfg.Stdout.RuntimeConfig.AppName)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_TARGET"), cfg.Stdout.RuntimeConfig.Target)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_STDOUT_LOG_LEVEL"), cfg.Stdout.RuntimeConfig.LogLevel.String())

	ctx := context.Background()
	message := "stdout config fixture logger write " + uniqueLoggerEnvTestID()

	output := captureStdoutForLoggerTest(t, func() {
		lg, err := NewLogger(
			ctx,
			cfg,
			appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
			nil,
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
		)
		require.NoError(t, err)
		require.NoError(t, lg.Info(ctx, message))
		closeLoggerForTest(t, lg)
	})
	require.Contains(t, output, message)
}

func TestOpenSearchLoggerUsesActualDotEnvConfigAndStoresDocument(t *testing.T) {
	appCfg, envFile := loadLiveBackendEnvironment(t, "OPENSEARCH_INTEGRATION_TEST", "opensearch")
	// backendの接続設定は実際の.envを使う一方、試験が生成するlog、failure spool、
	// checkpointは試験専用directoryへ隔離する。productionと同じ
	// MapLoggerConfig／NewSpoolConfig経路を通し、failure spoolのwriterとrunnerを
	// NewLoggerで一体構築する。
	localRoot := t.TempDir()
	appCfg.FsstoreConfig.LogPath = filepath.Join(localRoot, "logs")
	appCfg.FsstoreConfig.SpoolPath = filepath.Join(localRoot, "spool")
	appCfg.FsstoreConfig.AppStatePath = filepath.Join(localRoot, "state")
	// 利用者の結合試験用設定がERRORをsamplingしていても、送信したlogを必ず保存させます。
	appCfg.Logger.OpenSearchConfig.SamplingConfig.SamplingErrorRate = 1
	require.NoError(t, appconfig.ValidateApplicationConfig(appCfg))
	cfg, err := appconfig.MapLoggerConfig(appCfg)
	require.NoError(t, err)
	require.NotNil(t, cfg.OpenSearch)
	require.NotNil(t, cfg.OpenSearchClient)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_OPENSEARCH_INDEX"), cfg.OpenSearch.OpenSearchFlushManagerConfig.Index)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_OPENSEARCH_ENDPOINT"), cfg.OpenSearchClient.Endpoint)
	require.Equal(t, 1, cfg.OpenSearch.OpenSearchLoggerBaseConfig.SamplingConfig.SamplingErrorRate)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	message := "opensearch actual env logger write " + uniqueLoggerEnvTestID()
	lg, err := NewLogger(
		ctx,
		cfg,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		appconfig.NewSpoolConfig(&appCfg),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, lg.Error(ctx, message))
	closeLoggerForTest(t, lg)

	waitForOpenSearchLogDocumentForLoggerTest(
		ctx,
		t,
		cfg.OpenSearchClient,
		cfg.OpenSearch.OpenSearchFlushManagerConfig.Index,
		message,
	)
}

func TestLokiLoggerUsesActualDotEnvConfigAndStoresEntry(t *testing.T) {
	appCfg, envFile := loadLiveBackendEnvironment(t, "LOKI_INTEGRATION_TEST", "loki")
	// backendの接続設定は実際の.envを使う一方、試験が生成するlog、failure archive、
	// failure spool、checkpointは試験専用directoryへ隔離する。productionと同じ
	// validation／mapping／constructor経路でfailure storeも含めて構築する。
	localRoot := t.TempDir()
	appCfg.FsstoreConfig.LogPath = filepath.Join(localRoot, "logs")
	appCfg.FsstoreConfig.SpoolPath = filepath.Join(localRoot, "spool")
	appCfg.FsstoreConfig.AppStatePath = filepath.Join(localRoot, "state")
	// 利用者の結合試験用設定がERRORをsamplingしていても、送信したlogを必ず保存させます。
	appCfg.Logger.LokiConfig.SamplingConfig.SamplingErrorRate = 1
	require.NoError(t, appconfig.ValidateApplicationConfig(appCfg))
	cfg, err := appconfig.MapLoggerConfig(appCfg)
	require.NoError(t, err)
	require.NotNil(t, cfg.Loki)
	require.NotNil(t, cfg.LokiClient)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_LOKI_ENDPOINT"), cfg.LokiClient.Endpoint)
	require.Equal(t, 1, cfg.Loki.LokiLoggerBaseConfig.SamplingConfig.SamplingErrorRate)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sentAt := time.Now().UTC()
	message := "loki actual env logger write " + uniqueLoggerEnvTestID()
	lg, err := NewLogger(
		ctx,
		cfg,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		appconfig.NewSpoolConfig(&appCfg),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, lg.Error(ctx, message))
	closeLoggerForTest(t, lg)

	labels := map[string]string{
		"appName":  appCfg.AppConfig.AppName,
		"loglevel": "error",
		"env":      appCfg.RuntimeEnv.CoreInfo.Environment,
	}
	waitForLokiLogLineForLoggerTest(ctx, t, cfg.LokiClient, labels, message, sentAt.Add(-time.Minute), sentAt.Add(time.Minute))
}

func TestClickHouseLoggerUsesActualDotEnvConfigAndStoresRow(t *testing.T) {
	appCfg, envFile := loadLiveBackendEnvironment(t, "CLICKHOUSE_INTEGRATION_TEST", "clickhouse")
	// backendの接続設定は実際の.envを使う一方、通常のfile出力、failure archive、
	// failure spoolは試験専用directoryへ隔離する。productionと同じ
	// MapLoggerConfig／NewSpoolConfig経路を通し、failure spoolのwriterとrunnerを
	// NewLoggerで一体構築する。
	stateRoot := t.TempDir()
	appCfg.FsstoreConfig.LogPath = filepath.Join(stateRoot, "logs")
	appCfg.FsstoreConfig.AppStatePath = filepath.Join(stateRoot, "state")
	appCfg.FsstoreConfig.SpoolPath = filepath.Join(stateRoot, "spool")
	// 利用者の結合試験用設定がERRORをsamplingしていても、送信したlogを必ず保存させます。
	appCfg.Logger.ClickHouseConfig.SamplingConfig.SamplingErrorRate = 1
	require.NoError(t, appconfig.ValidateApplicationConfig(appCfg))
	cfg, err := appconfig.MapLoggerConfig(appCfg)
	require.NoError(t, err)
	require.NotNil(t, cfg.ClickHouse)
	require.NotNil(t, cfg.ClickHouseClient)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_CLICKHOUSE_ENDPOINT"), cfg.ClickHouseClient.Endpoint)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_CLICKHOUSE_DATABASE"), cfg.ClickHouseClient.Database)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_CLICKHOUSE_TABLE"), cfg.ClickHouseClient.Table)
	require.Equal(t, 1, cfg.ClickHouse.ClickHouseLoggerBaseConfig.SamplingConfig.SamplingErrorRate)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ensureClickHouseLogTableForLoggerTest(ctx, t, cfg.ClickHouseClient)

	message := "clickhouse actual env logger write " + uniqueLoggerEnvTestID()
	lg, err := NewLogger(
		ctx,
		cfg,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		appconfig.NewSpoolConfig(&appCfg),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, lg.Error(ctx, message))
	closeLoggerForTest(t, lg)

	waitForClickHouseLogRowForLoggerTest(ctx, t, cfg.ClickHouseClient, message)
}

func TestOpenSearchSpoolUsesActualDotEnvConfigActiveAndResendsToBackend(t *testing.T) {
	appCfg, envFile := loadLiveBackendEnvironment(t, "OPENSEARCH_INTEGRATION_TEST", "opensearch")
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.OpenSearch)
	require.NotNil(t, cfg.OpenSearchClient)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_OPENSEARCH_INDEX"), cfg.OpenSearch.OpenSearchFlushManagerConfig.Index)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_OPENSEARCH_ENDPOINT"), cfg.OpenSearchClient.Endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := uniqueLoggerEnvTestID()
	now := time.Now().UTC()
	message := "opensearch actual env spool resend " + id
	spoolCfg := loggerEnvBackendSpoolConfig{
		spoolPath: t.TempDir(),
		target:    "logger-env-spool-" + id,
		dir:       "opensearch",
	}
	clock := fixedLoggerEnvSpoolClock{now: now}
	activeStore := storeLoggerEnvMockFailureToActiveSpool(
		ctx,
		t,
		spoolCfg,
		clock,
		"opensearch",
		"opensearch-spool-"+id,
		map[string]any{
			"index": cfg.OpenSearch.OpenSearchFlushManagerConfig.Index,
			"id":    "opensearch-spool-" + id,
			"doc": map[string]any{
				"timestamp": now.Format(time.RFC3339Nano),
				"level":     "ERROR",
				"message":   message,
				"line":      message,
				"app_name":  appCfg.AppConfig.AppName,
				"env":       appCfg.RuntimeEnv.CoreInfo.Environment,
			},
			"op": string(pkgopensearch.WriteOpCreate),
		},
	)

	require.NotNil(t, cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig)
	osClient, err := infraopensearch.NewClient(
		infraopensearch.NewOpenSearchClientConfigAdapter(cfg.OpenSearchClient),
		opensearchlogger.NewRetryConfigProvider(cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	spoolSender, err := spoolsenderopensearch.NewOpensearchSpoolSender(
		fixedLoggerEnvSpoolClock{now: now.Add(time.Minute)},
		osClient,
		cfg.OpenSearch.MaxBulkRequestBytes,
	)
	require.NoError(t, err)

	doneRecords := runLoggerEnvSpoolResendOnce(ctx, t, spoolCfg, activeStore, spoolSender, "opensearch")
	require.Len(t, doneRecords, 1)
	waitForOpenSearchLogDocumentForLoggerTest(
		ctx,
		t,
		cfg.OpenSearchClient,
		cfg.OpenSearch.OpenSearchFlushManagerConfig.Index,
		message,
	)
}

func TestLokiSpoolUsesActualDotEnvConfigActiveAndResendsToBackend(t *testing.T) {
	appCfg, envFile := loadLiveBackendEnvironment(t, "LOKI_INTEGRATION_TEST", "loki")
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.Loki)
	require.NotNil(t, cfg.LokiClient)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_LOKI_ENDPOINT"), cfg.LokiClient.Endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := uniqueLoggerEnvTestID()
	now := time.Now().UTC()
	message := "loki actual env spool resend " + id
	labels := map[string]string{
		"appName":  appCfg.AppConfig.AppName,
		"loglevel": "error",
		"env":      appCfg.RuntimeEnv.CoreInfo.Environment,
	}
	spoolCfg := loggerEnvBackendSpoolConfig{
		spoolPath: t.TempDir(),
		target:    "logger-env-spool-" + id,
		dir:       "loki",
	}
	clock := fixedLoggerEnvSpoolClock{now: now}
	activeStore := storeLoggerEnvMockFailureToActiveSpool(
		ctx,
		t,
		spoolCfg,
		clock,
		"loki",
		"loki-spool-"+id,
		map[string]any{
			"timestamp": now.Format(time.RFC3339Nano),
			"labels":    labels,
			"level":     "ERROR",
			"message":   message,
			"line":      message,
		},
	)

	require.NotNil(t, cfg.Loki.LokiRetryableClientCaseTransportErrorConfig)
	lokiClient, err := infraloki.NewClient(
		*cfg.LokiClient,
		lokilogger.NewRetryConfigProvider(cfg.Loki.LokiRetryableClientCaseTransportErrorConfig),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	spoolSender, err := spoolsenderloki.NewLokiSpoolSender(
		fixedLoggerEnvSpoolClock{now: now.Add(time.Minute)},
		lokiClient,
		infraloki.PushLimits{
			MaxLineBytes:        cfg.Loki.MaxLineBytes,
			MaxPushRequestBytes: cfg.Loki.MaxPushRequestBytes,
		},
	)
	require.NoError(t, err)

	doneRecords := runLoggerEnvSpoolResendOnce(ctx, t, spoolCfg, activeStore, spoolSender, "loki")
	require.Len(t, doneRecords, 1)
	waitForLokiLogLineForLoggerTest(ctx, t, cfg.LokiClient, labels, message, now.Add(-time.Minute), time.Now().UTC().Add(time.Minute))
}

func TestClickHouseSpoolUsesActualDotEnvConfigActiveAndResendsToBackend(t *testing.T) {
	appCfg, envFile := loadLiveBackendEnvironment(t, "CLICKHOUSE_INTEGRATION_TEST", "clickhouse")
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.ClickHouse)
	require.NotNil(t, cfg.ClickHouseClient)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_CLICKHOUSE_ENDPOINT"), cfg.ClickHouseClient.Endpoint)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_CLICKHOUSE_DATABASE"), cfg.ClickHouseClient.Database)
	require.Equal(t, requiredDotEnvStringForLoggerTest(t, envFile, "HETEROLOG_CLICKHOUSE_TABLE"), cfg.ClickHouseClient.Table)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureClickHouseLogTableForLoggerTest(ctx, t, cfg.ClickHouseClient)

	id := uniqueLoggerEnvTestID()
	now := time.Now().UTC()
	message := "clickhouse actual env spool resend " + id
	spoolCfg := loggerEnvBackendSpoolConfig{
		spoolPath: t.TempDir(),
		target:    "logger-env-spool-" + id,
		dir:       "clickhouse",
	}
	clock := fixedLoggerEnvSpoolClock{now: now}
	activeStore := storeLoggerEnvMockFailureToActiveSpool(
		ctx,
		t,
		spoolCfg,
		clock,
		"clickhouse",
		"clickhouse-spool-"+id,
		map[string]any{
			"timestamp":         now.Format(time.RFC3339Nano),
			"level":             "ERROR",
			"message":           message,
			"line":              message,
			"request_id":        "clickhouse-spool-" + id,
			"app_name":          appCfg.AppConfig.AppName,
			"host_name":         appCfg.RuntimeEnv.CoreInfo.HostName,
			"environment":       appCfg.RuntimeEnv.CoreInfo.Environment,
			"runtime_type":      appCfg.RuntimeEnv.CoreInfo.RuntimeType,
			"execution_unit_id": appCfg.RuntimeEnv.CoreInfo.RuntimeInstanceID,
			"version":           appCfg.AppConfig.AppVersion,
			"source":            "clickhouse-spool-env-test",
			infraclickhouse.DurablePayloadFieldRecordID:   "clickhouse-spool-record-" + id,
			infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			pkgclickhouse.SettingInsertDeduplicationToken: "clickhouse-spool-token-" + id,
		},
	)

	require.NotNil(t, cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig)
	clickHouseClient, err := infraclickhouse.NewClient(
		*cfg.ClickHouseClient,
		clickhouselogger.NewRetryConfigProvider(cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	spoolSender, err := spoolsenderclickhouse.NewClickHouseSpoolSender(
		fixedLoggerEnvSpoolClock{now: now.Add(time.Minute)},
		clickHouseClient,
		cfg.ClickHouse.MaxInsertRequestBytes,
	)
	require.NoError(t, err)

	doneRecords := runLoggerEnvSpoolResendOnce(ctx, t, spoolCfg, activeStore, spoolSender, "clickhouse")
	require.Len(t, doneRecords, 1)
	waitForClickHouseLogRowForLoggerTest(ctx, t, cfg.ClickHouseClient, message)
}

func uniqueLoggerEnvTestID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func captureStdoutForLoggerTest(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)

	readDone := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		body, readErr := io.ReadAll(reader)
		readDone <- struct {
			body []byte
			err  error
		}{body: body, err: readErr}
	}()

	closed := false
	os.Stdout = writer
	defer func() {
		os.Stdout = original
		if !closed {
			_ = writer.Close()
			<-readDone
		}
		_ = reader.Close()
	}()

	fn()

	require.NoError(t, writer.Close())
	closed = true
	os.Stdout = original

	result := <-readDone
	require.NoError(t, result.err)
	return string(result.body)
}

func waitForOpenSearchLogDocumentForLoggerTest(
	ctx context.Context,
	t *testing.T,
	cfg *pkgopensearch.Config,
	index string,
	message string,
) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	var lastBody string
	var lastErr error
	for time.Now().Before(deadline) {
		_ = refreshOpenSearchIndexForLoggerTest(ctx, cfg, index)
		found, body, err := queryOpenSearchLogDocumentForLoggerTest(ctx, cfg, index, message)
		if err == nil && found {
			return
		}
		lastBody = body
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("opensearch log was not queryable: %v; last response: %s", lastErr, lastBody)
	}
	t.Fatalf("opensearch log was not found; index=%q message=%q last response=%s", index, message, lastBody)
}

func refreshOpenSearchIndexForLoggerTest(ctx context.Context, cfg *pkgopensearch.Config, index string) error {
	endpoint, err := openSearchEndpointURLForLoggerTest(cfg.Endpoint, index+"/_refresh")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	if err := applyOpenSearchAuthForLoggerTest(req, cfg); err != nil {
		return err
	}
	resp, err := openSearchHTTPClientForLoggerTest(cfg).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("opensearch refresh returned %s", resp.Status)
	}
	return nil
}

func queryOpenSearchLogDocumentForLoggerTest(
	ctx context.Context,
	cfg *pkgopensearch.Config,
	index string,
	message string,
) (bool, string, error) {
	endpoint, err := openSearchEndpointURLForLoggerTest(cfg.Endpoint, index+"/_search")
	if err != nil {
		return false, "", err
	}
	query := map[string]any{
		"size": 10,
		"query": map[string]any{
			"multi_match": map[string]any{
				"query":  message,
				"type":   "phrase",
				"fields": []string{"message", "body", "line"},
			},
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return false, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, bytes.NewReader(body))
	if err != nil {
		return false, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := applyOpenSearchAuthForLoggerTest(req, cfg); err != nil {
		return false, "", err
	}

	resp, err := openSearchHTTPClientForLoggerTest(cfg).Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	respText := string(respBody)
	if readErr != nil {
		return false, respText, readErr
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, respText, fmt.Errorf("opensearch search returned %s", resp.Status)
	}

	var decoded openSearchSearchResponseForLoggerTest
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return false, respText, err
	}
	for _, hit := range decoded.Hits.Hits {
		if openSearchSourceContainsMessageForLoggerTest(hit.Source, message) {
			return true, respText, nil
		}
	}
	return false, respText, nil
}

type openSearchSearchResponseForLoggerTest struct {
	Hits struct {
		Hits []struct {
			Source map[string]any `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func openSearchSourceContainsMessageForLoggerTest(source map[string]any, message string) bool {
	for _, key := range []string{"message", "body", "line"} {
		if strings.Contains(fmt.Sprint(source[key]), message) {
			return true
		}
	}
	body, err := json.Marshal(source)
	return err == nil && strings.Contains(string(body), message)
}

func openSearchEndpointURLForLoggerTest(endpoint string, path string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("opensearch endpoint must include scheme and host")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/" + strings.TrimLeft(path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func openSearchHTTPClientForLoggerTest(cfg *pkgopensearch.Config) *http.Client {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.SkipVerifySSL},
		},
	}
}

func applyOpenSearchAuthForLoggerTest(req *http.Request, cfg *pkgopensearch.Config) error {
	switch cfg.AuthConfig.Type {
	case pkgopensearch.AuthTypeBasic:
		basic, ok := cfg.AuthConfig.Config.(pkgopensearch.BasicAuthConfig)
		if !ok {
			return fmt.Errorf("invalid OpenSearch basic auth config")
		}
		req.SetBasicAuth(basic.Username, basic.Password)
	case pkgopensearch.AuthTypeAPIKey:
		apiKey, ok := cfg.AuthConfig.Config.(pkgopensearch.APIKeyAuthConfig)
		if !ok {
			return fmt.Errorf("invalid OpenSearch API key auth config")
		}
		token := base64.StdEncoding.EncodeToString([]byte(apiKey.ID + ":" + apiKey.Secret))
		req.Header.Set("Authorization", "ApiKey "+token)
	case pkgopensearch.AuthTypeSAML:
		saml, ok := cfg.AuthConfig.Config.(pkgopensearch.SAMLAuthConfig)
		if !ok {
			return fmt.Errorf("invalid OpenSearch SAML auth config")
		}
		req.Header.Set("Authorization", "Bearer "+saml.Token)
	case pkgopensearch.AuthTypeOIDC:
		oidc, ok := cfg.AuthConfig.Config.(pkgopensearch.OIDCAuthConfig)
		if !ok {
			return fmt.Errorf("invalid OpenSearch OIDC auth config")
		}
		req.Header.Set("Authorization", "Bearer "+oidc.Token)
	case pkgopensearch.AuthTypeNone, pkgopensearch.AuthTypeCertificate:
		return nil
	default:
		return fmt.Errorf("unsupported OpenSearch auth type: %s", cfg.AuthConfig.Type)
	}
	return nil
}

func waitForLokiLogLineForLoggerTest(
	ctx context.Context,
	t *testing.T,
	cfg *pkgloki.Config,
	labels map[string]string,
	message string,
	start time.Time,
	end time.Time,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	var lastBody string
	var lastErr error
	for time.Now().Before(deadline) {
		found, body, err := queryLokiLogLineForLoggerTest(ctx, cfg, labels, message, start, end)
		if err == nil && found {
			return
		}
		lastBody = body
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("loki log was not queryable: %v; last response: %s", lastErr, lastBody)
	}
	t.Fatalf("loki log was not found; labels=%v message=%q last response=%s", labels, message, lastBody)
}

func queryLokiLogLineForLoggerTest(
	ctx context.Context,
	cfg *pkgloki.Config,
	labels map[string]string,
	message string,
	start time.Time,
	end time.Time,
) (bool, string, error) {
	queryURL, err := lokiQueryRangeURLForLoggerTest(cfg.Endpoint)
	if err != nil {
		return false, "", err
	}

	query := fmt.Sprintf(
		`{appName=%s,loglevel=%s,env=%s} |= %s`,
		strconv.Quote(labels["appName"]),
		strconv.Quote(labels["loglevel"]),
		strconv.Quote(labels["env"]),
		strconv.Quote(message),
	)
	values := queryURL.Query()
	values.Set("query", query)
	values.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	values.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	values.Set("limit", "20")
	values.Set("direction", "backward")
	queryURL.RawQuery = values.Encode()

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL.String(), nil)
	if err != nil {
		return false, "", err
	}
	applyLokiQueryAuthForLoggerTest(req, cfg)

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()

	bodyBytes, readErr := io.ReadAll(resp.Body)
	body := string(bodyBytes)
	if readErr != nil {
		return false, body, readErr
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, body, fmt.Errorf("loki query_range returned %s", resp.Status)
	}

	var decoded lokiQueryRangeResponseForLoggerTest
	if err := json.Unmarshal(bodyBytes, &decoded); err != nil {
		return false, body, err
	}
	if decoded.Status != "success" {
		return false, body, fmt.Errorf("loki query_range status is %q", decoded.Status)
	}
	for _, result := range decoded.Data.Result {
		for _, value := range result.Values {
			if len(value) >= 2 && strings.Contains(value[1], message) {
				return true, body, nil
			}
		}
	}
	return false, body, nil
}

type lokiQueryRangeResponseForLoggerTest struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func lokiQueryRangeURLForLoggerTest(endpoint string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return nil, fmt.Errorf("parse loki endpoint: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("loki endpoint must include scheme and host")
	}

	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case path == "":
		parsed.Path = "/loki/api/v1/query_range"
	case strings.HasSuffix(path, "/loki/api/v1/push"):
		parsed.Path = strings.TrimSuffix(path, "/push") + "/query_range"
	default:
		return nil, fmt.Errorf("loki endpoint path must be empty or end with /loki/api/v1/push")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed, nil
}

func applyLokiQueryAuthForLoggerTest(req *http.Request, cfg *pkgloki.Config) {
	for key, value := range cfg.Headers {
		req.Header.Set(key, value)
	}
	if cfg.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", cfg.TenantID)
	}
	if cfg.BasicAuth != nil {
		req.SetBasicAuth(cfg.BasicAuth.Username, cfg.BasicAuth.Password)
	}
	if cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	}
}

func waitForClickHouseLogRowForLoggerTest(
	ctx context.Context,
	t *testing.T,
	cfg *pkgclickhouse.Config,
	message string,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	var lastBody string
	var lastErr error
	for time.Now().Before(deadline) {
		count, body, err := queryClickHouseLogCountForLoggerTest(ctx, cfg, message)
		if err == nil && count > 0 {
			return
		}
		lastBody = body
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("clickhouse log row was not queryable: %v; last response: %s", lastErr, lastBody)
	}
	t.Fatalf("clickhouse log row was not found; message=%q last response=%s", message, lastBody)
}

func ensureClickHouseLogTableForLoggerTest(ctx context.Context, t *testing.T, cfg *pkgclickhouse.Config) {
	t.Helper()

	if strings.TrimSpace(cfg.Database) != "" {
		// 作成前のdatabaseはX-ClickHouse-Databaseで選択できず、ClickHouseがUNKNOWN_DATABASE
		// （HTTP 404）を返すため、databaseを選択せずに作成します。
		bootstrap := *cfg
		bootstrap.Database = ""
		_, err := executeClickHouseQueryForLoggerTest(ctx, &bootstrap, "CREATE DATABASE IF NOT EXISTS "+clickHouseQuoteIdentifierForLoggerTest(cfg.Database))
		require.NoError(t, err)
	}

	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
timestamp String,
level String,
message String,
line String,
request_id String,
trace_id String,
span_id String,
error String,
app_name String,
host_name String,
environment String,
runtime_type String,
execution_unit_id String,
version String,
source String
) ENGINE = MergeTree ORDER BY tuple()`, clickHouseTableExpressionForLoggerTest(cfg.Database, cfg.Table))
	_, err := executeClickHouseQueryForLoggerTest(ctx, cfg, createTable)
	require.NoError(t, err)
}

func queryClickHouseLogCountForLoggerTest(
	ctx context.Context,
	cfg *pkgclickhouse.Config,
	message string,
) (int64, string, error) {
	query := fmt.Sprintf(
		"SELECT count() FROM %s WHERE message = %s",
		clickHouseTableExpressionForLoggerTest(cfg.Database, cfg.Table),
		clickHouseQuoteStringForLoggerTest(message),
	)
	body, err := executeClickHouseQueryForLoggerTest(ctx, cfg, query)
	if err != nil {
		return 0, body, err
	}
	count, err := strconv.ParseInt(strings.TrimSpace(body), 10, 64)
	if err != nil {
		return 0, body, err
	}
	return count, body, nil
}

func executeClickHouseQueryForLoggerTest(ctx context.Context, cfg *pkgclickhouse.Config, query string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(cfg.Endpoint))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("clickhouse endpoint must include scheme and host")
	}

	values := parsed.Query()
	values.Set("query", query)
	parsed.RawQuery = values.Encode()

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), nil)
	if err != nil {
		return "", err
	}
	if cfg.Database != "" {
		req.Header.Set("X-ClickHouse-Database", cfg.Database)
	}
	if cfg.Username != "" || cfg.Password != "" {
		req.SetBasicAuth(cfg.Username, cfg.Password)
	}
	for key, value := range cfg.Headers {
		req.Header.Set(key, value)
	}

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, readErr := io.ReadAll(resp.Body)
	body := string(bodyBytes)
	if readErr != nil {
		return body, readErr
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return body, fmt.Errorf("clickhouse query returned %s", resp.Status)
	}
	return body, nil
}

func clickHouseTableExpressionForLoggerTest(database string, table string) string {
	if strings.TrimSpace(database) != "" {
		return clickHouseQuoteIdentifierForLoggerTest(database) + "." + clickHouseQuoteIdentifierForLoggerTest(table)
	}
	parts := strings.Split(table, ".")
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
		return clickHouseQuoteIdentifierForLoggerTest(parts[0]) + "." + clickHouseQuoteIdentifierForLoggerTest(parts[1])
	}
	return clickHouseQuoteIdentifierForLoggerTest(table)
}

func clickHouseQuoteIdentifierForLoggerTest(identifier string) string {
	return "`" + strings.ReplaceAll(strings.TrimSpace(identifier), "`", "``") + "`"
}

func clickHouseQuoteStringForLoggerTest(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`'`, `\'`,
		"\n", `\n`,
		"\r", `\r`,
		"\t", `\t`,
	)
	return "'" + replacer.Replace(value) + "'"
}

// Config-based live tests must read the dedicated integration file, not the
// deterministic local fixture used by the ordinary configuration tests.
func loadLiveBackendEnvironment(t *testing.T, optIn, backend string) (appconfig.AppConfig, map[string]string) {
	t.Helper()
	envFile := testutil.RequireIntegrationEnvironment(t, optIn)
	require.NotEmpty(t, strings.TrimSpace(envFile["HETEROLOG_"+strings.ToUpper(backend)+"_ENDPOINT"]), "the integration file must explicitly configure the selected backend endpoint")
	t.Setenv("HETEROLOG_LOG_TYPE", backend)
	t.Setenv("HETEROLOG_APP_STATE_DIR", t.TempDir())
	cfg, err := appconfig.LoadConfig()
	require.NoError(t, err)
	return cfg, envFile
}

type loggerEnvBackendSpoolConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c loggerEnvBackendSpoolConfig) SpoolPath() string { return c.spoolPath }
func (c loggerEnvBackendSpoolConfig) Target() string    { return c.target }
func (c loggerEnvBackendSpoolConfig) Dir() string       { return c.dir }
func (c loggerEnvBackendSpoolConfig) FileNameFormat() string {
	return ""
}
func (c loggerEnvBackendSpoolConfig) FileDateFormat() string {
	return "20060102"
}
func (c loggerEnvBackendSpoolConfig) MaxWriteAttempts() int {
	return 1
}
func (c loggerEnvBackendSpoolConfig) FileOperationRetryWaitTime() time.Duration {
	return time.Millisecond
}
func (c loggerEnvBackendSpoolConfig) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return loggerEnvBackendSpoolOverloadConfig{}
}

type loggerEnvBackendSpoolOverloadConfig struct{}

func (loggerEnvBackendSpoolOverloadConfig) OverloadPolicy() pkgdurable.OverloadPolicy {
	return pkgdurable.OverloadPolicy("drop")
}
func (loggerEnvBackendSpoolOverloadConfig) SpillDestination() pkgdurable.SpillDestination {
	return pkgdurable.SpillDestination("stdout")
}
func (loggerEnvBackendSpoolOverloadConfig) MaxCallsSinceScan() uint64 { return 0 }
func (loggerEnvBackendSpoolOverloadConfig) StatsScanInterval() time.Duration {
	return 0
}
func (loggerEnvBackendSpoolOverloadConfig) MaxDirSize() uint64                 { return 0 }
func (loggerEnvBackendSpoolOverloadConfig) ActiveBacklogWarnAfterDays() int    { return 0 }
func (loggerEnvBackendSpoolOverloadConfig) MinAvailableDiskSpace() uint64      { return 0 }
func (loggerEnvBackendSpoolOverloadConfig) UseMemoryPressureForOverload() bool { return false }
func (loggerEnvBackendSpoolOverloadConfig) MemoryCriticalRatio() float64       { return 0 }
func (loggerEnvBackendSpoolOverloadConfig) MinAvailableMemoryBytes() uint64    { return 0 }
func (loggerEnvBackendSpoolOverloadConfig) MemoryCriticalConsecutiveHits() int { return 0 }

type fixedLoggerEnvSpoolClock struct {
	now time.Time
}

func (c fixedLoggerEnvSpoolClock) Now() time.Time {
	return c.now
}

func storeLoggerEnvMockFailureToActiveSpool(
	ctx context.Context,
	t *testing.T,
	cfg loggerEnvBackendSpoolConfig,
	clock fixedLoggerEnvSpoolClock,
	backend string,
	requestID string,
	data map[string]any,
) *fsstorestore.SpoolStore {
	t.Helper()
	store, err := fsstorestore.NewSpoolStore(clock, cfg, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	handler := spoolstorehandler.NewSpoolStoreHandler(
		ctx,
		clock,
		store,
		cfg,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		spoolstorehandler.InternalLogHandlers{},
	)
	require.NoError(t, handler.StoreSendResultByState(ctx, loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     backend,
			RequestID:   requestID,
			AttemptedAt: clock.now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			{
				FailedAt:      clock.now,
				Operation:     "spool_integration_seed",
				StatusCode:    http.StatusServiceUnavailable,
				FailureCode:   "mock_retryable_failure",
				FailureReason: "mock initial backend failure for spool integration",
				Payload:       data,
			},
		},
	}))

	activeRecords := readLoggerEnvSpoolRecords(t, loggerEnvSpoolStateRoot(cfg, pkgspool.StateActive))
	require.Len(t, activeRecords, 1)
	assertLoggerEnvDurableSpoolRecord(t, activeRecords[0], backend, requestID, clock.now, data)
	return store
}

func assertLoggerEnvDurableSpoolRecord(
	t *testing.T,
	record pkgspool.SpoolRecord,
	backend string,
	requestID string,
	occurredAt time.Time,
	wantData map[string]any,
) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.Unmarshal(record.Payload, &payload))
	require.NotNil(t, payload)
	assert.Equal(t, requestID, record.RequestID)
	assert.True(t, record.OccurredAt.Equal(occurredAt))
	assert.Equal(t, "spool_integration_seed", record.InitialFailureOperation)
	assert.Equal(t, http.StatusServiceUnavailable, record.InitialFailureStatusCode)
	assert.Equal(t, "mock_retryable_failure", record.InitialFailureCode)
	assert.Equal(t, "mock initial backend failure for spool integration", record.InitialFailureReason)
	assert.Equal(t, record.InitialFailureReason, record.LastFailureReason)

	wantJSON, err := json.Marshal(wantData)
	require.NoError(t, err)
	gotJSON, err := json.Marshal(payload)
	require.NoError(t, err)
	assert.JSONEq(t, string(wantJSON), string(gotJSON))
}

func runLoggerEnvSpoolResendOnce(
	ctx context.Context,
	t *testing.T,
	cfg loggerEnvBackendSpoolConfig,
	activeStore *fsstorestore.SpoolStore,
	sender spoolworker.Sender,
	backend string,
) []pkgspool.SpoolRecord {
	t.Helper()

	stateStore, err := fsworker.NewStateStore(cfg)
	require.NoError(t, err)
	worker, err := spoolworker.New(
		stateStore,
		loggerEnvActiveSpoolWriter{store: activeStore},
		sender,
	)
	require.NoError(t, err)
	worker.TargetBackendService = backend

	lifecycleManager, err := lifecycle.NewManager(loggerEnvSpoolLifecycleStoreStub{}, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		worker,
		lifecycleManager,
		loggerEnvSpoolRunnerConfigStub{},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 1, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)

	assert.Empty(t, readLoggerEnvSpoolRecords(t, loggerEnvSpoolStateRoot(cfg, pkgspool.StateActive)))
	assert.Empty(t, readLoggerEnvSpoolRecords(t, loggerEnvSpoolStateRoot(cfg, pkgspool.StateProcessing)))

	doneRecords := readLoggerEnvSpoolRecords(t, loggerEnvSpoolStateRoot(cfg, pkgspool.StateDone))
	require.Len(t, doneRecords, 1)
	assert.False(t, doneRecords[0].FinalizedAt.IsZero())
	assert.False(t, doneRecords[0].FirstResendAttemptedAt.IsZero())
	assert.Equal(t, 1, doneRecords[0].ResendAttemptCount)
	return doneRecords
}

func loggerEnvSpoolStateRoot(cfg loggerEnvBackendSpoolConfig, state pkgspool.State) string {
	return filepath.Join(cfg.spoolPath, cfg.target, "spool", cfg.dir, string(state))
}

func readLoggerEnvSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			records = append(records, record)
		}
		return scanner.Err()
	})
	require.NoError(t, err)
	return records
}

type loggerEnvActiveSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w loggerEnvActiveSpoolWriter) StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}

type loggerEnvSpoolRunnerConfigStub struct{}

func (loggerEnvSpoolRunnerConfigStub) Interval() time.Duration { return time.Millisecond }
func (loggerEnvSpoolRunnerConfigStub) StopOnError() bool       { return true }

type loggerEnvSpoolLifecycleStoreStub struct{}

func (loggerEnvSpoolLifecycleStoreStub) PurgeDoneIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (loggerEnvSpoolLifecycleStoreStub) PurgeDeadIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (loggerEnvSpoolLifecycleStoreStub) PurgeQuarantineIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}
