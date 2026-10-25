package loki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestLokiLoggerWithIntegrationEnvironmentPushesToLocalLoki(t *testing.T) {
	settings := testutil.RequireIntegrationEnvironment(t, "LOKI_INTEGRATION_TEST")
	if strings.TrimSpace(settings["HETEROLOG_LOKI_ENDPOINT"]) == "" {
		t.Fatal("integration environment must explicitly configure HETEROLOG_LOKI_ENDPOINT")
	}
	// 他のbackend試験と同じく出力先を試験対象に固定し、専用fileのHETEROLOG_LOG_TYPEに依存せず
	// Lokiの接続設定を読み込ませます。
	t.Setenv("HETEROLOG_LOG_TYPE", "loki")

	appConfig, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with integration environment: %v", err)
	}
	if appConfig.LokiClient.Endpoint == "" {
		t.Fatal("integration environment must explicitly configure HETEROLOG_LOKI_ENDPOINT")
	}

	loggerConfig := NewLokiLoggerConfigAdapter(config.NewLokiLoggerConfig(&appConfig))
	clientConfig := config.NewLokiClientConfig(&appConfig)
	fmConfig := NewFlushManagerConfigAdapter(config.NewLokiFlushManagerConfig(&appConfig))
	resourceProvider := resource.NewResourceAdapter(config.NewLokiFlushManagerConfig(&appConfig).Resource)

	retrySrc := appConfig.Logger.LokiConfig.LokiRetryAbleClientConfig
	retryCfg := NewRetryConfigProvider(&pkglogger.LokiRetryableClientCaseTransportErrorConfig{
		RetryClientMode: retrySrc.RetryClientMode,
		MaxAttempts:     retrySrc.MaxAttempts,
		BaseDelay:       retrySrc.BaseDelay,
		MaxDelay:        retrySrc.MaxDelay,
	})
	lokiClient, err := infraloki.NewClient(
		*clientConfig,
		retryCfg,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("create loki client from integration environment: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sentAt := time.Now().UTC()
	message := fmt.Sprintf("loki integration environment push %d", sentAt.UnixNano())
	loggerBase := &LoggerBase{
		Clock:  fixedIntegrationLokiClock{t: sentAt},
		Tracer: observer.NopTracer{},
		Meter:  observer.NopMeter{},
	}

	logger, err := NewLokiLogger(
		ctx,
		loggerConfig,
		lokiClient,
		fmConfig,
		resourceProvider,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		loggerBase,
	)
	if err != nil {
		t.Fatalf("create loki logger from integration environment: %v", err)
	}
	t.Cleanup(func() {
		_ = logger.Close(context.Background())
	})

	if err := logger.testErrorWithStack(ctx, message, errors.New("loki integration test error")); err != nil {
		t.Fatalf("send loki log through logger: %v", err)
	}
	if err := logger.Close(ctx); err != nil {
		t.Fatalf("flush loki logger to actual Loki: %v", err)
	}

	labels := map[string]string{
		"appName":  appConfig.AppConfig.AppName,
		"loglevel": "error",
		"env":      appConfig.RuntimeEnv.CoreInfo.Environment,
	}
	waitForLokiLogLine(ctx, t, clientConfig, labels, message, sentAt.Add(-time.Minute), sentAt.Add(time.Minute))
}

type fixedIntegrationLokiClock struct {
	t time.Time
}

func (c fixedIntegrationLokiClock) Now() time.Time {
	return c.t
}

var _ clock.Clock = fixedIntegrationLokiClock{}

func waitForLokiLogLine(
	ctx context.Context,
	t *testing.T,
	cfg *pkgloki.Config,
	labels map[string]string,
	message string,
	start time.Time,
	end time.Time,
) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	var lastBody string
	for time.Now().Before(deadline) {
		found, body, err := queryLokiLogLine(ctx, cfg, labels, message, start, end)
		if err == nil && found {
			return
		}
		lastErr = err
		lastBody = body
		time.Sleep(500 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("pushed log was not queryable from Loki: %v; last response: %s", lastErr, lastBody)
	}
	t.Fatalf("pushed log was not found in Loki response; labels=%v message=%q last response=%s", labels, message, lastBody)
}

func queryLokiLogLine(
	ctx context.Context,
	cfg *pkgloki.Config,
	labels map[string]string,
	message string,
	start time.Time,
	end time.Time,
) (bool, string, error) {
	queryURL, err := lokiQueryRangeURL(cfg.Endpoint)
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
	httpClient := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL.String(), nil)
	if err != nil {
		return false, "", err
	}
	applyLokiQueryAuth(req, cfg)

	resp, err := httpClient.Do(req)
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

	var decoded lokiQueryRangeResponse
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

type lokiQueryRangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func lokiQueryRangeURL(endpoint string) (*url.URL, error) {
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

func applyLokiQueryAuth(req *http.Request, cfg *pkgloki.Config) {
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
