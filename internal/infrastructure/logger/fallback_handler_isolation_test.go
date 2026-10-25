package logger

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	filelogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/require"
)

func TestGetHandlerKeepsFallbackBatchesSeparateBetweenLevels(t *testing.T) {
	recorder := &fallbackIsolationRequestRecorder{}
	backendConfig := fallbackIsolationClickHouseConfig()
	available := getHandler(pkglogger.LoggerConfig{
		ClickHouse:       &backendConfig,
		ClickHouseClient: fallbackIsolationClickHouseClient(recorder),
	}, streamrouting.None, nil, nil)
	owner := newFallbackHandlerCloserOwner()
	owner.Register("level-isolation", available)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.Len(t, available.OnInfo, 1)
	require.Len(t, available.OnWarn, 1)
	require.NotSame(t, available.OnInfo[0].Closer, available.OnWarn[0].Closer)

	for _, named := range []logging.NamedLoggerHandler{available.OnInfo[0], available.OnWarn[0]} {
		named.Handler(context.Background(), &logging.LogEntry{
			Level:   named.Level,
			Message: "separate-" + named.Level.String(),
		}, observer.NopTracer{}, observer.NopMeter{})
	}
	// 同じ設定providerを渡しても、異なるlevelの各一件が一つのbatchへ混ざりません。
	require.NoError(t, owner.Close())
	bodies := recorder.snapshot()
	require.Len(t, bodies, 2)
	for _, body := range bodies {
		info := strings.Contains(body, "separate-"+pkglogger.LogLevelInfo.String())
		warn := strings.Contains(body, "separate-"+pkglogger.LogLevelWarn.String())
		require.NotEqual(t, info, warn, "one batch must contain exactly one level")
	}
}

func TestBuildLoggersKeepsFallbackStatePerSourceAndSharesFileLogger(t *testing.T) {
	recorder := &fallbackIsolationRequestRecorder{}
	handlerCfg := fallbackIsolationHandlerConfig()

	fileCfg := newActivationFileConfig(t, "sync-fallback-shared-file")
	stdoutCfg := newActivationStdoutConfig()
	stdoutCfg.HandlerConfig = handlerCfg
	slackCfg := fallbackIsolationSlackConfig(handlerCfg)
	clickHouseCfg := fallbackIsolationClickHouseConfig()
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputSlack,
			pkglogger.LogOutputStdout,
		},
		File:             fileCfg,
		Stdout:           stdoutCfg,
		Slack:            &slackCfg,
		ClickHouse:       &clickHouseCfg,
		ClickHouseClient: fallbackIsolationClickHouseClient(recorder),
	}
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	fileLoggerSet, err := newFileLoggerSet(cfg.File, tracer, meter)
	require.NoError(t, err)
	internalDestinations, err := newInternalLogDestinationRuntime(
		cfg,
		streamrouting.None,
		fileLoggerSet,
		syncInternalLogFileDirectoryScopeLabel,
		tracer,
		meter,
	)
	require.NoError(t, err)
	fallbackCloserOwner := newFallbackHandlerCloserOwner()

	children, closers, err := buildLoggers(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		tracer,
		meter,
		fileLoggerSet,
		internalDestinations,
		fallbackCloserOwner,
		pkglogger.LoggerContractSync,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		for index := len(closers) - 1; index >= 0; index-- {
			require.NoError(t, closers[index].Close())
		}
		require.NoError(t, fallbackCloserOwner.Close())
		require.NoError(t, internalDestinations.Close())
		require.NoError(t, fileLoggerSet.Close())
	})

	stdoutSource, slackSource := fallbackIsolationSyncSources(t, children)
	invokeFallbackIsolationInfo(
		stdoutSource.OnInfo,
		common.SourceNameStdoutLogger,
		"stdout-first",
		tracer,
		meter,
	)
	invokeFallbackIsolationInfo(
		slackSource.OnInfo,
		common.SourceNameSlackLogger,
		"slack-first",
		tracer,
		meter,
	)
	requestsAfterFirstPair := recorder.count()

	invokeFallbackIsolationInfo(
		stdoutSource.OnInfo,
		common.SourceNameStdoutLogger,
		"stdout-second",
		tracer,
		meter,
	)
	invokeFallbackIsolationInfo(
		slackSource.OnInfo,
		common.SourceNameSlackLogger,
		"slack-second",
		tracer,
		meter,
	)

	require.Zero(t, requestsAfterFirstPair, "different source loggers must not fill one shared fallback batch")
	require.NoError(t, fallbackCloserOwner.Close())
	requireFallbackIsolationBatches(t, recorder.snapshot())
	requireFallbackIsolationSharedFile(
		t,
		fileLoggerSet,
		[]string{"stdout-first", "slack-first", "stdout-second", "slack-second"},
	)
}

func TestBuildDeliveryLoggersKeepsFallbackStatePerSourceAndSharesFileLogger(t *testing.T) {
	recorder := &fallbackIsolationRequestRecorder{}
	handlerCfg := fallbackIsolationHandlerConfig()

	fileCfg := newCloseOwnershipDeliveryFileConfig(
		t.TempDir(),
		"delivery-fallback-shared-file",
	)
	stdoutCfg := newActivationDeliveryStdoutConfig()
	stdoutCfg.HandlerConfig = handlerCfg
	slackCfg := fallbackIsolationDeliverySlackConfig(handlerCfg)
	clickHouseCfg := fallbackIsolationDeliveryClickHouseConfig()
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputSlack,
			pkglogger.LogOutputStdout,
		},
		File:             fileCfg,
		Stdout:           stdoutCfg,
		Slack:            &slackCfg,
		ClickHouse:       &clickHouseCfg,
		ClickHouseClient: fallbackIsolationClickHouseClient(recorder),
	}
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	fileLoggerSet, err := newDeliveryFileLoggerSet(cfg.File, tracer, meter)
	require.NoError(t, err)
	internalDestinations, err := newDeliveryInternalLogDestinationRuntime(
		cfg,
		streamrouting.None,
		fileLoggerSet,
		deliveryInternalLogFileDirectoryScopeLabel,
		tracer,
		meter,
	)
	require.NoError(t, err)
	fallbackCloserOwner := newFallbackHandlerCloserOwner()

	children, closers, err := buildDeliveryLoggers(
		context.Background(),
		cfg,
		streamrouting.None,
		tracer,
		meter,
		fileLoggerSet,
		internalDestinations,
		fallbackCloserOwner,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		for index := len(closers) - 1; index >= 0; index-- {
			require.NoError(t, closers[index].Close())
		}
		require.NoError(t, fallbackCloserOwner.Close())
		require.NoError(t, internalDestinations.Close())
		require.NoError(t, fileLoggerSet.Close())
	})

	stdoutSource, slackSource := fallbackIsolationDeliverySources(t, children)
	invokeFallbackIsolationInfo(
		stdoutSource.OnInfo,
		common.SourceNameStdoutLogger,
		"delivery-stdout-first",
		tracer,
		meter,
	)
	invokeFallbackIsolationInfo(
		slackSource.OnInfo,
		common.SourceNameSlackLogger,
		"delivery-slack-first",
		tracer,
		meter,
	)
	requestsAfterFirstPair := recorder.count()

	invokeFallbackIsolationInfo(
		stdoutSource.OnInfo,
		common.SourceNameStdoutLogger,
		"delivery-stdout-second",
		tracer,
		meter,
	)
	invokeFallbackIsolationInfo(
		slackSource.OnInfo,
		common.SourceNameSlackLogger,
		"delivery-slack-second",
		tracer,
		meter,
	)

	require.Zero(t, requestsAfterFirstPair, "different Delivery sources must not fill one shared fallback batch")
	require.NoError(t, fallbackCloserOwner.Close())
	requireFallbackIsolationBatches(t, recorder.snapshot())

	firstFile, err := fileLoggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	secondFile, err := fileLoggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	require.Same(t, firstFile, secondFile)
	content, err := os.ReadFile(firstFile.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	for _, message := range []string{
		"delivery-stdout-first",
		"delivery-slack-first",
		"delivery-stdout-second",
		"delivery-slack-second",
	} {
		require.Contains(t, string(content), message)
	}
}

func TestNewLoggerCloseCompletesRegisteredClickHouseAndSlackFallbackHandlers(t *testing.T) {
	t.Parallel()
	lokiRecorder := &fallbackIsolationRequestRecorder{}
	clickHouseRecorder := &fallbackIsolationRequestRecorder{}
	slackRecorder := &fallbackSlackRequestRecorder{}

	handlerCfg := fallbackCloseErrorHandlerConfig(
		pkglogger.LogOutputClickHouse.String(),
		pkglogger.LogOutputSlack.String(),
	)
	lokiCfg := fallbackCloseLokiConfig(handlerCfg)
	clickHouseCfg := fallbackIsolationClickHouseConfig()
	slackCfg := fallbackIsolationSlackConfig(pkgloggerhandler.HandlerConfig{})
	// process全体の状態は変更せず、Slack設定のHTTPClientでSlack API通信だけを観測します。
	// fallback loggerは最初のhandler実行時に生成されますが、構築時に渡したこの設定を使います。
	slackCfg.HTTPClient = &http.Client{Transport: slackRecorder}
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputLoki,
		},
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		Loki:             &lokiCfg,
		LokiClient:       fallbackCloseLokiClient(lokiRecorder),
		ClickHouse:       &clickHouseCfg,
		ClickHouseClient: fallbackIsolationClickHouseClient(clickHouseRecorder),
		Slack:            &slackCfg,
	}

	topLevel, err := NewLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	require.NoError(t, topLevel.Info(context.Background(), "trigger sync fallback handlers"))
	require.Equal(t, 1, lokiRecorder.count())
	require.Zero(t, clickHouseRecorder.count(), "ClickHouse fallback must remain below MaxEntries")
	// Slack fallbackは最初のbatchを即時実行するため、scheduler次第ではCloseより先に
	// 送信を完了します。Close後に一度だけ完了し、重複送信しないことを検証します。

	closeable, ok := topLevel.(applogging.CloseableLogger)
	require.True(t, ok)
	require.NoError(t, closeable.Close())
	require.Equal(t, 1, clickHouseRecorder.count())
	require.Equal(t, 1, slackRecorder.count())
	require.Contains(t, clickHouseRecorder.snapshot()[0], "loki push delay exceeded")
	slackForm, err := url.ParseQuery(slackRecorder.snapshot()[0])
	require.NoError(t, err)
	require.Contains(t, slackForm.Get("text"), "loki push delay exceeded")
}

// 通常・非同期loggerのSlack出力は、Slack設定のHTTPClientでSlack APIへ送信します。
func TestTopLevelSlackOutputUsesConfiguredHTTPClient(t *testing.T) {
	t.Parallel()
	// slackHangTimeoutは、完了するはずの送信がhangしたことを検出するための上限です。
	const slackHangTimeout = 10 * time.Second
	ctx := context.Background()
	for _, test := range []struct {
		name string
		// logはloggerを構築して一件を受理させ、loggerのCloseを返します。
		log func(t *testing.T, slackCfg *pkglogger.SlackLoggerConfig, message string) func() error
	}{
		{
			name: "sync",
			log: func(t *testing.T, slackCfg *pkglogger.SlackLoggerConfig, message string) func() error {
				topLevel, err := NewLogger(ctx, pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack},
					Timeout:               time.Second,
					MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
					Slack:                 slackCfg,
				}, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{})
				require.NoError(t, err)
				require.NoError(t, topLevel.Info(ctx, message))
				closeable, ok := topLevel.(applogging.CloseableLogger)
				require.True(t, ok)
				return closeable.Close
			},
		},
		{
			name: "async",
			log: func(t *testing.T, slackCfg *pkglogger.SlackLoggerConfig, message string) func() error {
				topLevel, err := NewAsyncLogger(ctx, pkglogger.AsyncBestEffortMultiLoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack},
					Timeout:               time.Second,
					MaxParallel:           1,
					MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
					Slack:                 slackCfg,
				}, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{})
				require.NoError(t, err)
				topLevel.Info(ctx, message)
				closeable, ok := topLevel.(applogging.AsyncCloseableLogger)
				require.True(t, ok)
				return closeable.Close
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			recorder := &fallbackSlackRequestRecorder{recorded: make(chan struct{}, 1)}
			httpClient := &http.Client{Transport: recorder, Timeout: time.Hour}
			slackCfg := fallbackIsolationSlackConfig(pkgloggerhandler.HandlerConfig{})
			slackCfg.HTTPClient = httpClient
			message := "Slack output through configured HTTP client: " + test.name

			closeLogger := test.log(t, &slackCfg, message)
			// 初回通知は即時に送信します。設定のclientを使わない場合、Closeは失敗したbatchの
			// 再送を続けるため、Close前に送信を待ってhangではなく失敗として検出します。
			select {
			case <-recorder.recorded:
			case <-time.After(slackHangTimeout):
				t.Fatal("Slack output did not send through the configured HTTP client")
			}
			require.NoError(t, closeLogger())

			forms := fallbackSlackRequestForms(t, recorder)
			require.Len(t, forms, 1)
			require.Equal(t, slackCfg.Token, forms[0].Get("token"))
			require.Contains(t, forms[0].Get("text"), message)
			require.Same(t, recorder, httpClient.Transport)
			require.Equal(t, time.Hour, httpClient.Timeout, "the caller's client must not change")
		})
	}
}

// Delivery本体とDeliveryのinternal-log fallbackは、どちらもSlack設定のHTTPClientで
// Slack APIへ送信します。
func TestNewDeliveryLoggerSlackOutputAndFallbackUseConfiguredHTTPClient(t *testing.T) {
	t.Parallel()
	lokiRecorder := &fallbackIsolationRequestRecorder{}
	slackRecorder := &fallbackSlackRequestRecorder{}
	httpClient := &http.Client{Transport: slackRecorder, Timeout: time.Hour}
	// Lokiの送信は常に遅延閾値を超え、ERRORのinternal logをSlack fallbackへ通知します。
	lokiCfg := fallbackCloseDeliveryLokiConfig(
		fallbackCloseErrorHandlerConfig(pkglogger.LogOutputSlack.String()),
	)
	slackCfg := fallbackIsolationDeliverySlackConfig(pkgloggerhandler.HandlerConfig{})
	slackCfg.HTTPClient = httpClient
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputLoki,
			pkglogger.LogOutputSlack,
		},
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		Loki:             &lokiCfg,
		LokiClient:       fallbackCloseLokiClient(lokiRecorder),
		Slack:            &slackCfg,
		InternalLogBatchConfig: pkglogger.DeliveryInternalLogBatchConfig{
			Slack: pkglogger.DeliveryInternalLogSlackBatchConfig{
				Intervals:              []time.Duration{time.Hour},
				MaxLogsPerNotification: 10,
				MaxRestThreshold:       time.Hour,
			},
		},
	}

	topLevel, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	const message = "Delivery Slack output through configured HTTP client"
	report, err := topLevel.Info(context.Background(), message)
	require.NoError(t, err)
	require.NoError(t, report.Error())
	require.Equal(t, 1, lokiRecorder.count())

	closeable, ok := topLevel.(applogging.DeliveryClosableLogger)
	require.True(t, ok)
	require.NoError(t, closeable.Close())

	forms := fallbackSlackRequestForms(t, slackRecorder)
	require.Len(t, forms, 2)
	var outputs, fallbacks int
	for _, form := range forms {
		require.Equal(t, slackCfg.Token, form.Get("token"))
		switch text := form.Get("text"); {
		case strings.Contains(text, "loki push delay exceeded"):
			fallbacks++
		case strings.Contains(text, message):
			outputs++
		}
	}
	require.Equal(t, 1, outputs, "Delivery Slack output must use the configured HTTP client")
	require.Equal(t, 1, fallbacks, "Delivery Slack fallback must use the configured HTTP client")
	require.Same(t, slackRecorder, httpClient.Transport)
	require.Equal(t, time.Hour, httpClient.Timeout, "the caller's client must not change")
}

func TestNewAsyncLoggerCloseFlushesRegisteredClickHouseFallbackHandler(t *testing.T) {
	lokiRecorder := &fallbackIsolationRequestRecorder{}
	clickHouseRecorder := &fallbackIsolationRequestRecorder{}
	handlerCfg := fallbackCloseErrorHandlerConfig(pkglogger.LogOutputClickHouse.String())
	lokiCfg := fallbackCloseLokiConfig(handlerCfg)
	clickHouseCfg := fallbackIsolationClickHouseConfig()
	cfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputLoki,
		},
		Timeout:          time.Second,
		MaxParallel:      1,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		Loki:             &lokiCfg,
		LokiClient:       fallbackCloseLokiClient(lokiRecorder),
		ClickHouse:       &clickHouseCfg,
		ClickHouseClient: fallbackIsolationClickHouseClient(clickHouseRecorder),
	}

	topLevel, err := NewAsyncLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	topLevel.Info(context.Background(), "trigger async fallback handler")

	closeable, ok := topLevel.(applogging.AsyncCloseableLogger)
	require.True(t, ok)
	require.NoError(t, closeable.Close())
	require.Equal(t, 1, lokiRecorder.count())
	require.Equal(t, 1, clickHouseRecorder.count())
	require.Contains(t, clickHouseRecorder.snapshot()[0], "loki push delay exceeded")
}

func TestNewDeliveryLoggerCloseFlushesRegisteredClickHouseFallbackHandler(t *testing.T) {
	lokiRecorder := &fallbackIsolationRequestRecorder{}
	clickHouseRecorder := &fallbackIsolationRequestRecorder{}
	handlerCfg := fallbackCloseErrorHandlerConfig(pkglogger.LogOutputClickHouse.String())
	lokiCfg := fallbackCloseDeliveryLokiConfig(handlerCfg)
	clickHouseCfg := fallbackIsolationDeliveryClickHouseConfig()
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputLoki,
		},
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		Loki:             &lokiCfg,
		LokiClient:       fallbackCloseLokiClient(lokiRecorder),
		ClickHouse:       &clickHouseCfg,
		ClickHouseClient: fallbackIsolationClickHouseClient(clickHouseRecorder),
	}

	topLevel, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	report, err := topLevel.Info(context.Background(), "trigger delivery fallback handler")
	require.NoError(t, err)
	require.NoError(t, report.Error())
	require.Equal(t, 1, lokiRecorder.count())
	require.Zero(t, clickHouseRecorder.count(), "ClickHouse fallback must remain below MaxEntries")

	closeable, ok := topLevel.(applogging.DeliveryClosableLogger)
	require.True(t, ok)
	require.NoError(t, closeable.Close())
	require.Equal(t, 1, clickHouseRecorder.count())
	require.Contains(t, clickHouseRecorder.snapshot()[0], "loki push delay exceeded")
}

type fallbackIsolationRequestRecorder struct {
	mu     sync.Mutex
	bodies []string
}

const (
	fallbackLokiBufferFlushBytes            = 1 * 1024 * 1024
	fallbackLokiQueueMaxEntries             = 1000
	fallbackLokiMaxLineBytes                = 256 * 1024
	fallbackLokiMaxPushRequestBytes         = 4 * 1024 * 1024
	fallbackClickHouseBufferFlushBytes      = 1 * 1024 * 1024
	fallbackClickHouseMaxInsertRequestBytes = 16 * 1024 * 1024
)

type fallbackSlackRequestRecorder struct {
	fallbackIsolationRequestRecorder
	// recordedがnilでなければ、requestの記録を待つ試験へ通知します。送信を止めないよう、
	// 未受信の通知がある間は追加の通知を省きます。
	recorded chan struct{}
}

func (r *fallbackSlackRequestRecorder) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	r.record(string(body))
	if r.recorded != nil {
		select {
		case r.recorded <- struct{}{}:
		default:
		}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			`{"ok":true,"channel":"dummy-channel","ts":"1"}`,
		)),
		Request: request,
	}, nil
}

func fallbackSlackRequestForms(
	t *testing.T,
	recorder *fallbackSlackRequestRecorder,
) []url.Values {
	t.Helper()
	bodies := recorder.snapshot()
	forms := make([]url.Values, 0, len(bodies))
	for _, body := range bodies {
		form, err := url.ParseQuery(body)
		require.NoError(t, err)
		forms = append(forms, form)
	}
	return forms
}

func (r *fallbackIsolationRequestRecorder) record(body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bodies = append(r.bodies, body)
}

func (r *fallbackIsolationRequestRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *fallbackIsolationRequestRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.bodies...)
}

func (r *fallbackIsolationRequestRecorder) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	r.record(string(body))
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    request,
	}, nil
}

func fallbackIsolationHandlerConfig() pkgloggerhandler.HandlerConfig {
	return pkgloggerhandler.HandlerConfig{
		OnInfoConfig: []string{
			pkglogger.LogOutputFile.String(),
			pkglogger.LogOutputClickHouse.String(),
		},
	}
}

func fallbackCloseErrorHandlerConfig(names ...string) pkgloggerhandler.HandlerConfig {
	return pkgloggerhandler.HandlerConfig{
		OnErrorConfig: append([]string(nil), names...),
	}
}

func fallbackCloseLokiConfig(
	handlerCfg pkgloggerhandler.HandlerConfig,
) pkglogger.LokiConfig {
	return pkglogger.LokiConfig{
		LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{
			RuntimeConfig: pkglogger.RuntimeConfig{
				AppName:  "fallback-close-loki-source",
				Target:   "test",
				LogLevel: pkglogger.LogLevelDebug,
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: time.RFC3339Nano,
			},
			FlushInterval:           time.Hour,
			MaxEntries:              1,
			BufferFlushBytes:        fallbackLokiBufferFlushBytes,
			FallbackQueueMaxEntries: fallbackLokiQueueMaxEntries,
			MaxLineBytes:            fallbackLokiMaxLineBytes,
			MaxPushRequestBytes:     fallbackLokiMaxPushRequestBytes,
			NoBulkFlushModeEnabled:  true,
			HandlerConfig:           handlerCfg,
		},
		LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
			FailureStoreConfig: pkgdurable.FailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			},
			Resource:                   activationResource(),
			PostErrorThresholdDuration: time.Nanosecond,
			PushLatencyEnabled:         true,
		},
	}
}

func fallbackCloseDeliveryLokiConfig(
	handlerCfg pkgloggerhandler.HandlerConfig,
) pkglogger.DeliveryLokiConfig {
	return pkglogger.DeliveryLokiConfig{
		DeliveryLokiLoggerBaseConfig: pkglogger.DeliveryLokiLoggerBaseConfig{
			RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
				AppName: "fallback-close-delivery-loki-source",
				Target:  "test",
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: time.RFC3339Nano,
			},
			MaxLineBytes:        fallbackLokiMaxLineBytes,
			MaxPushRequestBytes: fallbackLokiMaxPushRequestBytes,
			HandlerConfig:       handlerCfg,
		},
		DeliveryLokiFlushManagerConfig: pkglogger.DeliveryLokiFlushManagerConfig{
			FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			},
			Resource:                   activationResource(),
			PostErrorThresholdDuration: time.Nanosecond,
			PushLatencyEnabled:         true,
		},
	}
}

func fallbackCloseLokiClient(
	recorder *fallbackIsolationRequestRecorder,
) *pkgloki.Config {
	return &pkgloki.Config{
		Endpoint:   "http://loki.test",
		Timeout:    time.Second,
		HTTPClient: &http.Client{Transport: measurableLatencyTransport{next: recorder}},
	}
}

// measurableLatencyTransportは、monotonic clockが進むまで応答を返さないtransportです。
// これらの試験はLokiのpush遅延の閾値を1nsにして、遅延通知を必ず発生させます。
// Windowsのmonotonic clockはtimer割込みの間隔（標準約15.6ms）でしか進まないため、
// 即時に応答するとpush遅延の計測値が0になり、閾値を超えません。
type measurableLatencyTransport struct {
	next http.RoundTripper
}

func (t measurableLatencyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	start := time.Now()
	response, err := t.next.RoundTrip(request)
	for !time.Now().After(start) {
		time.Sleep(time.Millisecond)
	}
	return response, err
}

func fallbackIsolationSlackConfig(
	handlerCfg pkgloggerhandler.HandlerConfig,
) pkglogger.SlackLoggerConfig {
	return pkglogger.SlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			TimeZone: "UTC",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "fallback-isolation-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Token:                  "dummy-token",
		Channel:                "dummy-channel",
		Timeout:                time.Second,
		MaxLogsPerNotification: 10,
		MaxRestThreshold:       time.Hour,
		Intervals:              []time.Duration{time.Hour},
		MaxMessageLength:       10_000,
		HandlerConfig:          handlerCfg,
	}
}

func fallbackIsolationDeliverySlackConfig(
	handlerCfg pkgloggerhandler.HandlerConfig,
) pkglogger.DeliverySlackLoggerConfig {
	return pkglogger.DeliverySlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			TimeZone: "UTC",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName: "fallback-isolation-delivery-test",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Token:            "dummy-token",
		Channel:          "dummy-channel",
		Timeout:          time.Second,
		MaxMessageLength: 10_000,
		HandlerConfig:    handlerCfg,
	}
}

func fallbackIsolationClickHouseConfig() pkglogger.ClickHouseConfig {
	return pkglogger.ClickHouseConfig{
		ClickHouseLoggerBaseConfig: pkglogger.ClickHouseLoggerBaseConfig{
			RuntimeConfig: pkglogger.RuntimeConfig{
				AppName:  "fallback-isolation-test",
				LogLevel: pkglogger.LogLevelDebug,
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: time.RFC3339Nano,
			},
			FlushInterval:           time.Hour,
			MaxEntries:              2,
			BufferFlushBytes:        fallbackClickHouseBufferFlushBytes,
			FallbackQueueMaxEntries: 2,
			MaxInsertRequestBytes:   fallbackClickHouseMaxInsertRequestBytes,
		},
		ClickHouseFlushManagerConfig: pkglogger.ClickHouseFlushManagerConfig{
			FailureStoreConfig: pkgdurable.FailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			},
			Resource: activationResource(),
		},
	}
}

func fallbackIsolationDeliveryClickHouseConfig() pkglogger.DeliveryClickHouseConfig {
	return pkglogger.DeliveryClickHouseConfig{
		DeliveryClickHouseLoggerBaseConfig: pkglogger.DeliveryClickHouseLoggerBaseConfig{
			RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
				AppName: "fallback-isolation-delivery-test",
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: time.RFC3339Nano,
			},
			FlushInterval:           time.Hour,
			MaxEntries:              2,
			BufferFlushBytes:        fallbackClickHouseBufferFlushBytes,
			FallbackQueueMaxEntries: 2,
			MaxInsertRequestBytes:   fallbackClickHouseMaxInsertRequestBytes,
		},
		DeliveryClickHouseFlushManagerConfig: pkglogger.DeliveryClickHouseFlushManagerConfig{
			FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			},
			Resource: activationResource(),
		},
	}
}

func fallbackIsolationClickHouseClient(
	recorder *fallbackIsolationRequestRecorder,
) *pkgclickhouse.Config {
	return &pkgclickhouse.Config{
		Endpoint:   "http://clickhouse.test",
		Table:      "logs",
		Timeout:    time.Second,
		HTTPClient: &http.Client{Transport: recorder},
	}
}

func fallbackIsolationSyncSources(
	t *testing.T,
	children []logging.BackendLogEntryHandler,
) (*stdout.StdoutLogger, *slack.SlackLogger) {
	t.Helper()
	var stdoutSource *stdout.StdoutLogger
	var slackSource *slack.SlackLogger
	for _, child := range children {
		switch candidate := child.(type) {
		case *stdout.StdoutLogger:
			stdoutSource = candidate
		case *slack.SlackLogger:
			slackSource = candidate
		}
	}
	require.NotNil(t, stdoutSource)
	require.NotNil(t, slackSource)
	return stdoutSource, slackSource
}

func fallbackIsolationDeliverySources(
	t *testing.T,
	children []multi.DeliveryTarget,
) (*stdout.DeliveryStdoutLogger, *slack.DeliverySlackLogger) {
	t.Helper()
	var stdoutSource *stdout.DeliveryStdoutLogger
	var slackSource *slack.DeliverySlackLogger
	for _, child := range children {
		switch candidate := child.Logger.(type) {
		case *stdout.DeliveryStdoutLogger:
			require.Equal(t, pkglogger.LogOutputStdout.String(), child.Backend)
			stdoutSource = candidate
		case *slack.DeliverySlackLogger:
			require.Equal(t, pkglogger.LogOutputSlack.String(), child.Backend)
			slackSource = candidate
		}
	}
	require.NotNil(t, stdoutSource)
	require.NotNil(t, slackSource)
	return stdoutSource, slackSource
}

func invokeFallbackIsolationInfo(
	handler logging.LoggerHandlerFunc,
	source string,
	message string,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	handler(
		context.Background(),
		&logging.LogEntry{
			Level:   pkglogger.LogLevelInfo,
			Message: message,
			Source:  source,
		},
		tracer,
		meter,
	)
}

func requireFallbackIsolationBatches(t *testing.T, bodies []string) {
	t.Helper()
	require.Len(t, bodies, 2)
	for _, source := range []string{"stdout", "slack"} {
		var matchingBody string
		for _, body := range bodies {
			if strings.Contains(body, source+"-first") ||
				strings.Contains(body, "delivery-"+source+"-first") {
				matchingBody = body
				break
			}
		}
		require.NotEmpty(t, matchingBody, "missing %s fallback batch", source)
		require.Contains(t, matchingBody, source+"-first")
		require.Contains(t, matchingBody, source+"-second")
		otherSource := "stdout"
		if source == "stdout" {
			otherSource = "slack"
		}
		require.NotContains(t, matchingBody, otherSource+"-first")
		require.NotContains(t, matchingBody, otherSource+"-second")
	}
}

func requireFallbackIsolationSharedFile(
	t *testing.T,
	fileLoggerSet *filelogger.LoggerSet,
	messages []string,
) {
	t.Helper()
	firstFile, err := fileLoggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	secondFile, err := fileLoggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	require.Same(t, firstFile, secondFile)
	content, err := os.ReadFile(firstFile.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	for _, message := range messages {
		require.Contains(t, string(content), message)
	}
}
