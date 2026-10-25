package logger

import (
	"context"
	"errors"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

type directRetryConfigValues struct {
	mode        pkglogger.RetryClientMode
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

const (
	directRetryLokiBufferFlushBytes                 = 1 * 1024 * 1024
	directRetryLokiFallbackQueueMaxEntries          = 1
	directRetryLokiMaxLineBytes                     = 256 * 1024
	directRetryLokiMaxPushRequestBytes              = 4 * 1024 * 1024
	directRetryCloudWatchLogsBufferFlushBytes       = 1 * 1024 * 1024
	directRetryCloudWatchLogsFallbackQueueMaxEvents = 1
	directRetryClickHouseBufferFlushBytes           = 1 * 1024 * 1024
	directRetryClickHouseMaxInsertRequestBytes      = 16 * 1024 * 1024
)

func TestDirectLoggerConstructorsRejectInvalidRetryConfigForActiveBackend(t *testing.T) {
	tests := []struct {
		name      string
		values    directRetryConfigValues
		wantErr   error
		wantField pkglogger.ConfigField
		wantValue any
	}{
		{
			name: "unknown mode",
			values: directRetryConfigValues{
				mode:        pkglogger.RetryClientMode("future-mode"),
				maxAttempts: 1,
				baseDelay:   time.Millisecond,
				maxDelay:    time.Millisecond,
			},
			wantErr:   pkglogger.ErrUnsupportedRetryClientMode,
			wantField: pkglogger.ConfigFieldRetryClientMode,
			wantValue: pkglogger.RetryClientMode("future-mode"),
		},
		{
			name: "invalid attempts",
			values: directRetryConfigValues{
				maxAttempts: -1,
				mode:        pkglogger.RetryClientModeRetry,
				baseDelay:   time.Millisecond,
				maxDelay:    time.Millisecond,
			},
			wantErr:   pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldRetryMaxAttempts,
			wantValue: -1,
		},
		{
			name: "invalid base delay",
			values: directRetryConfigValues{
				mode:        pkglogger.RetryClientModeRetry,
				maxAttempts: 1,
				baseDelay:   -time.Millisecond,
				maxDelay:    time.Millisecond,
			},
			wantErr:   pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldRetryBaseDelay,
			wantValue: -time.Millisecond,
		},
		{
			name: "invalid max delay",
			values: directRetryConfigValues{
				mode:        pkglogger.RetryClientModeRetry,
				maxAttempts: 1,
				baseDelay:   time.Millisecond,
				maxDelay:    -time.Millisecond,
			},
			wantErr:   pkglogger.ErrNonPositiveSetting,
			wantField: pkglogger.ConfigFieldRetryMaxDelay,
			wantValue: -time.Millisecond,
		},
	}

	for _, mode := range directRetryConstructorModes() {
		for _, backend := range directRetryBackends() {
			for _, tt := range tests {
				t.Run(mode+"/"+backend.String()+"/"+tt.name, func(t *testing.T) {
					closeLogger, err := constructLoggerWithRetryConfig(
						t,
						mode,
						backend,
						tt.values,
						backend.String(),
						nil,
					)
					if closeLogger != nil {
						t.Cleanup(closeLogger)
					}
					requireRetryConfigFieldError(t, err, retryConfigFieldErrorWant{
						source:  directRetryContractSource(t, mode),
						backend: backend,
						field:   tt.wantField,
						value:   tt.wantValue,
						kind:    tt.wantErr,
					})
				})
			}
		}
	}
}

func TestDirectLoggerConstructorsRejectInvalidRetryConfigForNormalBackend(t *testing.T) {
	invalid := directRetryConfigValues{
		mode: pkglogger.RetryClientMode("future-mode"),
	}
	for _, mode := range directRetryConstructorModes() {
		for _, backend := range directRetryBackends() {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				err := constructNormalLoggerWithRetryConfig(t, mode, backend, invalid)
				requireRetryConfigFieldError(t, err, retryConfigFieldErrorWant{
					source:  directRetryContractSource(t, mode),
					backend: backend,
					field:   pkglogger.ConfigFieldRetryClientMode,
					value:   invalid.mode,
					kind:    pkglogger.ErrUnsupportedRetryClientMode,
				})
			})
		}
	}
}

func TestDirectLoggerConstructorsAcceptValidRetryConfigForActiveBackend(t *testing.T) {
	valid := validDirectRetryConfig()
	for _, mode := range directRetryConstructorModes() {
		for _, backend := range directRetryBackends() {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				closeLogger, err := constructLoggerWithRetryConfig(
					t,
					mode,
					backend,
					valid,
					backend.String(),
					nil,
				)
				if err != nil {
					t.Fatalf("direct constructor error = %v, want nil", err)
				}
				if closeLogger == nil {
					t.Fatal("direct constructor close function = nil")
				}
				t.Cleanup(closeLogger)
			})
		}
	}
}

func constructNormalLoggerWithRetryConfig(
	t *testing.T,
	mode string,
	backend pkglogger.LogOutputDestination,
	values directRetryConfigValues,
) error {
	t.Helper()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	switch mode {
	case "sync":
		cfg, _, _ := newHandlerOnlyLoggerConfig(t, backend)
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{backend}
		setLoggerRetryConfig(&cfg, backend, values)
		_, err := NewLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		return err
	case "async":
		base, _, _ := newHandlerOnlyLoggerConfig(t, backend)
		base.LogOutputDestinations = pkglogger.LogOutputDestinations{backend}
		setLoggerRetryConfig(&base, backend, values)
		_, err := NewAsyncLogger(
			context.Background(),
			directRetryAsyncConfig(base),
			streamrouting.None,
			nil,
			nil,
			tracer,
			meter,
		)

		return err
	case "delivery":
		cfg, _, _ := newHandlerOnlyDeliveryConfig(t, backend)
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{backend}
		setDeliveryRetryConfig(&cfg, backend, values)
		_, err := NewDeliveryLogger(context.Background(), cfg, streamrouting.None, tracer, meter)
		return err
	default:
		t.Fatalf("unsupported constructor mode %q", mode)
		return nil
	}
}

func TestDirectLoggerConstructorsIgnoreUnselectedRetryConfig(t *testing.T) {
	invalid := directRetryConfigValues{mode: pkglogger.RetryClientMode("future-mode")}
	for _, mode := range directRetryConstructorModes() {
		for _, backend := range directRetryBackends() {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				closeLogger, err := constructLoggerWithRetryConfig(
					t,
					mode,
					backend,
					invalid,
					"",
					nil,
				)
				if err != nil {
					t.Fatalf("direct constructor error = %v, want unselected config ignored", err)
				}
				if closeLogger == nil {
					t.Fatal("direct constructor close function = nil")
				}
				t.Cleanup(closeLogger)
			})
		}
	}
}

func TestDirectLoggerConstructorsAcceptValidInternalLogDestinationRetryConfig(t *testing.T) {
	baseValid := validDirectRetryConfig()
	internalLogValid := validDirectRetryConfig()
	for _, mode := range directRetryConstructorModes() {
		for _, backend := range directRetryBackends() {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				closeLogger, err := constructLoggerWithRetryConfig(
					t,
					mode,
					backend,
					baseValid,
					backend.String(),
					&internalLogValid,
				)
				if err != nil {
					t.Fatalf(
						"direct constructor error = %v, want valid internal log destination retry config",
						err,
					)
				}
				if closeLogger == nil {
					t.Fatal("direct constructor close function = nil")
				}
				t.Cleanup(closeLogger)
			})
		}
	}
}

func TestDirectLoggerConstructorsRejectInvalidInternalLogDestinationRetryConfig(t *testing.T) {
	baseValid := validDirectRetryConfig()
	internalLogInvalid := directRetryConfigValues{
		mode: pkglogger.RetryClientMode("internal-log-invalid"),
	}
	for _, mode := range directRetryConstructorModes() {
		for _, backend := range directRetryBackends() {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				closeLogger, err := constructLoggerWithRetryConfig(
					t,
					mode,
					backend,
					baseValid,
					backend.String(),
					&internalLogInvalid,
				)
				if closeLogger != nil {
					t.Cleanup(closeLogger)
				}
				requireRetryConfigFieldError(t, err, retryConfigFieldErrorWant{
					source:  directRetryContractSource(t, mode) + " internal log destination",
					backend: backend,
					field:   pkglogger.ConfigFieldRetryClientMode,
					value:   internalLogInvalid.mode,
					kind:    pkglogger.ErrUnsupportedRetryClientMode,
				})
			})
		}
	}
}

// retryConfigFieldErrorWantは、retry設定の検証で期待するpkg/logger.ConfigFieldErrorです。
type retryConfigFieldErrorWant struct {
	source  string
	backend pkglogger.LogOutputDestination
	field   pkglogger.ConfigField
	value   any
	kind    error
}

// requireRetryConfigFieldErrorは、retry設定の検証errorが期待した種類・出所・backend・
// 設定項目・値のpkg/logger.ConfigFieldErrorを含むことを確認します。
func requireRetryConfigFieldError(t *testing.T, err error, want retryConfigFieldErrorWant) {
	t.Helper()
	if !errors.Is(err, want.kind) {
		t.Fatalf("direct constructor error = %v, want %v", err, want.kind)
	}
	fieldErrs := pkglogger.ConfigFieldErrors(err)
	if len(fieldErrs) != 1 {
		t.Fatalf("direct constructor error = %v, want one config field error", err)
	}
	got := retryConfigFieldErrorWant{
		source:  fieldErrs[0].Source,
		backend: fieldErrs[0].Backend,
		field:   fieldErrs[0].Field,
		value:   fieldErrs[0].Value,
		kind:    fieldErrs[0].Err,
	}
	if got != want {
		t.Fatalf("retry config field error = %+v, want %+v", got, want)
	}
}

// directRetryContractSourceは、constructor modeの検証errorが設定の出所として表示する
// Logger契約名を返します。
func directRetryContractSource(t *testing.T, mode string) string {
	t.Helper()
	switch mode {
	case "sync":
		return pkglogger.LoggerContractSync.DisplayName()
	case "async":
		return pkglogger.LoggerContractAsyncBestEffort.DisplayName()
	case "delivery":
		return pkglogger.LoggerContractDelivery.DisplayName()
	default:
		t.Fatalf("unsupported constructor mode %q", mode)
		return ""
	}
}

func constructLoggerWithRetryConfig(
	t *testing.T,
	mode string,
	backend pkglogger.LogOutputDestination,
	baseRetry directRetryConfigValues,
	selector string,
	internalLogRetry *directRetryConfigValues,
) (func(), error) {
	t.Helper()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	switch mode {
	case "sync":
		cfg, _, _ := newHandlerOnlyLoggerConfig(t, backend)
		cfg.Stdout.HandlerConfig = directRetrySourceRouting(selector)
		setLoggerRetryConfig(&cfg, backend, baseRetry)
		if internalLogRetry != nil {
			cfg.InternalLogDestinations = newRetryInternalLogDestination(
				cfg,
				backend,
				*internalLogRetry,
			)
		}

		created, err := NewLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		if err != nil {
			return nil, err
		}
		closeable := created.(applogging.CloseableLogger)
		return func() {
			if err := closeable.Close(); err != nil {
				t.Errorf("close sync logger: %v", err)
			}
		}, nil

	case "async":
		base, _, _ := newHandlerOnlyLoggerConfig(t, backend)
		base.Stdout.HandlerConfig = directRetrySourceRouting(selector)
		setLoggerRetryConfig(&base, backend, baseRetry)
		if internalLogRetry != nil {
			base.InternalLogDestinations = newRetryInternalLogDestination(
				base,
				backend,
				*internalLogRetry,
			)
		}
		cfg := directRetryAsyncConfig(base)

		created, err := NewAsyncLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		if err != nil {
			return nil, err
		}
		closeable := created.(applogging.AsyncCloseableLogger)
		return func() {
			if err := closeable.Close(); err != nil {
				t.Errorf("close async logger: %v", err)
			}
		}, nil

	case "delivery":
		cfg, _, _ := newHandlerOnlyDeliveryConfig(t, backend)
		cfg.Stdout.HandlerConfig = directRetrySourceRouting(selector)
		setDeliveryRetryConfig(&cfg, backend, baseRetry)
		if internalLogRetry != nil {
			cfg.InternalLogDestinations = newDeliveryRetryInternalLogDestination(
				cfg,
				backend,
				*internalLogRetry,
			)
		}

		created, err := NewDeliveryLogger(context.Background(), cfg, streamrouting.None, tracer, meter)
		if err != nil {
			return nil, err
		}
		closeable := created.(applogging.DeliveryClosableLogger)
		return func() {
			if err := closeable.Close(); err != nil {
				t.Errorf("close delivery logger: %v", err)
			}
		}, nil

	default:
		t.Fatalf("unsupported constructor mode %q", mode)
		return nil, nil
	}
}

func directRetryConstructorModes() []string {
	return []string{"sync", "async", "delivery"}
}

func directRetryBackends() []pkglogger.LogOutputDestination {
	return []pkglogger.LogOutputDestination{
		pkglogger.LogOutputOpenSearch,
		pkglogger.LogOutputLoki,
		pkglogger.LogOutputCloudWatchLogs,
		pkglogger.LogOutputClickHouse,
	}
}

func directRetrySourceRouting(selector string) pkgloggerhandler.HandlerConfig {
	if selector == "" {
		return pkgloggerhandler.HandlerConfig{}
	}
	return pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{selector}}
}

func validDirectRetryConfig() directRetryConfigValues {
	return directRetryConfigValues{
		mode:        pkglogger.RetryClientModeRetryPolicy,
		maxAttempts: 1,
		baseDelay:   time.Millisecond,
		maxDelay:    time.Millisecond,
	}
}

func directRetryAsyncConfig(cfg pkglogger.LoggerConfig) pkglogger.AsyncBestEffortMultiLoggerConfig {
	return pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations:   cfg.LogOutputDestinations,
		Timeout:                 time.Second,
		MaxParallel:             1,
		MaxLogEntryBytes:        cfg.MaxLogEntryBytes,
		File:                    cfg.File,
		Stdout:                  cfg.Stdout,
		Slack:                   cfg.Slack,
		OpenSearch:              cfg.OpenSearch,
		OpenSearchClient:        cfg.OpenSearchClient,
		Loki:                    cfg.Loki,
		LokiClient:              cfg.LokiClient,
		CloudWatchLogs:          cfg.CloudWatchLogs,
		CloudWatchLogsClient:    cfg.CloudWatchLogsClient,
		ClickHouse:              cfg.ClickHouse,
		ClickHouseClient:        cfg.ClickHouseClient,
		InternalLogDestinations: cfg.InternalLogDestinations,
		Resource:                cfg.Resource,
	}
}

func setLoggerRetryConfig(
	cfg *pkglogger.LoggerConfig,
	backend pkglogger.LogOutputDestination,
	values directRetryConfigValues,
) {
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig =
			&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	case pkglogger.LogOutputLoki:
		cfg.Loki.BufferFlushBytes = directRetryLokiBufferFlushBytes
		cfg.Loki.FallbackQueueMaxEntries = directRetryLokiFallbackQueueMaxEntries
		cfg.Loki.MaxLineBytes = directRetryLokiMaxLineBytes
		cfg.Loki.MaxPushRequestBytes = directRetryLokiMaxPushRequestBytes
		cfg.Loki.LokiRetryableClientCaseTransportErrorConfig =
			&pkglogger.LokiRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	case pkglogger.LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs.BufferFlushMaxEvents = 1
		cfg.CloudWatchLogs.BufferFlushBytes = directRetryCloudWatchLogsBufferFlushBytes
		cfg.CloudWatchLogs.FallbackQueueMaxEvents = directRetryCloudWatchLogsFallbackQueueMaxEvents
		cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig =
			&pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouse.BufferFlushBytes = directRetryClickHouseBufferFlushBytes
		cfg.ClickHouse.MaxInsertRequestBytes = directRetryClickHouseMaxInsertRequestBytes
		cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig =
			&pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	}
}

func setDeliveryRetryConfig(
	cfg *pkglogger.DeliveryLoggerConfig,
	backend pkglogger.LogOutputDestination,
	values directRetryConfigValues,
) {
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig =
			&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	case pkglogger.LogOutputLoki:
		cfg.Loki.MaxLineBytes = directRetryLokiMaxLineBytes
		cfg.Loki.MaxPushRequestBytes = directRetryLokiMaxPushRequestBytes
		cfg.Loki.LokiRetryableClientCaseTransportErrorConfig =
			&pkglogger.LokiRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	case pkglogger.LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig =
			&pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouse.BufferFlushBytes = directRetryClickHouseBufferFlushBytes
		cfg.ClickHouse.MaxInsertRequestBytes = directRetryClickHouseMaxInsertRequestBytes
		cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig =
			&pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{
				RetryClientMode: values.mode,
				MaxAttempts:     values.maxAttempts,
				BaseDelay:       values.baseDelay,
				MaxDelay:        values.maxDelay,
			}
	}
}

func newRetryInternalLogDestination(
	base pkglogger.LoggerConfig,
	backend pkglogger.LogOutputDestination,
	values directRetryConfigValues,
) *pkglogger.InternalLogDestinationConfig {
	destination := &pkglogger.InternalLogDestinationConfig{}
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		cfg := *base.OpenSearch
		destination.OpenSearch = &cfg
		destination.OpenSearchClient = base.OpenSearchClient
	case pkglogger.LogOutputLoki:
		cfg := *base.Loki
		destination.Loki = &cfg
		destination.LokiClient = base.LokiClient
	case pkglogger.LogOutputCloudWatchLogs:
		cfg := *base.CloudWatchLogs
		destination.CloudWatchLogs = &cfg
		destination.CloudWatchLogsClient = base.CloudWatchLogsClient
	case pkglogger.LogOutputClickHouse:
		cfg := *base.ClickHouse
		destination.ClickHouse = &cfg
		destination.ClickHouseClient = base.ClickHouseClient
	}

	destinationCfg := destination.ToLoggerConfig()
	setLoggerRetryConfig(&destinationCfg, backend, values)
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		destination.OpenSearch = destinationCfg.OpenSearch
	case pkglogger.LogOutputLoki:
		destination.Loki = destinationCfg.Loki
	case pkglogger.LogOutputCloudWatchLogs:
		destination.CloudWatchLogs = destinationCfg.CloudWatchLogs
	case pkglogger.LogOutputClickHouse:
		destination.ClickHouse = destinationCfg.ClickHouse
	}
	return destination
}

func newDeliveryRetryInternalLogDestination(
	base pkglogger.DeliveryLoggerConfig,
	backend pkglogger.LogOutputDestination,
	values directRetryConfigValues,
) *pkglogger.DeliveryInternalLogDestinationConfig {
	destination := &pkglogger.DeliveryInternalLogDestinationConfig{}
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		cfg := *base.OpenSearch
		destination.OpenSearch = &cfg
		destination.OpenSearchClient = base.OpenSearchClient
	case pkglogger.LogOutputLoki:
		cfg := *base.Loki
		destination.Loki = &cfg
		destination.LokiClient = base.LokiClient
	case pkglogger.LogOutputCloudWatchLogs:
		cfg := *base.CloudWatchLogs
		destination.CloudWatchLogs = &cfg
		destination.CloudWatchLogsClient = base.CloudWatchLogsClient
	case pkglogger.LogOutputClickHouse:
		cfg := *base.ClickHouse
		destination.ClickHouse = &cfg
		destination.ClickHouseClient = base.ClickHouseClient
	}

	destinationCfg := destination.ToLoggerConfig()
	setDeliveryRetryConfig(&destinationCfg, backend, values)
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		destination.OpenSearch = destinationCfg.OpenSearch
	case pkglogger.LogOutputLoki:
		destination.Loki = destinationCfg.Loki
	case pkglogger.LogOutputCloudWatchLogs:
		destination.CloudWatchLogs = destinationCfg.CloudWatchLogs
	case pkglogger.LogOutputClickHouse:
		destination.ClickHouse = destinationCfg.ClickHouse
	}
	return destination
}
