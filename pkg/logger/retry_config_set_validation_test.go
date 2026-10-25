package logger

import (
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestRetryValidationIgnoresTerminalInternalLogFileLifecycleRouting(t *testing.T) {
	invalidRetry := &OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: RetryClientMode("future-mode"),
	}

	tests := []struct {
		name     string
		validate func() error
	}{
		{
			name: "sync and async config",
			validate: func() error {
				return ValidateLoggerRetryClientConfigs("logger", LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
					Stdout:                &StdoutLoggerConfig{},
					OpenSearch: &OpenSearchConfig{
						OpenSearchRetryableClientCaseTransportErrorConfig: invalidRetry,
					},
					InternalLogDestinations: &InternalLogDestinationConfig{
						File: &FileLoggerConfig{
							LifecycleMode: fsstore.FileLifecycleModeRunner,
							LifecycleHandlerConfig: loggerhandler.HandlerConfig{
								OnErrorConfig: []string{LogOutputOpenSearch.String()},
							},
						},
					},
				})
			},
		},
		{
			name: "delivery config",
			validate: func() error {
				return ValidateDeliveryLoggerRetryClientConfigs(
					"delivery logger",
					DeliveryLoggerConfig{
						LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
						Stdout:                &DeliveryStdoutLoggerConfig{},
						OpenSearch: &DeliveryOpenSearchConfig{
							OpenSearchRetryableClientCaseTransportErrorConfig: invalidRetry,
						},
						InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
							File: &DeliveryFileLoggerConfig{
								LifecycleMode: fsstore.FileLifecycleModeRunner,
								LifecycleRunnerSettings: DeliveryFileLifecycleRunnerSettings{
									LifecycleHandlerConfig: loggerhandler.HandlerConfig{
										OnErrorConfig: []string{
											LogOutputOpenSearch.String(),
										},
									},
								},
							},
						},
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.validate(); err != nil {
				t.Fatalf(
					"retry validation error = %v, terminal internal file routing must be ignored",
					err,
				)
			}
		})
	}
}

func TestRetryValidationValidatesInternalLogDestinationOverride(t *testing.T) {
	tests := []struct {
		name     string
		validate func() error
		// wantSourceは、不正なretry設定を持つinternal-log保存先の出所です。
		wantSource string
	}{
		{
			name:       "sync and async config",
			wantSource: "logger internal log destination",
			validate: func() error {
				return ValidateLoggerRetryClientConfigs("logger", LoggerConfig{
					InternalLogDestinations: &InternalLogDestinationConfig{
						OpenSearch: &OpenSearchConfig{
							OpenSearchRetryableClientCaseTransportErrorConfig: &OpenSearchRetryableClientCaseTransportErrorConfig{
								RetryClientMode: RetryClientMode("future-mode"),
							},
						},
					},
				})
			},
		},
		{
			name:       "delivery config",
			wantSource: "delivery logger internal log destination",
			validate: func() error {
				return ValidateDeliveryLoggerRetryClientConfigs(
					"delivery logger",
					DeliveryLoggerConfig{
						InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
							OpenSearch: &DeliveryOpenSearchConfig{
								OpenSearchRetryableClientCaseTransportErrorConfig: &OpenSearchRetryableClientCaseTransportErrorConfig{
									RetryClientMode: RetryClientMode("future-mode"),
								},
							},
						},
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate()
			fieldErr := findFieldError(err, ConfigFieldRetryClientMode, ErrUnsupportedRetryClientMode)
			if fieldErr == nil || fieldErr.Source != tt.wantSource || fieldErr.Backend != LogOutputOpenSearch ||
				fieldErr.Value != RetryClientMode("future-mode") {
				t.Fatalf("retry validation error = %v, want %s opensearch unsupported mode %q",
					err, tt.wantSource, "future-mode")
			}
		})
	}
}

func TestRetryValidationIgnoresBaseBackendReplacedByInternalLogOverride(t *testing.T) {
	invalidBaseRetry := &OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: RetryClientMode("future-mode"),
	}

	tests := []struct {
		name     string
		validate func() error
	}{
		{
			name: "sync and async config",
			validate: func() error {
				return ValidateLoggerRetryClientConfigs("logger", LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
					Stdout: &StdoutLoggerConfig{
						HandlerConfig: loggerhandler.HandlerConfig{
							OnErrorConfig: []string{LogOutputOpenSearch.String()},
						},
					},
					OpenSearch: &OpenSearchConfig{
						OpenSearchRetryableClientCaseTransportErrorConfig: invalidBaseRetry,
					},
					InternalLogDestinations: &InternalLogDestinationConfig{
						OpenSearch: &OpenSearchConfig{},
					},
				})
			},
		},
		{
			name: "delivery config",
			validate: func() error {
				return ValidateDeliveryLoggerRetryClientConfigs(
					"delivery logger",
					DeliveryLoggerConfig{
						LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
						Stdout: &DeliveryStdoutLoggerConfig{
							HandlerConfig: loggerhandler.HandlerConfig{
								OnErrorConfig: []string{LogOutputOpenSearch.String()},
							},
						},
						OpenSearch: &DeliveryOpenSearchConfig{
							OpenSearchRetryableClientCaseTransportErrorConfig: invalidBaseRetry,
						},
						InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
							OpenSearch: &DeliveryOpenSearchConfig{},
						},
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.validate(); err != nil {
				t.Fatalf("unused base retry config was validated: %v", err)
			}
		})
	}
}
