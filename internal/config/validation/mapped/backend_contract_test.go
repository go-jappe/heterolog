package mappedvalidation

import (
	"errors"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

func TestMappedLoggerConfigsRejectUnknownFileLifecycleMode(t *testing.T) {
	unknown := pkgfsstore.FileLifecycleMode("unknown")

	tests := []struct {
		name       string
		wantSource string
		wantKind   error
		validate   func() error
	}{
		{
			name:       "sync",
			wantSource: "logger file",
			wantKind:   pkgfsstore.ErrUnsupportedFileLifecycleMode,
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					File: &pkglogger.FileLoggerConfig{LifecycleMode: unknown},
				})
			},
		},
		{
			name:       "async",
			wantSource: "async logger file",
			wantKind:   pkgfsstore.ErrUnsupportedFileLifecycleMode,
			validate: func() error {
				return ValidateSyncLoggerConfig("async logger", pkglogger.LoggerConfig{
					File: &pkglogger.FileLoggerConfig{LifecycleMode: unknown},
				})
			},
		},
		{
			name:       "delivery",
			wantSource: "delivery logger file",
			wantKind:   pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode,
			validate: func() error {
				return ValidateDeliveryLoggerConfig(
					"delivery logger",
					pkglogger.DeliveryLoggerConfig{
						File: &pkglogger.DeliveryFileLoggerConfig{LifecycleMode: unknown},
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireConfigError(t, tt.validate(), tt.wantSource, ConfigSectionLifecycle, tt.wantKind)
		})
	}
}

func TestMappedOpenSearchConfigRejectsBlankIndex(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputOpenSearch,
		},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{Index: " "},
		},
		OpenSearchClient: validMappedOpenSearchClient(),
	}

	err := ValidateSyncLoggerConfig("logger", cfg)
	fieldErr := requireConfigFieldError(
		t,
		err,
		"logger",
		pkglogger.LogOutputOpenSearch,
		pkglogger.ConfigFieldIndex,
		pkglogger.ErrInvalidSetting,
	)
	if !errors.Is(fieldErr, pkgopensearch.ErrIndexRequired) {
		t.Fatalf("mapped OpenSearch index error = %v, want %v", fieldErr, pkgopensearch.ErrIndexRequired)
	}
}

func TestMappedSlackConfigRejectsBlankDestination(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack},
		Slack: &pkglogger.SlackLoggerConfig{
			Token:   "test-token",
			Channel: " ",
		},
	}

	err := ValidateSyncLoggerConfig("logger", cfg)
	fieldErr := requireConfigFieldError(
		t,
		err,
		"logger",
		pkglogger.LogOutputSlack,
		pkglogger.ConfigFieldClient,
		pkglogger.ErrInvalidClientConfig,
	)
	if !errors.Is(fieldErr, pkgslack.ErrChannelRequired) {
		t.Fatalf("mapped Slack validation error = %v, want %v", fieldErr, pkgslack.ErrChannelRequired)
	}
}

func TestMappedInternalLogOpenSearchValidatesOverrideAndIgnoresUnusedBaseDestination(t *testing.T) {
	client := validMappedOpenSearchClient()
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout: &pkglogger.StdoutLoggerConfig{
			HandlerConfig: pkgloggerhandler.HandlerConfig{
				OnErrorConfig: []string{"opensearch"},
			},
		},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				Index: "base-logs",
			},
		},
		OpenSearchClient: client,
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
			OpenSearch: &pkglogger.OpenSearchConfig{
				OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
					Index: " ",
				},
			},
			OpenSearchClient: client,
		},
	}

	err := ValidateSyncLoggerConfig("logger", cfg)
	fieldErr := requireConfigFieldError(
		t,
		err,
		"logger internal log destination",
		pkglogger.LogOutputOpenSearch,
		pkglogger.ConfigFieldIndex,
		pkglogger.ErrInvalidSetting,
	)
	if !errors.Is(fieldErr, pkgopensearch.ErrIndexRequired) {
		t.Fatalf("internal-log OpenSearch index error = %v, want %v", fieldErr, pkgopensearch.ErrIndexRequired)
	}

	cfg.InternalLogDestinations.OpenSearch.Index = "internal-logs"
	cfg.OpenSearch.Index = " "
	err = ValidateSyncLoggerConfig("logger", cfg)
	if errors.Is(err, pkgopensearch.ErrIndexRequired) {
		t.Fatalf("unused base OpenSearch config was validated: %v", err)
	}
}

func TestMappedInternalLogSlackValidatesOverrideAndIgnoresUnusedBaseDestination(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout: &pkglogger.StdoutLoggerConfig{
			HandlerConfig: pkgloggerhandler.HandlerConfig{
				OnErrorConfig: []string{"slack"},
			},
		},
		Slack: &pkglogger.SlackLoggerConfig{
			Token:   "test-token",
			Channel: "base-channel",
		},
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
			Slack: &pkglogger.SlackLoggerConfig{
				Token:   "test-token",
				Channel: " ",
			},
		},
	}

	err := ValidateSyncLoggerConfig("logger", cfg)
	fieldErr := requireConfigFieldError(
		t,
		err,
		"logger internal log destination",
		pkglogger.LogOutputSlack,
		pkglogger.ConfigFieldClient,
		pkglogger.ErrInvalidClientConfig,
	)
	if !errors.Is(fieldErr, pkgslack.ErrChannelRequired) {
		t.Fatalf("internal-log Slack validation error = %v, want %v", fieldErr, pkgslack.ErrChannelRequired)
	}

	cfg.InternalLogDestinations.Slack.Channel = "internal-logs"
	cfg.Slack.Channel = " "
	err = ValidateSyncLoggerConfig("logger", cfg)
	if errors.Is(err, pkgslack.ErrChannelRequired) {
		t.Fatalf("unused base Slack config was validated: %v", err)
	}
}

func TestMappedConfigIgnoresUnusedHandlerTargetRetrySettings(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout:                &pkglogger.StdoutLoggerConfig{},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchRetryableClientCaseTransportErrorConfig: &pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: pkglogger.RetryClientMode("unsupported"),
				MaxRetries:      -1,
				BaseDelay:       -time.Second,
				MaxDelay:        -time.Second,
			},
		},
	}

	if err := ValidateSyncLoggerConfig("logger", cfg); err != nil {
		t.Fatalf("unused handler target validation error = %v, want nil", err)
	}
}

func validMappedOpenSearchClient() *pkgopensearch.Config {
	return &pkgopensearch.Config{
		Endpoint: "http://localhost:9200",
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	}
}
