package bootstrap

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	"github.com/go-jappe/heterolog/internal/presentation/envconfig"
)

// NewFromEnvは、envのConfigが返す設定とoptionから同期loggerを作ります。optsはその
// optionの後に適用するため、置き換えたり、WithObserversやWithMemoryPressureMonitorを
// 加えたりできます。
func NewFromEnv(ctx context.Context, env *envconfig.EnvConfig, opts ...loggerruntime.Option) (*loggerruntime.Runtime, error) {
	cfg, envOpts, err := env.Config()
	if err != nil {
		return nil, err
	}
	return New(ctx, cfg, append(envOpts, opts...)...)
}

// NewAsyncFromEnvは、envのAsyncConfigが返す設定とoptionに続けてoptsを適用し、非同期
// loggerを作ります。
func NewAsyncFromEnv(ctx context.Context, env *envconfig.EnvConfig, opts ...loggerruntime.Option) (*loggerruntime.AsyncRuntime, error) {
	cfg, envOpts, err := env.AsyncConfig()
	if err != nil {
		return nil, err
	}
	return NewAsync(ctx, cfg, append(envOpts, opts...)...)
}

// NewDeliveryFromEnvは、envのDeliveryConfigが返す設定とoptionに続けてoptsを適用し、
// 送信結果を返すloggerを作ります。
func NewDeliveryFromEnv(ctx context.Context, env *envconfig.EnvConfig, opts ...loggerruntime.Option) (*loggerruntime.DeliveryRuntime, error) {
	cfg, envOpts, err := env.DeliveryConfig()
	if err != nil {
		return nil, err
	}
	return NewDelivery(ctx, cfg, append(envOpts, opts...)...)
}

// NewAdminAuditFromEnvは、envのAdminAuditConfigが返す設定とoptionに続けてoptsを適用し、
// 管理用APIの監査logを記録する送達確認loggerを作ります。
func NewAdminAuditFromEnv(ctx context.Context, env *envconfig.EnvConfig, opts ...loggerruntime.Option) (*loggerruntime.DeliveryRuntime, error) {
	cfg, envOpts, err := env.AdminAuditConfig()
	if err != nil {
		return nil, err
	}
	return NewDelivery(ctx, cfg, append(envOpts, opts...)...)
}

// NewTelemetryFromEnvは、envのTelemetryConfigが返す設定とoptsから、telemetryを作ります。
func NewTelemetryFromEnv(ctx context.Context, env *envconfig.EnvConfig, opts ...loggerruntime.Option) (*telemetryruntime.Runtime, error) {
	return NewTelemetry(ctx, env.TelemetryConfig(), opts...)
}
