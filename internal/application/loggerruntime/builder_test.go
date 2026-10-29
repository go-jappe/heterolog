package loggerruntime_test

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// recordingBuilderは、試験用のloggerruntime.Builderです。組み立てに渡された設定とoptionを
// 記録し、閉じることだけができるloggerを返します。返したloggerでlogを記録してはいけません。
type recordingBuilder struct {
	built    int
	logger   pkglogger.LoggerConfig
	async    pkglogger.AsyncBestEffortMultiLoggerConfig
	delivery pkglogger.DeliveryLoggerConfig
	options  loggerruntime.Options
}

func (b *recordingBuilder) NewLogger(_ context.Context, cfg pkglogger.LoggerConfig, o loggerruntime.Options) (logging.CloseableLogger, error) {
	b.built++
	b.logger, b.options = cfg, o
	return closeOnlyLogger{}, nil
}

func (b *recordingBuilder) NewAsyncLogger(_ context.Context, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, o loggerruntime.Options) (logging.AsyncCloseableLogger, error) {
	b.built++
	b.async, b.options = cfg, o
	return closeOnlyAsyncLogger{}, nil
}

func (b *recordingBuilder) NewDeliveryLogger(_ context.Context, cfg pkglogger.DeliveryLoggerConfig, o loggerruntime.Options) (logging.DeliveryClosableLogger, error) {
	b.built++
	b.delivery, b.options = cfg, o
	return closeOnlyDeliveryLogger{}, nil
}

type closeOnlyLogger struct{ pkglogger.Logger }

func (closeOnlyLogger) Close() error { return nil }

type closeOnlyAsyncLogger struct{ pkglogger.AsyncLogger }

func (closeOnlyAsyncLogger) Close() error { return nil }

type closeOnlyDeliveryLogger struct{ pkglogger.DeliveryLogger }

func (closeOnlyDeliveryLogger) Close() error { return nil }

// constructorは、解決したoptionと、稼働中の設定を持たせた設定をBuilderへ渡します。Builderが
// 組み立てるloggerは、管理用APIが変えるものと同じLiveSettingsを使います。
func TestConstructorsPassResolvedOptionsAndLiveSettingsToBuilder(t *testing.T) {
	ctx := context.Background()
	spool := loggerruntime.DefaultSpoolConfig()

	builder := &recordingBuilder{}
	runtime, err := loggerruntime.New(ctx, builder, loggerruntime.DefaultConfig(), loggerruntime.WithSpool(spool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	requireResolvedOptions(t, builder.options)
	if builder.built != 1 || builder.options.Spool == nil {
		t.Errorf("New built %d loggers with spool %v, want one with the spool", builder.built, builder.options.Spool)
	}
	if builder.logger.Resource == nil {
		t.Error("New passed no resource to the builder")
	}
	if live := builder.logger.Stdout.RuntimeConfig.Live; live == nil || live != runtime.OutputLiveSettings()[pkglogger.LogOutputStdout] {
		t.Errorf("New passed stdout LiveSettings %p, want the one in OutputLiveSettings", live)
	}

	builder = &recordingBuilder{}
	async, err := loggerruntime.NewAsync(ctx, builder, loggerruntime.DefaultAsyncConfig(), loggerruntime.WithSpool(spool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = async.Close() })
	requireResolvedOptions(t, builder.options)
	if builder.built != 1 || builder.options.Spool == nil {
		t.Errorf("NewAsync built %d loggers with spool %v, want one with the spool", builder.built, builder.options.Spool)
	}
	if live := builder.async.Stdout.RuntimeConfig.Live; live == nil || live != async.OutputLiveSettings()[pkglogger.LogOutputStdout] {
		t.Errorf("NewAsync passed stdout LiveSettings %p, want the one in OutputLiveSettings", live)
	}

	builder = &recordingBuilder{}
	delivery, err := loggerruntime.NewDelivery(ctx, builder, loggerruntime.DefaultDeliveryConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = delivery.Close() })
	requireResolvedOptions(t, builder.options)
	if builder.built != 1 || builder.delivery.Resource == nil {
		t.Errorf("NewDelivery built %d loggers with resource %v, want one with a resource", builder.built, builder.delivery.Resource)
	}
}

// requireResolvedOptionsは、oがResolveOptionsで解決した既定の値を持つことを確かめます。
func requireResolvedOptions(t *testing.T, o loggerruntime.Options) {
	t.Helper()
	if o.Tracer == nil || o.Meter == nil {
		t.Errorf("builder received tracer %v and meter %v, want no-op defaults", o.Tracer, o.Meter)
	}
	if o.Routing != pkgcloudwatchlogs.StreamRoutingNone {
		t.Errorf("builder received stream routing %q, want %q", o.Routing, pkgcloudwatchlogs.StreamRoutingNone)
	}
}
