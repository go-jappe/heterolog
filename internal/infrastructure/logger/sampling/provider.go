package sampling

import "github.com/go-jappe/heterolog/pkg/logger"

type SamplingConfigProvider interface {
	IsSamplingEnabled() bool
	EnableLoggingByRequestID() bool
	LoggingByRequestIDRate() int
	SamplingDebugRate() int
	SamplingInfoRate() int
	SamplingWarnRate() int
	SamplingErrorRate() int
}

type samplingConfigAdapter struct {
	IsSamplingEnabledConfig        bool
	EnableLoggingByRequestIDConfig bool
	LoggingByRequestIDRateConfig   int
	SamplingDebugRateConfig        int
	SamplingInfoRateConfig         int
	SamplingWarnRateConfig         int
	SamplingErrorRateConfig        int
}

// NewConfigProviderは、liveがnilならcfgの値を、そうでなければliveの現在の値を返す
// providerを返します。liveはbackend設定のRuntimeConfig.Liveです。
func NewConfigProvider(cfg logger.SamplingConfig, live *logger.LiveSettings) SamplingConfigProvider {
	if live != nil {
		return liveSamplingConfig{live: live}
	}
	return NewSamplingConfigAdapter(cfg)
}

func NewSamplingConfigAdapter(cfg logger.SamplingConfig) SamplingConfigProvider {
	return samplingConfigAdapter{
		IsSamplingEnabledConfig:        cfg.IsSamplingEnabled,
		EnableLoggingByRequestIDConfig: cfg.EnableLoggingByRequestID,
		LoggingByRequestIDRateConfig:   cfg.LoggingByRequestIDRate,
		SamplingDebugRateConfig:        cfg.SamplingDebugRate,
		SamplingInfoRateConfig:         cfg.SamplingInfoRate,
		SamplingWarnRateConfig:         cfg.SamplingWarnRate,
		SamplingErrorRateConfig:        cfg.SamplingErrorRate,
	}
}

func (a samplingConfigAdapter) SamplingConfig() logger.SamplingConfig {
	return logger.SamplingConfig{
		IsSamplingEnabled:        a.IsSamplingEnabledConfig,
		EnableLoggingByRequestID: a.EnableLoggingByRequestIDConfig,
		LoggingByRequestIDRate:   a.LoggingByRequestIDRateConfig,
		SamplingDebugRate:        a.SamplingDebugRateConfig,
		SamplingInfoRate:         a.SamplingInfoRateConfig,
		SamplingWarnRate:         a.SamplingWarnRateConfig,
		SamplingErrorRate:        a.SamplingErrorRateConfig,
	}
}
func (a samplingConfigAdapter) IsSamplingEnabled() bool {
	return a.IsSamplingEnabledConfig
}
func (a samplingConfigAdapter) EnableLoggingByRequestID() bool {
	return a.EnableLoggingByRequestIDConfig
}
func (a samplingConfigAdapter) LoggingByRequestIDRate() int {
	return a.LoggingByRequestIDRateConfig
}
func (a samplingConfigAdapter) SamplingDebugRate() int {
	return a.SamplingDebugRateConfig
}
func (a samplingConfigAdapter) SamplingInfoRate() int {
	return a.SamplingInfoRateConfig
}
func (a samplingConfigAdapter) SamplingWarnRate() int {
	return a.SamplingWarnRateConfig
}
func (a samplingConfigAdapter) SamplingErrorRate() int {
	return a.SamplingErrorRateConfig
}

// liveSamplingConfigは、LiveSettingsの現在のsampling設定を返すproviderです。
type liveSamplingConfig struct {
	live *logger.LiveSettings
}

// samplingSnapshotは、一回の判定で使う設定を一度に読み取れるproviderです。method
// ごとに読むと、判定の途中で設定が変わった場合に別々の設定の値を組み合わせるためです。
type samplingSnapshot interface {
	SamplingConfig() logger.SamplingConfig
}

func (p liveSamplingConfig) SamplingConfig() logger.SamplingConfig {
	return p.live.Sampling()
}
func (p liveSamplingConfig) IsSamplingEnabled() bool {
	return p.live.Sampling().IsSamplingEnabled
}
func (p liveSamplingConfig) EnableLoggingByRequestID() bool {
	return p.live.Sampling().EnableLoggingByRequestID
}
func (p liveSamplingConfig) LoggingByRequestIDRate() int {
	return p.live.Sampling().LoggingByRequestIDRate
}
func (p liveSamplingConfig) SamplingDebugRate() int {
	return p.live.Sampling().SamplingDebugRate
}
func (p liveSamplingConfig) SamplingInfoRate() int {
	return p.live.Sampling().SamplingInfoRate
}
func (p liveSamplingConfig) SamplingWarnRate() int {
	return p.live.Sampling().SamplingWarnRate
}
func (p liveSamplingConfig) SamplingErrorRate() int {
	return p.live.Sampling().SamplingErrorRate
}
