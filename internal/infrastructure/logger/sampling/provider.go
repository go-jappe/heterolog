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
