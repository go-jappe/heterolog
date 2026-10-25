package objectstoragespool

import "time"

// PurgeConfigAdapterは保持設定を値として保持し、補完や検証をせずに返します。
type PurgeConfigAdapter struct {
	config TerminalPurgeConfig
}

var _ PurgeConfigProvider = PurgeConfigAdapter{}

func NewPurgeConfigAdapter(config TerminalPurgeConfig) PurgeConfigAdapter {
	return PurgeConfigAdapter{config: config}
}

func (a PurgeConfigAdapter) DonePurge() StatePurgeConfigProvider {
	return NewStatePurgeConfigAdapter(a.config.Done)
}

func (a PurgeConfigAdapter) DeadPurge() StatePurgeConfigProvider {
	return NewStatePurgeConfigAdapter(a.config.Dead)
}

func (a PurgeConfigAdapter) QuarantinePurge() StatePurgeConfigProvider {
	return NewStatePurgeConfigAdapter(a.config.Quarantine)
}

func (a PurgeConfigAdapter) CapacityPurgeMinimumFileAge() time.Duration {
	return a.config.CapacityPurgeMinimumFileAge
}
