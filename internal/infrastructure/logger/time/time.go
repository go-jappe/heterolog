package time

import "github.com/go-jappe/heterolog/pkg/logger"

type TimeConfigProvider interface {
	EnableLocalTime() bool
	TimeZone() string
}

type timeAdapter struct {
	EnableLocalTimeConfig bool
	TimeZoneConfig        string
}

func NewTimeConfigAdapter(cfg logger.TimeConfig) TimeConfigProvider {
	return timeAdapter{
		EnableLocalTimeConfig: cfg.IsLocalTimeEnabled,
		TimeZoneConfig:        cfg.TimeZone,
	}
}

func (a timeAdapter) EnableLocalTime() bool {
	return a.EnableLocalTimeConfig
}

func (a timeAdapter) TimeZone() string {
	return a.TimeZoneConfig
}
