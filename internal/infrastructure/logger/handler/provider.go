package handler

import "github.com/go-jappe/heterolog/pkg/logger/handler"

type ConfigProvider interface {
	OnError() []string
	OnWarn() []string
	OnInfo() []string
	OnDebug() []string
}

type configAdapter struct {
	OnErrorConfig []string
	OnWarnConfig  []string
	OnInfoConfig  []string
	OnDebugConfig []string
}

func NewConfigAdapter(cfg handler.HandlerConfig) configAdapter {
	return configAdapter{
		OnErrorConfig: cfg.OnErrorConfig,
		OnWarnConfig:  cfg.OnWarnConfig,
		OnInfoConfig:  cfg.OnInfoConfig,
		OnDebugConfig: cfg.OnDebugConfig,
	}
}

func (a configAdapter) OnError() []string {
	return a.OnErrorConfig
}
func (a configAdapter) OnWarn() []string {
	return a.OnWarnConfig
}
func (a configAdapter) OnInfo() []string {
	return a.OnInfoConfig
}
func (a configAdapter) OnDebug() []string {
	return a.OnDebugConfig
}
