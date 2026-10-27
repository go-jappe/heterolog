package objectstorage

import (
	"slices"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// CompressedLogFileUploadSpoolServices returns the destination services in
// configuration order when this route selects spool evacuation for compressed
// logs. Duplicate services are merged into one entry.
func (cfg UploadTargetConfig) CompressedLogFileUploadSpoolServices() []UploadTarget {
	if !cfg.Enabled || !slices.Contains(cfg.CompressedLogFileUploadFailureStore.DurableType, failurestore.DurableTypeSpool) {
		return nil
	}
	var services []UploadTarget
	for _, service := range cfg.Targets {
		if !slices.Contains(services, service) {
			services = append(services, service)
		}
	}
	return services
}

// CompressedLogFileUploadSpoolServices returns, in configuration order, the
// upload backends for which the roles in use select spool evacuation. A
// backend used by several roles is also merged into one entry.
func (cfg LogFileUploadConfig) CompressedLogFileUploadSpoolServices(separateError, separateDebug bool) []UploadTarget {
	if !cfg.Enabled {
		return nil
	}
	targets := []UploadTargetConfig{cfg.Default}
	if separateError {
		targets = append(targets, cfg.Error)
	}
	if separateDebug {
		targets = append(targets, cfg.Debug)
	}
	var services []UploadTarget
	for _, target := range targets {
		for _, service := range target.CompressedLogFileUploadSpoolServices() {
			if !slices.Contains(services, service) {
				services = append(services, service)
			}
		}
	}
	return services
}
