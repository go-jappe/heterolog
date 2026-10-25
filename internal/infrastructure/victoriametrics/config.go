package victoriametrics

import "fmt"

// normalizePushManagerConfig owns its copy so applying defaults does not change
// the caller's settings. A zero retry count/delay deliberately disables retries
// or waiting; unlike capacities and timeouts, those values need no default.
func normalizePushManagerConfig(config *PushManagerConfig) (*PushManagerConfig, error) {
	defaults := DefaultConfig()
	if config == nil {
		config = defaults
	}
	result := *config
	if result.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	for _, field := range []struct {
		name     string
		negative bool
	}{
		{"push interval", result.PushInterval < 0},
		{"batch size", result.BatchSize < 0},
		{"worker count", result.WorkerCount < 0},
		{"timeout", result.Timeout < 0},
		{"max retries", result.MaxRetries < 0},
		{"retry delay", result.RetryDelay < 0},
	} {
		if field.negative {
			return nil, fmt.Errorf("%s must not be negative", field.name)
		}
	}
	if result.PushInterval == 0 {
		result.PushInterval = defaults.PushInterval
	}
	if result.BatchSize == 0 {
		result.BatchSize = defaults.BatchSize
	}
	if result.WorkerCount == 0 {
		result.WorkerCount = defaults.WorkerCount
	}
	if result.Timeout == 0 {
		result.Timeout = defaults.Timeout
	}
	if result.OnError == nil {
		result.OnError = defaults.OnError
	}
	return &result, nil
}
