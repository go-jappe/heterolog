package victoriametrics

import "fmt"

// normalizePushManagerConfigは自分用のcopyを持つため、既定値を適用しても呼び出し元の
// 設定は変わりません。retry回数やretry delayが0の場合は、意図的にretryや待機を無効に
// します。容量やtimeoutと違い、これらの値に既定値は必要ありません。
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
