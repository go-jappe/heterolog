package sampling

import (
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// TestApplySamplingLogicUsesOneSnapshotWhileSettingsChangeは、稼働中に設定を変えても、
// 一回の判定を一つの設定の値だけで行うことを検証します。samplingを無効にした設定は割合の
// 検証を省くため0を持てます。有効な設定と別々に読み合わせると、0の割合で判定して
// panicします。
func TestApplySamplingLogicUsesOneSnapshotWhileSettingsChange(t *testing.T) {
	enabled := logger.SamplingConfig{IsSamplingEnabled: true, SamplingDebugRate: 5, SamplingInfoRate: 5, SamplingWarnRate: 5, SamplingErrorRate: 5}
	live, err := logger.NewLiveSettings(logger.LogLevelDebug, enabled)
	if err != nil {
		t.Fatal(err)
	}
	provider := NewConfigProvider(logger.SamplingConfig{}, live)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			next := enabled
			if i%2 == 0 {
				next = logger.SamplingConfig{}
			}
			if err := live.Set(logger.LogLevelDebug, next); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 20000; i++ {
		ApplySamplingLogic(&logging.LogEntry{Level: logger.LogLevelInfo}, provider)
	}
	close(stop)
	wg.Wait()
}
