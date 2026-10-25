package fsstore

import (
	"errors"
	"fmt"
)

var (
	// ErrUnsupportedFileLifecycleModeは、ValidateFileLifecycleModeが受け付けないlifecycle modeです。
	ErrUnsupportedFileLifecycleMode = errors.New("unsupported file lifecycle mode")
	// ErrUnsupportedDeliveryFileLifecycleModeは、ValidateDeliveryFileLifecycleModeが受け付けない
	// lifecycle modeです。
	ErrUnsupportedDeliveryFileLifecycleMode = errors.New("unsupported delivery file lifecycle mode")
)

// ValidateFileLifecycleModeは、通常FileLoggerとfailure archiveが使用するlifecycle modeを
// 検証します。
//
// 空値は、最小限の設定で通常FileLoggerを利用できるようinline modeとして扱います。
// Delivery FileLoggerの空値はlifecycle無効であり、同じ空値でも利用側のLogger契約に
// よって既定動作が異なります。環境変数のalias解決やdefault適用はconfig loaderの責務です。
func ValidateFileLifecycleMode(mode FileLifecycleMode) error {
	switch mode {
	case "", FileLifecycleModeDisable, FileLifecycleModeInline, FileLifecycleModeRunner:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFileLifecycleMode, mode)
	}
}

// ValidateDeliveryFileLifecycleModeは、Delivery FileLoggerが実装するlifecycle modeを
// 検証します。Delivery writerはinline lifecycleを持たないため、runnerまたはdisable
// だけを受け付けます。空値はlifecycleを起動しない安全な既定動作として許容します。
// 通常FileLoggerでは空値がinlineになるため、両Loggerの空値は同じ意味ではありません。
func ValidateDeliveryFileLifecycleMode(mode FileLifecycleMode) error {
	switch mode {
	case "", FileLifecycleModeDisable, FileLifecycleModeRunner:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedDeliveryFileLifecycleMode, mode)
	}
}
