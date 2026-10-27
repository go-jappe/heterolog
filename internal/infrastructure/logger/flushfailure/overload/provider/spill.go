package provider

import (
	"errors"
	"io"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
)

var ErrNilSpillDestinationConfig = errors.New("spill destination config is nil")

// NewDegradeHandlerは、直接のdegradationとoverload時の退避の経路とで共有する、
// 退避先のwriterを構築します。
func NewDegradeHandler(
	cfg overload.SpillDestinationConfigProvider,
) (overload.Handler, error) {
	return NewDegradeHandlerWithStdout(cfg, nil)
}

// NewDegradeHandlerWithStdoutは、stdoutへの退避にos.Stdoutの代わりにwriterを使います。
// writerがnilならos.Stdoutへ出力します。
func NewDegradeHandlerWithStdout(
	cfg overload.SpillDestinationConfigProvider,
	stdout io.Writer,
) (overload.Handler, error) {
	if cfg == nil {
		return nil, ErrNilSpillDestinationConfig
	}

	return evacuation.NewHandlerWithStdout(cfg.SpillDestination(), stdout)
}
