package provider

import (
	"errors"
	"io"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
)

var ErrNilSpillDestinationConfig = errors.New("spill destination config is nil")

// NewDegradeHandler builds a destination writer shared by direct degradation
// and overload evacuation paths.
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
