//go:build !unix && !windows

package memorypressure

import (
	"context"
	"errors"
)

func (defaultProvider) ReadMemoryPressure(ctx context.Context) (MemoryPressure, error) {
	_ = ctx
	return MemoryPressure{}, errors.New("memory pressure is not supported on this OS")
}
