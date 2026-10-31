//go:build !unix && !windows

package status

import (
	"errors"
	"time"
)

// CPUTimesは、このOSではCPU時間を読めません。
func (processProbe) CPUTimes() (time.Duration, time.Duration, error) {
	return 0, 0, errors.ErrUnsupported
}
