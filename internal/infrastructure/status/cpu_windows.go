//go:build windows

package status

import (
	"time"

	"golang.org/x/sys/windows"
)

// CPUTimesは、GetProcessTimesで、このprocessがuserとkernelで使ったCPU時間を返します。
func (processProbe) CPUTimes() (time.Duration, time.Duration, error) {
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &creation, &exit, &kernel, &user); err != nil {
		return 0, 0, err
	}
	return time.Duration(user.Nanoseconds()), time.Duration(kernel.Nanoseconds()), nil
}
