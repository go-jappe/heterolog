// free_and_total_windows.go
//go:build windows

package disk

import "golang.org/x/sys/windows"

func FreeAndTotal(path string) (free uint64, total uint64, err error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}

	var freeBytesAvailable uint64
	var totalBytes uint64
	var totalFreeBytes uint64

	if err := windows.GetDiskFreeSpaceEx(
		p,
		&freeBytesAvailable,
		&totalBytes,
		&totalFreeBytes,
	); err != nil {
		return 0, 0, err
	}

	return freeBytesAvailable, totalBytes, nil
}
