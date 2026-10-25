// free_and_total_unsupported.go
//go:build !windows && !unix

package disk

import "errors"

func FreeAndTotal(path string) (free uint64, total uint64, err error) {
	_ = path
	return 0, 0, errors.New("disk stats are not supported on this platform")
}
