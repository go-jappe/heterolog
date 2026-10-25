// free_and_total_unix.go
//go:build unix

package disk

import "golang.org/x/sys/unix"

func FreeAndTotal(path string) (free uint64, total uint64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}

	bsize := uint64(st.Bsize)
	total = uint64(st.Blocks) * bsize
	free = uint64(st.Bavail) * bsize

	return free, total, nil
}
