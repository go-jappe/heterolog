// Package goroutineid identifies direct callback reentry on the current goroutine.
package goroutineid

import (
	"bytes"
	"runtime"
	"strconv"
)

// Current reads the current goroutine ID from runtime.Stack's header. Go does
// not expose a public goroutine ID API; this internal helper is used only to
// recognize direct callback reentry. It returns false if the runtime's header
// cannot be read, rather than confusing an unrelated goroutine with the caller.
func Current() (uint64, bool) {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	return Parse(buf[:n])
}

// Parse reads the ID in the runtime.Stack header "goroutine N [...]".
func Parse(stack []byte) (uint64, bool) {
	rest, found := bytes.CutPrefix(stack, []byte("goroutine "))
	if !found {
		return 0, false
	}
	end := bytes.IndexByte(rest, ' ')
	if end <= 0 || !bytes.HasPrefix(rest[end:], []byte(" [")) {
		return 0, false
	}
	id, err := strconv.ParseUint(string(rest[:end]), 10, 64)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}
