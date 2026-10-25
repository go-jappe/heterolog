package jsonutil

import "unicode/utf8"

// UTF8StringBytes returns the byte length of s as decoded by a JSON receiver.
// Like encoding/json, it counts each invalid UTF-8 byte as a three-byte U+FFFD
// replacement rune. JSON quotes and escaping are excluded. The input is not
// modified, and no normalized copy is allocated.
func UTF8StringBytes(s string) int {
	if utf8.ValidString(s) {
		return len(s)
	}
	n := 0
	for _, r := range s {
		n += utf8.RuneLen(r)
	}
	return n
}
