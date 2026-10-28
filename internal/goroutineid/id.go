// Package goroutineidは、callbackが同じgoroutineから直接呼び直されたことを見分けるために、
// 今のgoroutineのidを読みます。
package goroutineid

import (
	"bytes"
	"runtime"
	"strconv"
)

// Currentは、runtime.Stackの先頭行から、今のgoroutineのidを読みます。Goにはgoroutineの
// idを読む公開のAPIがないため、callbackの直接の呼び直しを見分ける用途だけに使う内部の
// 補助です。先頭行を読めない場合は、無関係のgoroutineを呼び出し元と取り違えないよう、
// falseを返します。
func Current() (uint64, bool) {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	return Parse(buf[:n])
}

// Parseは、runtime.Stackの先頭行「goroutine N [...]」からNを読みます。
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
