package jsonutil

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestUTF8StringBytesMatchesJSONReceiver(t *testing.T) {
	inputs := []string{
		"", "ASCII", "日本語😀", "\"\\\n\t<>&\u2028", "\ufffd",
		"\xff\xfe", "a\xffb", "\xc3", "\xe3\x81", "\xed\xa0\x80",
		"\xf0\x9f\x92", "\xc0\xaf", "\ufffd\xff日本\xc3",
	}
	// 取り得るすべての1byte値で、ASCIIと不正な先頭byte・継続byteを網羅します。
	// 上の複数byteの例は、正しいU+FFFDと不正なbyteを区別します。
	for b := 0; b < 256; b++ {
		inputs = append(inputs, string([]byte{byte(b)}))
	}
	for i, input := range inputs {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			encoded, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			var decoded string
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if got := UTF8StringBytes(input); got != len(decoded) {
				t.Fatalf("UTF8StringBytes(%q) = %d, JSON receiver gets %d bytes (%q)", input, got, len(decoded), decoded)
			}
		})
	}
}
