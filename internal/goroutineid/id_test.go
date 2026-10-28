package goroutineid

import (
	"fmt"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

// Parseは、先頭行が「goroutine N [」で始まり、Nが1以上のuint64である場合だけidを返します。
func TestParseReadsTheStackHeader(t *testing.T) {
	for _, test := range []struct {
		stack string
		want  uint64
		ok    bool
	}{
		{stack: "goroutine 1 [running]:\nmain.main()", want: 1, ok: true},
		{stack: "goroutine 18446744073709551615 [running]:", want: 18446744073709551615, ok: true},
		{stack: "goroutine 42 [chan receive, 3 minutes]:", want: 42, ok: true},
		{stack: "goroutine 0 [running]:"},
		{stack: "goroutine 18446744073709551616 [running]:"},
		{stack: "goroutine -1 [running]:"},
		{stack: "goroutine 12[running]:"},
		{stack: "goroutine 12 running:"},
		{stack: "goroutine  [running]:"},
		{stack: "Goroutine 12 [running]:"},
		{stack: "goroutine 12"},
		{stack: ""},
	} {
		t.Run(fmt.Sprintf("%q", test.stack), func(t *testing.T) {
			got, ok := Parse([]byte(test.stack))
			if got != test.want || ok != test.ok {
				t.Fatalf("Parse(%q) = %d, %t; want %d, %t", test.stack, got, ok, test.want, test.ok)
			}
		})
	}
}

// Currentは、同じgoroutineでは同じidを、別のgoroutineでは違うidを返します。
func TestCurrentIdentifiesTheCallingGoroutine(t *testing.T) {
	first, ok := Current()
	if !ok {
		t.Fatal("Current could not read the runtime stack header")
	}
	if again, _ := Current(); again != first {
		t.Fatalf("the same goroutine returned ids %d and %d", first, again)
	}
	ids := make(chan uint64, 1)
	go func() {
		id, _ := Current()
		ids <- id
	}()
	if other := boundedwait.Receive(t, ids, "the goroutine ID read on another goroutine"); other == 0 || other == first {
		t.Fatalf("another goroutine returned id %d, the test goroutine %d", other, first)
	}
}
