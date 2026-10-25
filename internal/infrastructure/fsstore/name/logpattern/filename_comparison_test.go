package logpattern

import (
	"runtime"
	"testing"
)

func TestFileNameComparisonKeyUsesHostRules(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		input      string
		windowsKey string
	}{
		{name: "empty", input: "", windowsKey: ""},
		{name: "lowercase", input: "app_20260914_1.log.gz", windowsKey: "app_20260914_1.log.gz"},
		{name: "mixed case", input: "Error_App_20260914_1.LOG.GZ", windowsKey: "error_app_20260914_1.log.gz"},
		{name: "extension", input: ".GZ", windowsKey: ".gz"},
		{name: "custom extension", input: ".BR", windowsKey: ".br"},
		{name: "unicode", input: "Kernel_ログ.LOG", windowsKey: "kernel_ログ.log"},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := test.input
			if runtime.GOOS == "windows" {
				want = test.windowsKey
			}
			if got := FileNameComparisonKey(test.input); got != want {
				t.Fatalf("FileNameComparisonKey(%q) = %q, want %q", test.input, got, want)
			}
		})
	}
}
