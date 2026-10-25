package pathidentity

import (
	"strings"
	"testing"
)

func TestAddWindowsLongPathPrefix(t *testing.T) {
	t.Parallel()

	drive := `C:\MiXeD\`
	drive247 := drive + strings.Repeat("a", 247-len(drive))
	drive248 := drive + strings.Repeat("a", 248-len(drive))
	unc := `\\Server\Share\MiXeD\`
	unc247 := unc + strings.Repeat("a", 247-len(unc))
	unc248 := unc + strings.Repeat("a", 248-len(unc))
	unicodePath := drive + strings.Repeat("界", 80)

	for _, test := range []struct {
		name string
		path string
		want string
	}{
		{name: "short drive", path: drive, want: drive},
		{name: "drive below boundary", path: drive247, want: drive247},
		{name: "drive at boundary", path: drive248, want: `\\?\` + drive248},
		{name: "short UNC", path: unc, want: unc},
		{name: "UNC below boundary", path: unc247, want: unc247},
		{name: "UNC at boundary", path: unc248, want: `\\?\UNC\` + unc248[2:]},
		{name: "Unicode byte boundary", path: unicodePath, want: `\\?\` + unicodePath},
		{name: "existing drive prefix", path: `\\?\` + drive248, want: `\\?\` + drive248},
		{name: "existing UNC prefix", path: `\\?\UNC\` + unc248[2:], want: `\\?\UNC\` + unc248[2:]},
		{name: "device prefix", path: `\\.\` + drive248, want: `\\.\` + drive248},
		{name: "native prefix", path: `\??\` + drive248, want: `\??\` + drive248},
		{name: "volume GUID prefix", path: `\\?\Volume{ABC}\` + drive248, want: `\\?\Volume{ABC}\` + drive248},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := AddWindowsLongPathPrefix(test.path)
			if got != test.want {
				t.Fatalf("AddWindowsLongPathPrefix(%q) = %q, want %q", test.path, got, test.want)
			}
			if again := AddWindowsLongPathPrefix(got); again != got {
				t.Fatalf("prefix was added twice: %q -> %q", got, again)
			}
		})
	}
}
