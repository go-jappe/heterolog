//go:build linux

package memorypressure

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"testing"
	"testing/fstest"
)

func linuxMemoryFixture(group, mountRoot, mountPoint string) fstest.MapFS {
	return fstest.MapFS{
		"proc/self/cgroup":    {Data: []byte("0::" + group + "\n")},
		"proc/self/mountinfo": {Data: []byte(linuxTestMount(mountRoot, mountPoint))},
		"proc/meminfo":        {Data: []byte("MemTotal: 1000000 kB\nMemAvailable: 900000 kB\n")},
		"proc/self/status":    {Data: []byte("Name:\theterolog\nVmRSS:\t1024 kB\n")},
	}
}

func linuxTestMount(root, point string) string {
	escape := strings.NewReplacer("\\", "\\134", " ", "\\040", "\t", "\\011", "\n", "\\012")
	return fmt.Sprintf("29 23 0:26 %s %s rw,nosuid shared:7 - cgroup2 cgroup rw\n", escape.Replace(root), escape.Replace(point))
}

func setLinuxTestLimit(filesystem fstest.MapFS, directory, current, maximum string) {
	directory = strings.TrimPrefix(directory, "/")
	filesystem[path.Join(directory, "memory.current")] = &fstest.MapFile{Data: []byte(current + "\n")}
	filesystem[path.Join(directory, "memory.max")] = &fstest.MapFile{Data: []byte(maximum + "\n")}
}

func TestLinuxMemoryPressureResolvesProcessCgroup(t *testing.T) {
	for _, tc := range []struct {
		name, group, root, mount, directory string
	}{
		{"namespace root", "/", "/", "/sys/fs/cgroup", "/sys/fs/cgroup"},
		{"systemd service", "/system.slice/log.service", "/", "/sys/fs/cgroup", "/sys/fs/cgroup/system.slice/log.service"},
		{"alternate mount", "/app", "/", "/run/unified", "/run/unified/app"},
		{"subtree bind mount", "/tenant/service", "/tenant", "/run/memory", "/run/memory/service"},
		{"membership at mount root", "/tenant", "/tenant", "/run/memory", "/run/memory"},
		{"escaped mount paths", "/tenant group/service\\x2dname", "/tenant group", "/run/cg space\tline\n\\dir", "/run/cg space\tline\n\\dir/service\\x2dname"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := linuxMemoryFixture(tc.group, tc.root, tc.mount)
			setLinuxTestLimit(filesystem, tc.directory, "99614720", "104857600")
			// rootに見えるこのおとりが、processの実際のcgroupを上書きしてはいけません。
			if tc.directory != "/sys/fs/cgroup" {
				setLinuxTestLimit(filesystem, "/sys/fs/cgroup", "104857600", "1073741824")
			}
			p, err := readLinuxMemoryPressure(context.Background(), filesystem)
			if err != nil {
				t.Fatal(err)
			}
			if p.Source != SourceLinuxCgroupV2 || !p.Reliable || p.DirectHost || !p.Container ||
				p.Ratio != .95 || p.UsedBytes != 99614720 || p.LimitBytes != 104857600 || p.AvailableBytes != 5242880 ||
				p.RSSBytes != 1024*1024 || !p.RSSReliable || !IsCriticalOverload(p, .9) {
				t.Fatalf("process cgroup must determine pressure, got %+v", p)
			}
			if p.CgroupLimit != (MemoryLimitUsage{UsedBytes: 99614720, LimitBytes: 104857600, Reliable: true}) {
				t.Fatalf("remaining-byte constraint = %+v", p.CgroupLimit)
			}
		})
	}
}

func TestLinuxMemoryPressureChecksAncestorConstraintsIndependently(t *testing.T) {
	filesystem := linuxMemoryFixture("/tenant/app", "/", "/sys/fs/cgroup")
	// 子は残りの予算が最も小さく、その親は使用率が最も高い状態です。
	setLinuxTestLimit(filesystem, "/sys/fs/cgroup/tenant/app", "80", "100")
	setLinuxTestLimit(filesystem, "/sys/fs/cgroup/tenant", "900", "1000")
	p, err := readLinuxMemoryPressure(context.Background(), filesystem)
	if err != nil {
		t.Fatal(err)
	}
	if p.Ratio != .9 || p.UsedBytes != 900 || p.LimitBytes != 1000 || p.AvailableBytes != 100 {
		t.Fatalf("primary observation must retain one consistent scope: %+v", p)
	}
	if p.CgroupLimit != (MemoryLimitUsage{UsedBytes: 80, LimitBytes: 100, Reliable: true}) {
		t.Fatalf("smallest remaining constraint = %+v", p.CgroupLimit)
	}
	if !MeetsCriticalThresholds(p, .85, 0) || !MeetsCriticalThresholds(p, .95, 20) || MeetsCriticalThresholds(p, .95, 19) {
		t.Fatalf("ratio and remaining-byte thresholds must independently see their constraints: %+v", p)
	}
}

func TestLinuxMemoryPressureKeepsKnownAncestorOrChildWhenOtherScopesAreUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name, childCurrent, childMax, parentCurrent, parentMax string
		unreadableChild, unreadableParent                      bool
	}{
		{name: "unlimited child", childCurrent: "10", childMax: "max", parentCurrent: "95", parentMax: "100"},
		{name: "unlimited parent", childCurrent: "95", childMax: "100", parentCurrent: "100", parentMax: "max"},
		{name: "malformed child", childCurrent: "broken", childMax: "100", parentCurrent: "95", parentMax: "100"},
		{name: "malformed parent", childCurrent: "95", childMax: "100", parentCurrent: "broken", parentMax: "100"},
		{name: "unreadable child", childCurrent: "10", childMax: "100", parentCurrent: "95", parentMax: "100", unreadableChild: true},
		{name: "unreadable parent", childCurrent: "95", childMax: "100", parentCurrent: "10", parentMax: "100", unreadableParent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := linuxMemoryFixture("/tenant/app", "/", "/sys/fs/cgroup")
			setLinuxTestLimit(filesystem, "/sys/fs/cgroup/tenant/app", tc.childCurrent, tc.childMax)
			setLinuxTestLimit(filesystem, "/sys/fs/cgroup/tenant", tc.parentCurrent, tc.parentMax)
			var source fs.FS = filesystem
			if tc.unreadableChild {
				source = linuxReadFailureFS{FS: source, name: "sys/fs/cgroup/tenant/app/memory.current"}
			}
			if tc.unreadableParent {
				source = linuxReadFailureFS{FS: source, name: "sys/fs/cgroup/tenant/memory.current"}
			}
			p, err := readLinuxMemoryPressure(context.Background(), source)
			if err != nil || p.Source != SourceLinuxCgroupV2 || p.Ratio != .95 || !IsCriticalOverload(p, .9) {
				t.Fatalf("known constraint was lost: %+v, %v", p, err)
			}
		})
	}
}

type linuxReadFailureFS struct {
	fs.FS
	name string
}

func (filesystem linuxReadFailureFS) Open(name string) (fs.File, error) {
	if name == filesystem.name {
		return nil, fs.ErrPermission
	}
	return filesystem.FS.Open(name)
}

func TestLinuxMemoryPressureDoesNotWalkOutsideVisibleMount(t *testing.T) {
	filesystem := linuxMemoryFixture("/tenant/app", "/tenant", "/run/visible")
	setLinuxTestLimit(filesystem, "/run/visible/app", "40", "100")
	setLinuxTestLimit(filesystem, "/run/visible", "50", "100")
	setLinuxTestLimit(filesystem, "/run", "100", "100")
	p, err := readLinuxMemoryPressure(context.Background(), filesystem)
	if err != nil || p.Ratio != .5 || p.CgroupLimit.UsedBytes != 50 {
		t.Fatalf("reader crossed cgroup mount boundary: %+v, %v", p, err)
	}
}

func TestLinuxMemoryPressureUsesOtherVisibleMountToReadAncestor(t *testing.T) {
	filesystem := linuxMemoryFixture("/tenant/app", "/tenant/app", "/run/leaf")
	filesystem["proc/self/mountinfo"].Data = append(filesystem["proc/self/mountinfo"].Data, []byte(linuxTestMount("/", "/run/all"))...)
	setLinuxTestLimit(filesystem, "/run/leaf", "20", "100")
	setLinuxTestLimit(filesystem, "/run/all/tenant", "95", "100")
	p, err := readLinuxMemoryPressure(context.Background(), filesystem)
	if err != nil || p.Ratio != .95 || !IsCriticalOverload(p, .9) {
		t.Fatalf("broader visible mount's parent constraint was lost: %+v, %v", p, err)
	}
}

func TestLinuxMemoryPressureFallsBackToHostWhenNoFiniteConstraintIsReadable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(fstest.MapFS)
	}{
		{"no membership file", func(f fstest.MapFS) { delete(f, "proc/self/cgroup") }},
		{"legacy only", func(f fstest.MapFS) { f["proc/self/cgroup"].Data = []byte("4:memory:/app\n") }},
		{"missing mountinfo", func(f fstest.MapFS) { delete(f, "proc/self/mountinfo") }},
		{"no memory controller", func(f fstest.MapFS) {}},
		{"unlimited", func(f fstest.MapFS) { setLinuxTestLimit(f, "/sys/fs/cgroup/app", "10", "max") }},
		{"malformed maximum", func(f fstest.MapFS) { setLinuxTestLimit(f, "/sys/fs/cgroup/app", "10", "broken") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := linuxMemoryFixture("/app", "/", "/sys/fs/cgroup")
			tc.change(filesystem)
			p, err := readLinuxMemoryPressure(context.Background(), filesystem)
			if err != nil || p.Source != SourceLinuxProcMem || !p.DirectHost || p.Container || !p.Reliable || p.Ratio != .1 ||
				p.CgroupLimit != (MemoryLimitUsage{}) || IsCriticalOverload(p, .9) || !p.RSSReliable {
				t.Fatalf("host fallback must identify its scope: %+v, %v", p, err)
			}
		})
	}
}

func TestLinuxMemoryPressureDoesNotReplaceKnownLimitWithHostAfterUsageReadFailure(t *testing.T) {
	for _, failure := range []string{"missing", "malformed", "permission denied"} {
		t.Run(failure, func(t *testing.T) {
			filesystem := linuxMemoryFixture("/app", "/", "/sys/fs/cgroup")
			setLinuxTestLimit(filesystem, "/sys/fs/cgroup/app", "95", "100")
			var source fs.FS = filesystem
			name := "sys/fs/cgroup/app/memory.current"
			switch failure {
			case "missing":
				delete(filesystem, name)
			case "malformed":
				filesystem[name].Data = []byte("broken")
			case "permission denied":
				source = linuxReadFailureFS{FS: source, name: name}
			}
			p, err := readLinuxMemoryPressure(context.Background(), source)
			if err != nil || p.Source != SourceLinuxCgroupV2 || p.DirectHost || !p.Container || p.Reliable || p.Ratio != 0 ||
				p.LimitBytes != 100 || p.CgroupLimit.Reliable || !p.RSSReliable {
				t.Fatalf("known limit with unknown usage was replaced by host: %+v, %v", p, err)
			}
		})
	}
}

func TestLinuxMemoryPressurePreservesMeasuredZeroAndClampsUsageAboveLimit(t *testing.T) {
	for _, tc := range []struct {
		name, used, limit string
		wantUsed          uint64
		wantRatio         float64
		critical          bool
	}{
		{"zero usage", "0", "100", 0, 0, false},
		{"at limit", "100", "100", 100, 1, true},
		{"above limit", "120", "100", 100, 1, true},
		{"zero limit", "0", "0", 0, 1, true},
		{"zero limit with residual usage", "10", "0", 0, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := linuxMemoryFixture("/app", "/", "/sys/fs/cgroup")
			setLinuxTestLimit(filesystem, "/sys/fs/cgroup/app", tc.used, tc.limit)
			p, err := readLinuxMemoryPressure(context.Background(), filesystem)
			if err != nil || !p.Reliable || !p.CgroupLimit.Reliable || p.UsedBytes != tc.wantUsed || p.Ratio != tc.wantRatio ||
				IsCriticalOverload(p, .9) != tc.critical || MeetsCriticalThresholds(p, 0, 1) != tc.critical {
				t.Fatalf("zero/over-limit measurement = %+v, %v", p, err)
			}
		})
	}
}

func TestLinuxMemoryPressureRejectsUnresolvablePathsWithoutReadingUnrelatedLimits(t *testing.T) {
	for _, tc := range []struct{ name, membership, mountinfo string }{
		{"membership traversal", "0::/../app\n", linuxTestMount("/", "/sys/fs/cgroup")},
		{"relative membership", "0::app\n", linuxTestMount("/", "/sys/fs/cgroup")},
		{"duplicate membership", "0::/app\n0::/other\n", linuxTestMount("/", "/sys/fs/cgroup")},
		{"mount root prefix collision", "0::/tenant-other/app\n", linuxTestMount("/tenant", "/sys/fs/cgroup")},
		{"mount traversal", "0::/app\n", "29 23 0:26 / /sys/../sys/fs/cgroup rw - cgroup2 cgroup rw\n"},
		{"invalid mount escape", "0::/app\n", "29 23 0:26 / /sys/fs/cgroup\\057 rw - cgroup2 cgroup rw\n"},
		{"legacy mount", "0::/app\n", "29 23 0:26 / /sys/fs/cgroup rw - cgroup cgroup rw\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := linuxMemoryFixture("/app", "/", "/sys/fs/cgroup")
			filesystem["proc/self/cgroup"].Data = []byte(tc.membership)
			filesystem["proc/self/mountinfo"].Data = []byte(tc.mountinfo)
			setLinuxTestLimit(filesystem, "/sys/fs/cgroup/app", "99", "100")
			setLinuxTestLimit(filesystem, "/sys/fs/cgroup", "99", "100")
			p, err := readLinuxMemoryPressure(context.Background(), filesystem)
			if err != nil || p.Source != SourceLinuxProcMem || p.Ratio != .1 {
				t.Fatalf("unrelated cgroup was used: %+v, %v", p, err)
			}
		})
	}
}

func TestLinuxMemoryPressureRefreshesMembershipAndKeepsRSSIndependent(t *testing.T) {
	filesystem := linuxMemoryFixture("/first", "/", "/sys/fs/cgroup")
	setLinuxTestLimit(filesystem, "/sys/fs/cgroup/first", "95", "100")
	setLinuxTestLimit(filesystem, "/sys/fs/cgroup/second", "20", "100")
	first, err := readLinuxMemoryPressure(context.Background(), filesystem)
	if err != nil || first.Ratio != .95 || !first.RSSReliable {
		t.Fatalf("first observation: %+v, %v", first, err)
	}
	filesystem["proc/self/cgroup"].Data = []byte("0::/second\n")
	delete(filesystem, "proc/self/status")
	second, err := readLinuxMemoryPressure(context.Background(), filesystem)
	if err != nil || second.Ratio != .2 || !second.Reliable || second.RSSReliable || second.RSSBytes != 0 {
		t.Fatalf("fresh observation: %+v, %v", second, err)
	}
}

func TestLinuxMemoryPressurePropagatesCancellationAndHostReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readLinuxMemoryPressure(ctx, fstest.MapFS{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if _, err := readLinuxMemoryPressure(context.Background(), fstest.MapFS{}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing host fallback must not return a successful observation: %v", err)
	}
}
