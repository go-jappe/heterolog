//go:build linux

package memorypressure

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
)

type linuxCgroupMount struct {
	root  string
	point string
}

// readLinuxCgroupV2Pressureは所属cgroupから見える祖先までの有限上限を観測します。
// 主観測は最大使用率のscope、CgroupLimitは最小残量のscopeです。同じscopeに
// 両方の役割がある場合も値を保持し、確定した残量0を判定できるようにします。
// 不可視の祖先は観測できず、一部の読取り失敗で既知の制約を破棄しません。
func readLinuxCgroupV2Pressure(ctx context.Context, filesystem fs.FS) (MemoryPressure, error) {
	membership, err := fs.ReadFile(filesystem, "proc/self/cgroup")
	if err != nil {
		return MemoryPressure{}, err
	}
	group, err := linuxUnifiedCgroupPath(string(membership))
	if err != nil {
		return MemoryPressure{}, err
	}
	mountinfo, err := fs.ReadFile(filesystem, "proc/self/mountinfo")
	if err != nil {
		return MemoryPressure{}, err
	}
	mounts := linuxCgroupMounts(string(mountinfo))
	p := MemoryPressure{Source: SourceLinuxCgroupV2}
	finiteLimitKnown := false
	// 別のbind mountから同じscopeを読める場合、取得成功した観測は重複させません。
	observed := make(map[string]bool)
	for _, mount := range mounts {
		relative, ok := linuxPathUnder(group, mount.root)
		if !ok {
			continue
		}
		for current := group; ; current = path.Dir(current) {
			if err := ctx.Err(); err != nil {
				return MemoryPressure{}, err
			}
			if !observed[current] {
				directory := path.Join(mount.point, relative)
				usage, limited, err := readLinuxCgroupLimit(filesystem, directory)
				if limited {
					// 上限があると分かった後のusage取得失敗はhostの正常値で代用しません。
					if !finiteLimitKnown {
						p.LimitBytes = usage.LimitBytes
					}
					finiteLimitKnown = true
				}
				if err == nil {
					observed[current] = true
					if usage.Reliable {
						ratio := 1.0 // memory.max=0も有効な制約であり、余裕はありません。
						if usage.LimitBytes > 0 {
							ratio = float64(usage.UsedBytes) / float64(usage.LimitBytes)
						}
						remaining := usage.LimitBytes - usage.UsedBytes
						if !p.Reliable || ratio > p.Ratio {
							p.UsedBytes, p.LimitBytes, p.AvailableBytes = usage.UsedBytes, usage.LimitBytes, remaining
							p.Ratio, p.Reliable = ratio, true
						}
						if !p.CgroupLimit.Reliable || remaining < p.CgroupLimit.LimitBytes-p.CgroupLimit.UsedBytes {
							p.CgroupLimit = usage
						}
					}
				}
			}
			if current == mount.root {
				break
			}
			relative = path.Dir(relative)
		}
	}
	if !finiteLimitKnown {
		return MemoryPressure{}, errors.New("no readable finite cgroup v2 memory limit")
	}
	// Containerはcontainer runtimeの有無ではなく、host全体ではない制約付きscopeの
	// limit / usageを使う観測かを表します。systemd serviceのMemoryMaxなどによるcgroupでも、
	// 所属cgroupの有限上限を観測している点はWindowsのJob制約と同じなのでtrueにします。
	// usageを取得できずReliable=falseの観測も、scopeは制約付きのままです。
	p.Container = true
	return p, nil
}

func readLinuxCgroupLimit(filesystem fs.FS, directory string) (MemoryLimitUsage, bool, error) {
	read := func(name string) ([]byte, error) {
		return fs.ReadFile(filesystem, strings.TrimPrefix(path.Join(directory, name), "/"))
	}
	maximum, err := read("memory.max")
	if err != nil {
		return MemoryLimitUsage{}, false, err
	}
	maximumText := strings.TrimSpace(string(maximum))
	if maximumText == "max" {
		return MemoryLimitUsage{}, false, nil
	}
	limit, err := strconv.ParseUint(maximumText, 10, 64)
	if err != nil {
		return MemoryLimitUsage{}, false, fmt.Errorf("invalid cgroup memory.max %q", maximumText)
	}
	usage := MemoryLimitUsage{LimitBytes: limit}
	current, err := read("memory.current")
	if err != nil {
		return usage, true, err
	}
	used, err := strconv.ParseUint(strings.TrimSpace(string(current)), 10, 64)
	if err != nil {
		return usage, true, fmt.Errorf("parse cgroup memory.current: %w", err)
	}
	return MemoryLimitUsage{UsedBytes: min(used, limit), LimitBytes: limit, Reliable: true}, true, nil
}

func linuxUnifiedCgroupPath(body string) (string, error) {
	var group string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		candidate := strings.TrimPrefix(line, "0::")
		if group != "" || !linuxAbsolutePath(candidate) {
			return "", errors.New("invalid unified cgroup membership")
		}
		group = candidate
	}
	if group == "" {
		return "", errors.New("no unified cgroup membership")
	}
	return group, nil
}

// mountinfoのoptional fieldsの個数には依存しません。rootとmountpointは
// procfsのoctal escapeを復号した後に、境界付きのpath照合へ使用します。
func linuxCgroupMounts(body string) []linuxCgroupMount {
	var mounts []linuxCgroupMount
	for _, line := range strings.Split(body, "\n") {
		before, after, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		fields, filesystem := strings.Fields(before), strings.Fields(after)
		if len(fields) < 6 || len(filesystem) < 3 || filesystem[0] != "cgroup2" {
			continue
		}
		root, rootOK := linuxMountPath(fields[3])
		point, pointOK := linuxMountPath(fields[4])
		if rootOK && pointOK {
			mounts = append(mounts, linuxCgroupMount{root: root, point: point})
		}
	}
	return mounts
}

func linuxMountPath(encoded string) (string, bool) {
	var decoded strings.Builder
	for i := 0; i < len(encoded); i++ {
		if encoded[i] != '\\' {
			decoded.WriteByte(encoded[i])
			continue
		}
		if i+3 >= len(encoded) {
			return "", false
		}
		switch encoded[i+1 : i+4] {
		case "040":
			decoded.WriteByte(' ')
		case "011":
			decoded.WriteByte('\t')
		case "012":
			decoded.WriteByte('\n')
		case "134":
			decoded.WriteByte('\\')
		default:
			return "", false
		}
		i += 3
	}
	value := decoded.String()
	return value, linuxAbsolutePath(value)
}

func linuxAbsolutePath(value string) bool {
	return strings.HasPrefix(value, "/") && path.Clean(value) == value && !strings.ContainsRune(value, 0)
}

func linuxPathUnder(value, root string) (string, bool) {
	if value == root {
		return "", true
	}
	prefix := strings.TrimSuffix(root, "/") + "/"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	return strings.TrimPrefix(value, prefix), true
}
