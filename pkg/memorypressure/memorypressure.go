package memorypressure

import (
	"context"
	"errors"
	"time"
)

type PressureSource string

const (
	SourceUnknown       PressureSource = "unknown"
	SourceLinuxCgroupV2 PressureSource = "linux_cgroup_v2"
	SourceLinuxProcMem  PressureSource = "linux_proc_meminfo"
	SourceWindowsAPI    PressureSource = "windows_globalmemorystatusex"
	SourceMacOSVMStat   PressureSource = "macos_vm_stat"
	SourceUnixSysctl    PressureSource = "unix_sysctl"
)

// MonitorSnapshot は、monitor が保持している直近状態の読み取り用スナップショットです。
//
// provider を新たに叩かず、内部状態をそのまま外部へ見せるために使います。
type MonitorSnapshot struct {
	// 直近取得した pressure
	Pressure MemoryPressure

	// 直近取得時刻。未取得なら zero time
	LastReadAt time.Time

	// 直近評価結果
	LastCritical bool

	// 直近で危険状態が何回連続したか
	ConsecutiveHits int

	// 現在の危険状態が始まった時刻。危険状態でなければ zero time
	CriticalSince time.Time
}

// MemoryLimitUsageは、明示的なメモリ上限と、それに対応する現在使用量です。
// Reliableは使用量を取得できたことを表し、正常に観測した0 byteと取得失敗を区別します。
type MemoryLimitUsage struct {
	UsedBytes  uint64
	LimitBytes uint64
	Reliable   bool
}

type MemoryPressure struct {
	// ProcessLimitは、全体上限とは別に課されたprocess単体の追加制約です。
	// WindowsでJob全体・process単体の両上限があるときに使用します。
	// 既存のRatio等はJob全体を表し、判定ではこの追加制約も独立して確認します。
	// process上限しかない場合は既存fieldで表すため、このfieldはゼロ値です。
	// 値型なのでmonitorのsnapshotにも観測時の値がそのままコピーされます。
	ProcessLimit MemoryLimitUsage

	// CgroupLimitはLinuxで読み取れた有限のcgroup制約のうち、残量が最小の観測です。
	// Ratio等の主観測は最大使用率のscopeなので、この制約とは異なる場合があります。
	// 主観測と同じscopeでも設定し、取得済みの残量0を未取得と区別します。
	// このfieldではReliable=trueならLimitBytes=0も有効な上限0（残量0・使用率1）です。
	// ProcessLimitのLimitBytes=0を未取得とする扱いには影響しません。
	// Linux以外・取得不能・有限制約なしではゼロ値です。値型なのでsnapshotのコピーは独立です。
	CgroupLimit MemoryLimitUsage

	// メモリ逼迫率。
	//
	// 想定範囲は 0.0 - 1.0。
	// 0.0 は逼迫なし寄り、1.0 は上限到達寄りを表す。
	//
	// 基本的には UsedBytes / LimitBytes を正規化した値を入れる。
	// ただし、OS・実行環境ごとに取得元が異なるため、
	// 同じ 0.90 でも厳密にまったく同じ意味とは限らない。
	//
	// Reliable=true のときは、overload 判定に使ってよい値を想定する。
	Ratio float64

	// 利用可能メモリ量（bytes）。
	//
	// Linux bare metal では MemAvailable 相当、
	// Linux container では cgroup limit からの残量、
	// WindowsではJob/process上限からの残量、上限がなければ空き物理メモリ相当、
	// macOS / その他 Unix では近似値になることがある。
	//
	// 取得できない場合は 0。
	// 0 は「本当に空きが 0」の意味とは限らず、「未取得」も含む。
	AvailableBytes uint64

	// 判定上の上限値（bytes）。
	//
	// Ratio の分母として使った上限量を入れる。
	// Linux container では cgroup の memory.max、
	// bare metal では総物理メモリ相当を想定する。
	//
	// 取得できない場合は 0。
	// 未取得の0ではRatioを判定に使えないため、Reliable=falseとする。
	// Linuxでmemory.max=0を実際に観測した場合は、Ratio=1とCgroupLimitで
	// 既知の上限到達を表し、使用量も取得できていればReliable=trueとする。
	LimitBytes uint64

	// 現在使用量（bytes）。
	//
	// 基本的には LimitBytes - AvailableBytes、
	// または OS が直接返す使用量を入れる。
	//
	// 取得できない場合は 0。
	// 0 は「未使用」ではなく「未取得」の可能性もある。
	UsedBytes uint64

	// 値の取得元。
	//
	// 例:
	//   - linux_cgroup_v2
	//   - linux_proc_meminfo
	//   - windows_globalmemorystatusex
	//   - macos_vm_stat
	//
	// 呼び出し側はこの値を見て、どの環境視点の値かを判別できる。
	Source PressureSource

	// overload 判定に使ってよい程度に信頼できる値か。
	//
	// true:
	//   その環境で妥当な取得元から算出できた
	// false:
	//   近似しか取れていない、または必要情報が不足している
	//
	// falseの場合、この全体観測は判定に使わない。追加制約の信頼性は独立です。
	Reliable bool

	// ホスト全体視点のメモリ値か。
	//
	// true:
	//   bare metal やホスト視点 API / procfs など
	// false:
	//   container / cgroup など制約付き実行環境視点、または未判定
	DirectHost bool

	// container / cgroup など、実行単位に閉じたメモリ視点か。
	//
	// true:
	//   container 制約下の limit / usage を使っている。
	//   Linuxでは有限のcgroup v2上限、WindowsではJob objectの上限を観測した場合で、
	//   container runtimeの有無ではなく、host全体ではない制約付きscopeかを表す。
	// false:
	//   ホスト全体視点、または container と判定していない
	Container bool

	RSSBytes    uint64
	RSSReliable bool
}

func (p MemoryPressure) Validate() error {
	if p.Ratio < 0 || p.Ratio > 1 {
		return errors.New("invalid memory pressure ratio")
	}
	return nil
}

type Provider interface {
	ReadMemoryPressure(ctx context.Context) (MemoryPressure, error)
}

type defaultProvider struct{}

func New() Provider {
	return defaultProvider{}
}

func IsCriticalOverload(p MemoryPressure, criticalRatio float64) bool {
	if p.Reliable && p.Ratio >= criticalRatio {
		return true
	}
	limit := p.ProcessLimit
	if limit.Reliable && limit.LimitBytes > 0 &&
		float64(min(limit.UsedBytes, limit.LimitBytes))/float64(limit.LimitBytes) >= criticalRatio {
		return true
	}
	limit = p.CgroupLimit
	if !limit.Reliable {
		return false
	}
	if limit.LimitBytes == 0 {
		return criticalRatio <= 1
	}
	return float64(min(limit.UsedBytes, limit.LimitBytes))/float64(limit.LimitBytes) >= criticalRatio
}
