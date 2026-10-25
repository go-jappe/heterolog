package lifecycle

import (
	"github.com/go-jappe/heterolog/pkg/spool/lifecyclemanager/lifecyclefsstore"
	"time"
)

// それぞれdone, dead, quaratineディレクトリのpurgeの設定
type ConfigProvider interface {
	SpoolPath() string
	Target() string
	Dir() string

	EnablePurgeDone() bool
	EnablePurgeDead() bool
	EnablePurgeQuarantine() bool

	// 有効なstateのMaxFileAgeとMaxDirSizeは正の値が必須です。
	DoneMaxFileAge() int
	DoneMaxDirSize() uint64

	DeadMaxFileAge() int
	DeadMaxDirSize() uint64

	QuarantineMaxFileAge() int
	QuarantineMaxDirSize() uint64

	// by-size purge を走らせる最小間隔。
	// 正の値が必須です。
	SizePurgeInterval() time.Duration

	// true の場合、size 推定値キャッシュの利用を許可する。
	// ただし source of truth は filesystem のままとする。
	EnableSizeEstimateCache() bool

	// size check を何回行うごとに full scan を行うか。
	// size estimate cacheが有効な場合は正の値が必須です。
	// 1の場合は毎回full scanとします。
	SizeFullScanEveryNChecks() int

	// full scan を最低でもこの間隔では実行する。
	// size estimate cacheが有効な場合は正の値が必須です。
	SizeFullScanMaxInterval() time.Duration
}

type ConfigAdapter struct {
	spoolPath string
	target    string
	dir       string

	enablePurgeDone       bool
	enablePurgeDead       bool
	enablePurgeQuarantine bool

	doneMaxFileAge int
	doneMaxDirSize uint64

	deadMaxFileAge int
	deadMaxDirSize uint64

	quarantineMaxFileAge int
	quarantineMaxDirSize uint64

	sizePurgeInterval time.Duration

	enableSizeEstimateCache bool

	sizeFullScanEveryNChecks int

	sizeFullScanMaxInterval time.Duration
}

func NewConfigAdapter(cfg lifecyclefsstore.LifeCycleFsstoreConfig) ConfigProvider {
	return &ConfigAdapter{
		spoolPath:                cfg.SpoolPath,
		target:                   cfg.Target,
		dir:                      cfg.Dir,
		enablePurgeDone:          cfg.EnablePurgeDone,
		enablePurgeDead:          cfg.EnablePurgeDead,
		enablePurgeQuarantine:    cfg.EnablePurgeQuarantine,
		doneMaxFileAge:           cfg.DoneMaxFileAge,
		doneMaxDirSize:           cfg.DoneMaxDirSize,
		deadMaxFileAge:           cfg.DeadMaxFileAge,
		deadMaxDirSize:           cfg.DeadMaxDirSize,
		quarantineMaxFileAge:     cfg.QuarantineMaxFileAge,
		quarantineMaxDirSize:     cfg.QuarantineMaxDirSize,
		sizePurgeInterval:        cfg.SizePurgeInterval,
		enableSizeEstimateCache:  cfg.EnableSizeEstimateCache,
		sizeFullScanEveryNChecks: cfg.SizeFullScanEveryNChecks,
		sizeFullScanMaxInterval:  cfg.SizeFullScanMaxInterval,
	}
}

func (a *ConfigAdapter) SpoolPath() string {
	return a.spoolPath
}
func (a *ConfigAdapter) Target() string {
	return a.target
}

func (a *ConfigAdapter) Dir() string {
	return a.dir
}

func (a *ConfigAdapter) EnablePurgeDone() bool {
	return a.enablePurgeDone
}

func (a *ConfigAdapter) EnablePurgeDead() bool {
	return a.enablePurgeDead
}

func (a *ConfigAdapter) EnablePurgeQuarantine() bool {
	return a.enablePurgeQuarantine
}

func (a *ConfigAdapter) DoneMaxFileAge() int {
	return a.doneMaxFileAge
}

func (a *ConfigAdapter) DoneMaxDirSize() uint64 {
	return a.doneMaxDirSize
}

func (a *ConfigAdapter) DeadMaxFileAge() int {
	return a.deadMaxFileAge
}

func (a *ConfigAdapter) DeadMaxDirSize() uint64 {
	return a.deadMaxDirSize
}

func (a *ConfigAdapter) QuarantineMaxFileAge() int {
	return a.quarantineMaxFileAge
}

func (a *ConfigAdapter) QuarantineMaxDirSize() uint64 {
	return a.quarantineMaxDirSize
}

func (a *ConfigAdapter) SizePurgeInterval() time.Duration {
	return a.sizePurgeInterval
}

func (a *ConfigAdapter) EnableSizeEstimateCache() bool {
	return a.enableSizeEstimateCache
}

func (a *ConfigAdapter) SizeFullScanEveryNChecks() int {
	return a.sizeFullScanEveryNChecks
}

func (a *ConfigAdapter) SizeFullScanMaxInterval() time.Duration {
	return a.sizeFullScanMaxInterval
}
