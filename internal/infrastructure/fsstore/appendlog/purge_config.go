package appendlog

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// resolveLogFileRolePurgeConfigはerror/debug系列の保持設定を構築時に確定します。
// 個別設定rolePurgeがnilなら、通常logの保持設定normalLogPurgeをそのまま返します。
// 個別設定がある場合、0・負数を含む各値は補完せず、validationへ渡します。
// 通常logとのdirectory共有時だけ容量上限を通常logへ揃え、入力の設定は変更しません。
func resolveLogFileRolePurgeConfig(
	normalLogPurge FilePurgeConfigProvider,
	rolePurge *pkgfsstore.FilePurgeConfig,
	layout FsstoreLayoutConfigProvider,
	roleDirectory string,
) FilePurgeConfigProvider {
	if rolePurge == nil {
		return normalLogPurge
	}
	maxDirSize := rolePurge.MaxDirSize
	defaultPath := pathidentity.ComparisonKey(filepath.Join(layout.LogPath(), layout.Target(), layout.LogDir()))
	rolePath := pathidentity.ComparisonKey(filepath.Join(layout.LogPath(), layout.Target(), roleDirectory))
	if defaultPath == rolePath {
		maxDirSize = normalLogPurge.MaxDirSize()
	}
	return &FilePurgeConfigAdapter{
		UsageCacheTTLConfig:  normalLogPurge.UsageCacheTTL(),
		MaxFileBackupsConfig: rolePurge.MaxFileBackups,
		MaxFileAgeConfig:     rolePurge.MaxFileAge,
		MaxFileDirSizeConfig: maxDirSize,
	}
}

// WithResolvedLogFilePurgeDirectoriesは、初期化時に確定した実体ディレクトリを使い、
// 通常ログと保存先を共有するerror/debugの容量上限を通常ログへ揃えます。
// 日数・世代数は系列ごとの値を維持し、元のcfgは変更しません。
// 検証とinline/runnerの構築で共用します。この関数自体はfilesystemへアクセスしません。
// 無効な系列のdirectoryは空文字で構いません。
func WithResolvedLogFilePurgeDirectories(
	cfg ConfigProvider,
	normalDirectory, errorDirectory, debugDirectory string,
) ConfigProvider {
	sharedPurge := func(rolePurge FilePurgeConfigProvider, directory string) FilePurgeConfigProvider {
		if rolePurge == nil || directory == "" ||
			pathidentity.ComparisonKey(normalDirectory) != pathidentity.ComparisonKey(directory) {
			return rolePurge
		}
		return purgeWithDirectoryLimit{
			FilePurgeConfigProvider: rolePurge,
			maxDirSize:              cfg.Purge().MaxDirSize(),
		}
	}
	return configWithResolvedRolePurge{
		ConfigProvider: cfg,
		errorPurge:     sharedPurge(cfg.ErrorPurge(), errorDirectory),
		debugPurge:     sharedPurge(cfg.DebugPurge(), debugDirectory),
	}
}

type purgeWithDirectoryLimit struct {
	FilePurgeConfigProvider
	maxDirSize uint64
}

func (p purgeWithDirectoryLimit) MaxDirSize() uint64 { return p.maxDirSize }

type configWithResolvedRolePurge struct {
	ConfigProvider
	errorPurge FilePurgeConfigProvider
	debugPurge FilePurgeConfigProvider
}

func (c configWithResolvedRolePurge) ErrorPurge() FilePurgeConfigProvider { return c.errorPurge }
func (c configWithResolvedRolePurge) DebugPurge() FilePurgeConfigProvider { return c.debugPurge }

// CompressionCodecはConfigProvider外の依存なので、保持設定の差し替え時にも引き継ぎます。
func (c configWithResolvedRolePurge) CompressionCodec() pkgfsstore.CompressionCodec {
	if provider, ok := c.ConfigProvider.(CompressionCodecProvider); ok {
		return provider.CompressionCodec()
	}
	return nil
}

// WithPurgeConfigはrunner mode専用で、Purge()の返却値だけをpurgeへ差し替えた
// ConfigProviderを生成します。元のcfgは変更せず、他の設定とCompressionCodecを引き継ぎます。
// 各FileSeriesManagerが自分の系列の保持設定を使うために使用します。
// purgeにはadapterのPurge/ErrorPurge/DebugPurgeから取得した、nilでない設定を渡します。
// error/debugの個別設定省略時の通常logからの継承はadapter生成時に解決済みです。
func WithPurgeConfig(cfg ConfigProvider, purge FilePurgeConfigProvider) ConfigProvider {
	return configWithPurge{ConfigProvider: cfg, purge: purge}
}

type configWithPurge struct {
	ConfigProvider
	purge FilePurgeConfigProvider
}

func (c configWithPurge) Purge() FilePurgeConfigProvider { return c.purge }

// CompressionCodecはConfigProviderに含まれないoptional依存なので明示的に引き継ぎます。
func (c configWithPurge) CompressionCodec() pkgfsstore.CompressionCodec {
	if provider, ok := c.ConfigProvider.(CompressionCodecProvider); ok {
		return provider.CompressionCodec()
	}
	return nil
}

// PurgeRoleは保持設定を検証したlog系列です。
type PurgeRole string

const (
	PurgeRoleDefault PurgeRole = "default"
	PurgeRoleError   PurgeRole = "error"
	PurgeRoleDebug   PurgeRole = "debug"
)

// ErrPurgeConfigRequiredは、有効な系列の保持設定providerがnilであることを表します。
var ErrPurgeConfigRequired = errors.New("log purge config is nil")

// PurgeConfigErrorは、一つの系列の保持設定が不正であることを表します。errors.Asで系列を
// 取り出し、Errに含まれるfsstoreの項目別sentinel（ErrInvalidMaxFileBackupsなど）や
// ErrPurgeConfigRequiredをerrors.Isで判定できます。
type PurgeConfigError struct {
	Role PurgeRole
	Err  error
}

func (e *PurgeConfigError) Error() string {
	if errors.Is(e.Err, ErrPurgeConfigRequired) {
		return fmt.Sprintf("%s log purge config is nil", e.Role)
	}
	return fmt.Sprintf("%s log purge config: %v", e.Role, e.Err)
}

func (e *PurgeConfigError) Unwrap() error { return e.Err }

// ValidateLogFilePurgeConfigsは有効な分離系列の解決済み保持設定を検証します。
// 有効な系列はnilを許可せず、未使用の分離系列の値は検証しません。
// 通常logと共有するdirectoryでは既に解決された通常logの容量上限を検証します。
// 不正な系列ごとにPurgeConfigErrorを返し、複数の系列はerrors.Joinでまとめます。
func ValidateLogFilePurgeConfigs(cfg ConfigProvider) error {
	var errs []error
	validate := func(role PurgeRole, enabled bool, purge FilePurgeConfigProvider) {
		if !enabled {
			return
		}
		if purge == nil {
			errs = append(errs, &PurgeConfigError{Role: role, Err: ErrPurgeConfigRequired})
			return
		}
		if err := fsstore.ValidateFileLifecycleConfigProvider(logFilePurgeValidationConfig{
			FilePurgeConfigProvider: purge,
			maxFileSize:             cfg.Lifecycle().MaxFileSize(),
		}); err != nil {
			errs = append(errs, &PurgeConfigError{Role: role, Err: err})
		}
	}
	validate(PurgeRoleError, cfg.Layout().IsSeparateErrorLogsEnabled(), cfg.ErrorPurge())
	validate(PurgeRoleDebug, cfg.Layout().IsSeparateDebugLogsEnabled(), cfg.DebugPurge())
	return errors.Join(errs...)
}

type logFilePurgeValidationConfig struct {
	FilePurgeConfigProvider
	maxFileSize uint64
}

func (c logFilePurgeValidationConfig) MaxFileSize() uint64 { return c.maxFileSize }
