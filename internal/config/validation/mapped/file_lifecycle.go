package mappedvalidation

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// validateMappedLogFilePurgeConfigsは、別ファイルへ出力するerror・debugログの保持設定を検証します。
// 対象は保持世代数・保持日数・容量上限などで、無効な分離設定の値は検証しません。
// 起動時に使用中の保存先を実体pathへ解決し、通常ログと共有するdirectoryの容量上限だけを
// 通常ログへ揃えます。世代数・日数と元設定は変更せず、解決失敗も設定元の名前付きで返します。
func validateMappedLogFilePurgeConfigs(sourceLabel string, cfg appendlog.ConfigProvider) error {
	layout := cfg.Layout()
	if !layout.IsSeparateErrorLogsEnabled() && !layout.IsSeparateDebugLogsEnabled() {
		return nil
	}
	var normalDirectory, errorDirectory, debugDirectory string
	for _, role := range []struct {
		name, directory string
		enabled         bool
		resolved        *string
	}{
		{"normal", layout.LogDir(), true, &normalDirectory},
		{"error", layout.ErrorLogDir(), layout.IsSeparateErrorLogsEnabled(), &errorDirectory},
		{"debug", layout.DebugLogDir(), layout.IsSeparateDebugLogsEnabled(), &debugDirectory},
	} {
		if !role.enabled {
			continue
		}
		directory := filepath.Join(layout.LogPath(), layout.Target(), role.directory)
		resolved, err := pathidentity.ResolveDirectory(directory)
		if err != nil {
			return &ConfigError{
				Source:  sourceLabel,
				Section: ConfigSectionLifecycle,
				Err:     &LogDirectoryResolutionError{Role: role.name, Directory: directory, Err: err},
			}
		}
		*role.resolved = resolved
	}
	cfg = appendlog.WithResolvedLogFilePurgeDirectories(cfg, normalDirectory, errorDirectory, debugDirectory)
	if err := appendlog.ValidateLogFilePurgeConfigs(cfg); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionLifecycle, Err: err}
	}
	return nil
}

// validateMappedLogFileDirectoriesは、既定値・設定継承を適用済みのfile出力設定について、
// 通常ログのLogDirと、個別指定が有効なdebug保存先の空文字・空白だけの値を拒否します。
// file出力を使用しない設定は呼び出し元が除外します。debugの個別指定が無効なら
// DebugLogDirは検証しません。ディレクトリの実在や書込み権限は確認しません。
func validateMappedLogFileDirectories(sourceLabel string, layout pkgfsstore.FsstoreConfig) error {
	if err := errors.Join(
		pkgfsstore.ValidateLogDirectory(layout.LogDir),
		pkgfsstore.ValidateDebugLogDirectory(layout.IsSeparateDebugLogDirEnabled, layout.DebugLogDir),
	); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionDirectory, Err: err}
	}
	return nil
}

// validateMappedLogFileNameConfigは、ファイル名に使うTarget・書式・有効なerror/debugの
// 接頭辞に「:」が含まれていないことを検証します。Windowsで通常のファイルではなく
// NTFSの別データ領域へ書き込む名前になることを防ぐため、全OSで同じ条件を使います。
// 保存先ディレクトリのドライブ指定（C:\logsなど）や、無効な分離設定の接頭辞は対象外です。
func validateMappedLogFileNameConfig(sourceLabel string, layout pkgfsstore.FsstoreConfig) error {
	if err := appendlog.ValidateLogFileNameConfigProvider(appendlog.NewFsstoreConfigAdapter(layout)); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionFileName, Err: err}
	}
	return nil
}

// validateMappedUsageCacheTTLは、filesystem使用量の概算cacheを再観測する間隔を検証します。
// 0または負数はエラーにし、ここで既定値へ置き換えることはありません。
// cacheを使うmodeかどうかは呼び出し側が判定し、使わない場合はこの関数を呼びません。
func validateMappedUsageCacheTTL(sourceLabel string, ttl time.Duration) error {
	if err := pkgfsstore.ValidateUsageCacheTTL(ttl); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionLifecycle, Err: err}
	}
	return nil
}

// validateMappedFileLifecycleModeは、同期・非同期file loggerと、送信失敗レコードを
// 保存するfile archiveの圧縮・削除などの実行方式がinline・runner・disableのいずれかを検証します。
// 環境変数が空なら読み込み時に既定値を適用済みなので、変換後に残る空値は設定不備です。
// ライブラリを直接使う場合の既定動作には頼らず、ここでは空値も拒否します。
func validateMappedFileLifecycleMode(sourceLabel string, mode pkgfsstore.FileLifecycleMode) error {
	if mode == "" {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionLifecycle, Err: ErrEmptyFileLifecycleMode}
	}
	if err := pkgfsstore.ValidateFileLifecycleMode(mode); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionLifecycle, Err: err}
	}
	return nil
}

// validateMappedDeliveryFileLifecycleModeは、Deliveryのfile loggerでrunnerまたはdisableが
// 選ばれていることを検証します。ログ書込みに伴って圧縮・削除などを行うinlineは使えません。
// 環境変数の既定値は読み込み時に適用済みなので、変換後の空値も拒否します。
// Deliveryの送信失敗レコード用file archiveは別扱いで、上の通常mode検証を使います。
func validateMappedDeliveryFileLifecycleMode(
	sourceLabel string,
	mode pkgfsstore.FileLifecycleMode,
) error {
	if mode == "" {
		return &ConfigError{
			Source:  sourceLabel,
			Section: ConfigSectionLifecycle,
			Err:     ErrEmptyDeliveryFileLifecycleMode,
		}
	}
	if err := pkgfsstore.ValidateDeliveryFileLifecycleMode(mode); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionLifecycle, Err: err}
	}
	return nil
}
