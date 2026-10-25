package mappedvalidation

import (
	"errors"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// validateMappedAppendLogRunnerConfigは、ログファイルの圧縮・古いファイルの削除などを
// 定期実行するrunnerについて、読み込み・変換済みの設定をアプリ起動時に検証します。
// 対象はfile logger（通常出力・内部ログ）と、backendへの送信失敗レコードを保存する
// file archiveのlifecycleです。spoolの再送runnerは対象ではありません。
//
// lifecycle modeがrunnerの場合だけ、次の数値を検証します。
//   - 実行間隔（Interval）が0より大きいこと。
//   - 自動再起動（Supervisor）が有効なら、再起動までの待機時間（RestartDelay）と
//     最大再起動回数（MaxRestarts）がそれぞれ0より大きいこと。
//
// エラー時にrunnerを終了するStopOnError=trueの場合は、Supervisorの有効化も必須です。
// これは、再起動する仕組みがないままrunnerを終了させないためのアプリ固有の制約です。
// inline/disableではrunner設定を使わないため、検証しません。
// Supervisorが無効なら、未使用のRestartDelay・MaxRestartsも検証しません。
func validateMappedAppendLogRunnerConfig(
	sourceLabel string,
	mode pkgfsstore.FileLifecycleMode,
	config pkgfsstore.AppendLogRunnerConfig,
) error {
	if mode != pkgfsstore.FileLifecycleModeRunner {
		return nil
	}
	var errs []error
	if err := pkgfsstore.ValidateAppendLogRunnerConfig(config); err != nil {
		errs = append(errs, err)
	}
	if config.StopOnError && !config.Supervisor.Enabled {
		errs = append(errs, ErrAppendLogRunnerStopOnErrorRequiresSupervisor)
	}
	if err := errors.Join(errs...); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionAppendLogRunner, Err: err}
	}
	return nil
}
