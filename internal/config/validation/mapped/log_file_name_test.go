package mappedvalidation

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// TestMappedLogFileNameValidationは、ファイル名に使うTarget・書式・有効な分離prefixに':'があれば、
// 通常出力・handler用の既定設定・内部ログ専用設定のいずれでも拒否することを確認します。
// WindowsでNTFSの代替データストリームへ書き込む問題を防ぐ制約ですが、検証は全OS共通です。
// ここでは変換済み設定の検証結果を確認し、命名形式全般の妥当性や実ファイルの作成は検証しません。
func TestMappedLogFileNameValidation(t *testing.T) {
	cases := []struct {
		name string
		edit func(*pkglogger.FileLoggerConfig)
		want string
	}{
		// 名前構成値に':'のない基準設定は受理する。wantが空のケースはエラーなしを期待する。
		{name: "valid"},
		{name: "target", edit: func(cfg *pkglogger.FileLoggerConfig) {
			// LogDir=..で保存先directoryからTargetが相殺されても、ファイル名には残るため拒否する。
			cfg.FsstoreConfig.Layout.Target = "app:stream"
			cfg.FsstoreConfig.Layout.LogDir = ".."
		}, want: "target"},
		{name: "file name format", edit: func(cfg *pkglogger.FileLoggerConfig) {
			// Targetだけでなく、ファイル名テンプレート自身に含まれる':'も拒否する。
			cfg.FsstoreConfig.Layout.FileNameFormat = "/%s%s:%s_%d.log"
		}, want: "file name format"},
		{name: "file date format", edit: func(cfg *pkglogger.FileLoggerConfig) {
			// 日付書式からファイル名へ時刻区切りの':'が入る場合も拒否する。
			cfg.FsstoreConfig.Layout.FileDateFormat = "20060102T15:04:05"
		}, want: "file date format"},
		// 基準設定ではerror/debugの分離出力が有効なので、両prefixが検証対象になる。
		{name: "error log prefix", edit: func(cfg *pkglogger.FileLoggerConfig) {
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error:"
		}, want: "error log prefix"},
		{name: "debug log prefix", edit: func(cfg *pkglogger.FileLoggerConfig) {
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug:"
		}, want: "debug log prefix"},
		{name: "disabled lifecycle still writes files", edit: func(cfg *pkglogger.FileLoggerConfig) {
			// 圧縮・upload・purgeを無効にしてもログの書込みは行うため、命名検証は省略しない。
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
			cfg.FsstoreConfig.Layout.Target = "app:stream"
		}, want: "target"},
		{name: "inactive prefixes", edit: func(cfg *pkglogger.FileLoggerConfig) {
			// 分離出力を使わないログ種別のprefixはファイル名に使われず、不正な値が残っていても受理する。
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error:"
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug:"
		}},
		{name: "drive paths and literal brackets", edit: func(cfg *pkglogger.FileLoggerConfig) {
			// 保存先のC:はWindowsのドライブ指定であり、ファイル名構成値の':'禁止とは別物。
			// directoryや名前の角括弧もこの検証では拒否しない。全OSで文字列として検証し、
			// 実ドライブへのアクセスや角括弧を含むファイルの探索までは確認しない。
			cfg.FsstoreConfig.Layout.LogPath = `C:\logs[old]`
			cfg.FsstoreConfig.Layout.Target = "app[1]"
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error[1]_"
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug[1]_"
			cfg.FsstoreConfig.Layout.FileNameFormat = pkgfsstore.FileNameFormat
			cfg.FsstoreConfig.Layout.FileDateFormat = pkgfsstore.FileDateFormat
			cfg.ArchiveCheckpointConfig.Directory = `C:\state\checkpoints`
		}},
	}
	// 同期・非同期は同じvalidatorへ異なるsource labelを渡し、Deliveryは専用validatorを通す。
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, route := range []string{"normal", "handler base", "internal override"} {
			for _, tc := range cases {
				t.Run(source+"/"+route+"/"+tc.name, func(t *testing.T) {
					cfg := mappedRolePurgeFileConfig()
					if tc.edit != nil {
						tc.edit(cfg)
					}
					err := validateMappedLogFileNameDestination(source, route, cfg, nil)
					if tc.want == "" {
						if err != nil {
							t.Fatalf("validation error = %v, want nil", err)
						}
						return
					}
					label := source + " file"
					if route == "internal override" {
						label = source + " internal log file"
					}
					// 不正な項目名だけでなく、どのloggerの通常用／内部ログ用設定かもエラーから識別できること。
					requireLogFileNameComponentError(t, err, label, tc.want)
				})
			}
		}
	}
}

// requireLogFileNameComponentErrorは、errがsourceのファイル名設定について、fieldの値に':'が
// 含まれることを理由に拒否したerrorを含むことを確認します。
func requireLogFileNameComponentError(t *testing.T, err error, source, field string) {
	t.Helper()
	configErr := requireConfigError(t, err, source, ConfigSectionFileName, pkgfsstore.ErrInvalidLogFileNameComponent)
	if _, ok := errortree.Find(configErr, func(nameErr *pkgfsstore.LogFileNameComponentError) bool {
		return nameErr.Field == field
	}); !ok {
		t.Fatalf("%s file name error = %v, want %s component error", source, configErr, field)
	}
}

// TestMappedLogFileNameIgnoresUnusedBaseは、使われない既定のfile設定まで拒否しないことを確認します。
// stdoutだけを使う場合と、stdoutのerror通知先を正常な内部ログ専用file設定で置き換える場合に、
// 既定設定へ':'を含む値が残っていても同期・非同期・Deliveryの設定検証が成功することを期待します。
func TestMappedLogFileNameIgnoresUnusedBase(t *testing.T) {
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, route := range []string{"unused", "internal override"} {
			t.Run(source+"/"+route, func(t *testing.T) {
				invalid := mappedRolePurgeFileConfig()
				invalid.FsstoreConfig.Layout.Target = "app:stream"
				invalid.FsstoreConfig.Layout.FileNameFormat = "/%s%s:%s_%d.log"
				invalid.FsstoreConfig.Layout.FileDateFormat = "15:04:05"
				invalid.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error:"
				invalid.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug:"
				if err := validateMappedLogFileNameDestination(source, route, mappedRolePurgeFileConfig(), invalid); err != nil {
					t.Fatalf("unused base validation error = %v, want nil", err)
				}
			})
		}
	}
}

// validateMappedLogFileNameDestinationは、file設定が使用される経路を組み立ててvalidatorを呼びます。
// normalは通常のfile出力、handler baseはstdoutのerror通知先で既定のfile設定を使う経路です。
// internal overrideは同じ通知先に内部ログ専用file設定を指定し、既定設定を使わない経路です。
// unusedはstdout出力だけでfileを参照しません。unusedBaseがあれば、それを未使用の既定設定に置きます。
// loggerの生成やログ送信はせず、Deliveryの場合だけ設定型を変換して専用validatorへ渡します。
func validateMappedLogFileNameDestination(source, route string, file, unusedBase *pkglogger.FileLoggerConfig) error {
	base := file
	if unusedBase != nil {
		base = unusedBase
	}
	if source == "delivery logger" {
		cfg := pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			File:                  mappedRolePurgeDeliveryConfig(base),
		}
		if route != "normal" {
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
			cfg.Stdout = &pkglogger.DeliveryStdoutLoggerConfig{}
			if route != "unused" {
				cfg.Stdout.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}}
			}
		}
		if route == "internal override" {
			cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{File: mappedRolePurgeDeliveryConfig(file)}
		}
		return ValidateDeliveryLoggerConfig(source, cfg)
	}
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File:                  base,
	}
	if route != "normal" {
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
		cfg.Stdout = &pkglogger.StdoutLoggerConfig{}
		if route != "unused" {
			cfg.Stdout.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}}
		}
	}
	if route == "internal override" {
		cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: file}
	}
	return ValidateSyncLoggerConfig(source, cfg)
}

// TestMappedLogFileNameFailureArchiveは、OpenSearchの送信失敗をfile archiveへ保存する設定について、
// Target・ファイル名書式・日付書式の':'を全OS共通で拒否することを確認します。角括弧入りTargetは受理します。
// lifecycle=disableでも保存自体は行うため、選択中のarchiveには命名検証が必要です。
// 一方、OpenSearchを通常出力に使わない場合や保存方式にfile archiveを選ばない場合は、
// 未使用のarchive設定に':'が残っていても拒否しません。実際の送信・保存や他backendの網羅試験ではありません。
func TestMappedLogFileNameFailureArchive(t *testing.T) {
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, selection := range []string{"selected", "unused backend", "unselected archive"} {
			for _, tc := range []struct {
				name           string
				target         string
				fileNameFormat string
				fileDateFormat string
				want           string
			}{
				{name: "target", target: "app:stream", want: "target"},
				{name: "file name format", fileNameFormat: "/%s%s:%s_%d.jsonl", want: "file name format"},
				{name: "file date format", fileDateFormat: "20060102T15:04:05", want: "file date format"},
				{name: "literal brackets", target: "app[1]"},
			} {
				t.Run(source+"/"+selection+"/"+tc.name, func(t *testing.T) {
					var err error
					if source == "delivery logger" {
						cfg := failureArchiveLifecycleValidationDeliveryLoggerConfig(pkgfsstore.FileLifecycleModeDisable, objectstorage.UploadTargetConfig{})
						archive := &cfg.OpenSearch.FailureStoreConfig.FileArchiveConfig
						archive.Target = tc.target
						archive.FileNameFormat = tc.fileNameFormat
						archive.FileDateFormat = tc.fileDateFormat
						if selection == "unused backend" {
							cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
						} else if selection == "unselected archive" {
							cfg.OpenSearch.FailureStoreConfig.DurableType = []durable.DurableType{durable.DurableTypeNone}
						}
						err = validateMappedDeliveryFailureStores(source, cfg)
					} else {
						cfg := failureArchiveLifecycleValidationLoggerConfig(pkgfsstore.FileLifecycleModeDisable, objectstorage.UploadTargetConfig{})
						archive := &cfg.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig
						archive.Target = tc.target
						archive.FileNameFormat = tc.fileNameFormat
						archive.FileDateFormat = tc.fileDateFormat
						if selection == "unused backend" {
							cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
						} else if selection == "unselected archive" {
							cfg.OpenSearch.FailureStoreConfig.DurableType = []durable.DurableType{durable.DurableTypeNone}
						}
						err = validateMappedFailureStores(source, cfg)
					}
					if selection != "selected" || tc.want == "" {
						// 未使用設定の不正値、または使用する設定の正常値では検証エラーを期待しない。
						if err != nil {
							t.Fatalf("validation error = %v, want nil", err)
						}
						return
					}
					// 使用するarchiveの不正値は、logger種別・OpenSearch・項目名を示して拒否する。
					requireLogFileNameComponentError(t, err, source+" opensearch failure file archive", tc.want)
				})
			}
		}
	}
}
