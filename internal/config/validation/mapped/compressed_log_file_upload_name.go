package mappedvalidation

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// 通常のFileLoggerが圧縮前のログファイルに使う拡張子です。
const mappedAppendLogFileExtension = "log"

// mappedCompressedLogFileUploadSeriesは、出力元とアップロード先サービスごとに、
// 日付・連番を除いたログファイル名の構成要素を保持します。
type mappedCompressedLogFileUploadSeries struct {
	source    string
	service   objectstorage.UploadTarget
	prefix    string
	target    string
	extension string
	// directoriesは、この系列のファイル名を照合・保存し得るdirectoryです。元ログの保存先と、
	// 選択されたupload失敗時の退避先（file archive・spool）を含み、大小文字の扱いの判定に使います。
	directories []string
}

// mappedDirectoryNameCaseは、directory直下のファイル名の大小文字の扱いを返す判定です。
// 本番ではpathidentity.DirectoryNameCaseを使い、試験では両方の規則を指定します。
type mappedDirectoryNameCase func(directory string) pathidentity.NameCase

// validateMappedSyncCompressedLogFileUploadNameCollisionsは、同期・非同期Loggerそれぞれで、
// 通常のfile出力と内部ログ専用のfile出力が同じ圧縮ログ名を生成し得る設定を拒否します。
// 両方のfile設定があり、通常の出力先にfileが選ばれている場合（出力先未指定も含む）だけ
// 比較します。さらに各出力でinlineまたはrunnerによるアップロードが有効で、同じサービスを
// 選んだ組だけが対象です。
//
// アップロード失敗時のローカル保存では元directoryを残さず、再送先もファイル名から
// 選ぶため、元directoryや送信先bucket・object keyの違いでは衝突を回避できません。
// prefixとtargetを連結した値、および圧縮前の拡張子が同じなら衝突とします。圧縮形式の
// 変更前に保存されたファイルも再送対象なので、圧縮suffixの違いは区別に使いません。
// 例えば通常出力のtargetがerror_appで、内部ログのerror出力がprefix=error_、target=app
// なら同じ名前になります。異なるLogger設定間や、片側の出力同士の比較は行いません。
// 大小文字は、両系列の元ログの保存先・退避先のいずれかが大小文字を区別しない場合に
// 区別せず比較します。判定にはpathidentity.DirectoryNameCaseを使い、OS名では決めません。
func validateMappedSyncCompressedLogFileUploadNameCollisions(
	sourceLabel string,
	cfg pkglogger.LoggerConfig,
) error {
	return validateMappedSyncCompressedLogFileUploadNameCollisionsForNameCase(
		sourceLabel,
		cfg,
		pathidentity.DirectoryNameCase,
	)
}

// validateMappedSyncCompressedLogFileUploadNameCollisionsForNameCaseは、directoryの大小文字の
// 判定directoryNameCaseを指定して、同期・非同期Loggerの衝突を検証します。
func validateMappedSyncCompressedLogFileUploadNameCollisionsForNameCase(
	sourceLabel string,
	cfg pkglogger.LoggerConfig,
	directoryNameCase mappedDirectoryNameCase,
) error {
	if cfg.File == nil || cfg.InternalLogDestinations == nil ||
		cfg.InternalLogDestinations.File == nil ||
		!pkglogger.LoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile) {
		return nil
	}

	contract := canonicalMappedLoggerContractLabel(sourceLabel)
	base := mappedSyncCompressedLogFileUploadSeries(contract+".file", cfg.File)
	internal := mappedSyncCompressedLogFileUploadSeries(
		contract+".internal_log.file",
		cfg.InternalLogDestinations.File,
	)
	return validateMappedCompressedLogFileUploadSeriesCollisions(base, internal, directoryNameCase)
}

// validateMappedDeliveryCompressedLogFileUploadNameCollisionsは、Delivery Loggerの通常の
// file出力と内部ログ専用のfile出力について、同じサービスへのアップロード失敗時に
// ローカル保存名が重なる設定を拒否します。両方のfile設定があり、通常の出力先にfileが
// 選ばれている場合（出力先未指定も含む）に、runnerによるアップロードが有効な組だけを
// 比較します。比較項目・大小文字の扱いと対象外の範囲は上記と同じです。
func validateMappedDeliveryCompressedLogFileUploadNameCollisions(
	sourceLabel string,
	cfg pkglogger.DeliveryLoggerConfig,
) error {
	return validateMappedDeliveryCompressedLogFileUploadNameCollisionsForNameCase(
		sourceLabel,
		cfg,
		pathidentity.DirectoryNameCase,
	)
}

// validateMappedDeliveryCompressedLogFileUploadNameCollisionsForNameCaseは、directoryの
// 大小文字の判定directoryNameCaseを指定して、Delivery Loggerの衝突を検証します。
func validateMappedDeliveryCompressedLogFileUploadNameCollisionsForNameCase(
	sourceLabel string,
	cfg pkglogger.DeliveryLoggerConfig,
	directoryNameCase mappedDirectoryNameCase,
) error {
	if cfg.File == nil || cfg.InternalLogDestinations == nil ||
		cfg.InternalLogDestinations.File == nil ||
		!pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile) {
		return nil
	}

	contract := canonicalMappedLoggerContractLabel(sourceLabel)
	base := mappedDeliveryCompressedLogFileUploadSeries(contract+".file", cfg.File)
	internal := mappedDeliveryCompressedLogFileUploadSeries(
		contract+".internal_log.file",
		cfg.InternalLogDestinations.File,
	)
	return validateMappedCompressedLogFileUploadSeriesCollisions(base, internal, directoryNameCase)
}

// mappedSyncCompressedLogFileUploadSeriesは、inlineまたはrunnerでアップロードする
// 同期・非同期のfile出力から比較対象を取り出します。設定なし・disableは対象外です。
func mappedSyncCompressedLogFileUploadSeries(
	sourcePrefix string,
	cfg *pkglogger.FileLoggerConfig,
) []mappedCompressedLogFileUploadSeries {
	if cfg == nil || !filelifecycle.UsesObjectStorageUpload(cfg.LifecycleMode) {
		return nil
	}
	return mappedCompressedLogFileUploadSeriesForConfig(
		sourcePrefix,
		cfg.FsstoreConfig.Layout,
		cfg.ObjectStorageUploadConfig,
	)
}

// mappedDeliveryCompressedLogFileUploadSeriesは、Deliveryのrunner設定から比較対象を
// 取り出します。file設定がない場合やrunner以外のmodeでは比較対象を返しません。
func mappedDeliveryCompressedLogFileUploadSeries(
	sourcePrefix string,
	cfg *pkglogger.DeliveryFileLoggerConfig,
) []mappedCompressedLogFileUploadSeries {
	lifecycleCfg := cfg.LifecycleRunnerConfig()
	if lifecycleCfg == nil {
		return nil
	}
	return mappedCompressedLogFileUploadSeriesForConfig(
		sourcePrefix,
		lifecycleCfg.FsstoreConfig.Layout,
		lifecycleCfg.ObjectStorageUploadConfig,
	)
}

// mappedCompressedLogFileUploadSeriesForConfigは、アップロードが有効なdefault出力と、
// 分離が有効なerror・debug出力について、選択されたサービスごとに比較項目を作ります。
// 全体または出力別に無効なアップロードは含めず、同じ出力内のサービス重複は一件にします。
// 各出力の元ログの保存先は、FileLoggerと同じくLogPath・Target・役割別のdirectoryから求めます。
func mappedCompressedLogFileUploadSeriesForConfig(
	sourcePrefix string,
	layout pkgfsstore.FsstoreConfig,
	upload objectstorage.LogFileUploadConfig,
) []mappedCompressedLogFileUploadSeries {
	if !upload.Enabled {
		return nil
	}

	type physicalSeries struct {
		role      string
		prefix    string
		directory string
		upload    objectstorage.UploadTargetConfig
	}
	configured := appendlog.NewFsstoreConfigAdapter(layout)
	logDirectory := func(directory string) string {
		return filepath.Join(configured.LogPath(), configured.Target(), directory)
	}
	series := []physicalSeries{{role: "default", directory: logDirectory(configured.LogDir()), upload: upload.Default}}
	if layout.IsSeparateErrorLogsEnabled {
		series = append(series, physicalSeries{
			role:      "error",
			prefix:    layout.SeparateErrorLogsPrefix,
			directory: logDirectory(configured.ErrorLogDir()),
			upload:    upload.Error,
		})
	}
	if layout.IsSeparateDebugLogsEnabled {
		series = append(series, physicalSeries{
			role:      "debug",
			prefix:    layout.SeparateDebugLogsPrefix,
			directory: logDirectory(configured.DebugLogDir()),
			upload:    upload.Debug,
		})
	}

	var result []mappedCompressedLogFileUploadSeries
	for _, physical := range series {
		if !physical.upload.Enabled {
			continue
		}
		seenServices := make(map[objectstorage.UploadTarget]struct{}, len(physical.upload.Targets))
		for _, service := range physical.upload.Targets {
			if _, seen := seenServices[service]; seen {
				continue
			}
			seenServices[service] = struct{}{}
			result = append(result, mappedCompressedLogFileUploadSeries{
				source:    sourcePrefix + "." + physical.role,
				service:   service,
				prefix:    physical.prefix,
				target:    layout.Target,
				extension: mappedAppendLogFileExtension,
				directories: mappedCompressedLogFileUploadDirectories(
					physical.directory,
					physical.upload,
					upload.CompressedLogFileUploadSpool,
				),
			})
		}
	}
	return result
}

// mappedCompressedLogFileUploadDirectoriesは、元ログの保存先と、upload失敗時に選択された
// 退避先のdirectoryを返します。退避先では元directoryを残さず、複数系列のファイルを同じ
// directoryへ保存します。spoolは送信経路に個別の設定がなければ、共通の設定を使います。
// directoryの作成や存在確認は行いません。
func mappedCompressedLogFileUploadDirectories(
	logDirectory string,
	target objectstorage.UploadTargetConfig,
	sharedSpool objectstorage.CompressedLogFileUploadSpoolConfigProvider,
) []string {
	directories := []string{logDirectory}
	failureStore := target.CompressedLogFileUploadFailureStore
	if slices.Contains(failureStore.DurableType, failurestore.DurableTypeFileArchive) {
		if root := failureStore.DurableStoreConfig.FileArchiveConfig.RootPath; root != "" {
			directories = append(directories, root)
		}
	}
	if slices.Contains(failureStore.DurableType, failurestore.DurableTypeSpool) {
		spool := target.CompressedLogFileUploadSpool
		if spool == nil {
			spool = sharedSpool
		}
		if spool != nil && spool.SpoolPath() != "" {
			directories = append(directories, filepath.Join(spool.SpoolPath(), spool.Target()))
		}
	}
	return directories
}

// validateMappedCompressedLogFileUploadSeriesCollisionsは、通常出力と内部ログ出力の全組合せを
// 比較し、サービスとファイル名の比較値が両方一致した組を、出力元を示すエラーにまとめます。
// ファイル名は、組ごとにmappedCompressedLogFileUploadNameCaseで決めた規則で比較します。
func validateMappedCompressedLogFileUploadSeriesCollisions(
	base []mappedCompressedLogFileUploadSeries,
	internal []mappedCompressedLogFileUploadSeries,
	directoryNameCase mappedDirectoryNameCase,
) error {
	var errs []error
	for _, left := range base {
		for _, right := range internal {
			if left.service != right.service {
				continue
			}
			nameCase := mappedCompressedLogFileUploadNameCase(directoryNameCase, left, right)
			if left.identity(nameCase) != right.identity(nameCase) {
				continue
			}
			errs = append(errs, &CompressedLogFileUploadNameCollisionError{
				Service:  left.service,
				Base:     left.name(),
				Internal: right.name(),
			})
		}
	}
	return errors.Join(errs...)
}

// mappedCompressedLogFileUploadNameCaseは、二つの系列のファイル名を比較する大小文字の規則を返します。
// どちらかの元ログの保存先、または退避先が大小文字を区別しない場合は、同じ退避先への保存や
// ファイル名による再送経路の選択で同じファイルとして扱われ得るため、区別しない規則を使います。
func mappedCompressedLogFileUploadNameCase(
	directoryNameCase mappedDirectoryNameCase,
	series ...mappedCompressedLogFileUploadSeries,
) pathidentity.NameCase {
	for _, current := range series {
		for _, directory := range current.directories {
			if directoryNameCase(directory) == pathidentity.NameCaseInsensitive {
				return pathidentity.NameCaseInsensitive
			}
		}
	}
	return pathidentity.NameCaseSensitive
}

// nameは、エラーで報告するファイル名の構成要素を返します。
func (s mappedCompressedLogFileUploadSeries) name() CompressedLogFileUploadName {
	return CompressedLogFileUploadName{
		Source:    s.source,
		Prefix:    s.prefix,
		Target:    s.target,
		Extension: s.extension,
	}
}

// identityは、prefixとtargetの連結値と圧縮前の拡張子を、境界が混ざらない区切りで結合します。
// 日付・連番は両出力で重なり得るため含めず、一連のログファイル名を比較する値にします。
func (s mappedCompressedLogFileUploadSeries) identity(nameCase pathidentity.NameCase) string {
	// 退避先や再送時のlogpatternによるbasename照合と同じdirectoryの比較規則を使います。
	// 比較だけに使用し、設定されたfile名やobject storage keyは変更しません。
	return nameCase.Key(s.prefix+s.target) + "\x00" + nameCase.Key(s.extension)
}

// canonicalMappedLoggerContractLabelは、エラーに使う設定元の空白区切りをunderscoreへ揃えます。
// 保存先のpathや生成するログファイル名には使いません。
func canonicalMappedLoggerContractLabel(sourceLabel string) string {
	return strings.Join(strings.Fields(sourceLabel), "_")
}
