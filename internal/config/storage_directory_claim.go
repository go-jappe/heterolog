package config

import (
	"errors"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	fsstorefailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	fsstorespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/appstatepath"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const archiveCheckpointDirectoryClaimSource = "application.reserved_directory.archive_checkpoint"

// storageDirectoryClaimは、一つのwriter構成内で同じ保存先directoryを使用する物理file
// role群またはstoreが、そのdirectoryを他の所有単位と共有しないという所有権宣言です。
//
// 特定のstore実装に依存せず、所有権の比較に必要な情報だけを保持します。sourceは
// 衝突したLoggerContractKindとroleまたはstoreをエラーで特定するための診断名です。pathには`.env`の
// 指定値そのものではなく、各storeと同じ規則で解決した保存先の比較keyを格納します。
// Windowsでは大小文字とextended path表記も統一し、実際の保存先表記は変更しません。
//
// sourceには、所有単位に応じて次のような値が入ります。
//   - `logger.file.default+debug`: sync file loggerのdefaultとdebugがdirectoryを共有
//   - `async_logger.file.error`: async error file loggerが独立したdirectoryを使用
//   - `logger.loki.failure_spool`: sync Lokiのfailure spool
//   - `delivery_logger.opensearch.failure_archive`: Delivery OpenSearchのfailure archive
//   - `logger.object_storage_compressed_log.file_archive.s3`: sync logger契約とS3が共有するcompressed log file archive
//   - `async_logger.object_storage_compressed_log.spool.s3`: async logger契約とS3が共有するcompressed log file spool
//   - `application.reserved_directory.archive_checkpoint`: application内部の予約directory
//
// FileLoggerSetのように一つの構成が意図的に同じdirectoryを共有する場合は、その構成内の
// 複数roleを一つの所有単位へまとめてから、他の所有単位と比較します。したがって、
// `logger.file`の`logger`はLoggerContractSyncのcanonical ID、`file`は出力先、
// `default`、`error`、`debug`は物理role名であり、同じpathを
// 使用するroleの組み合わせがdirectoryのownerになります。衝突判定に使用するのは
// pathだけで、sourceは診断専用です。
// pathはフルパスです。
type storageDirectoryClaim struct {
	source string
	path   string
}

// newArchiveCheckpointDirectoryClaimは、archive checkpointを保存するapplication内部の
// 予約directoryを、一つのapplication-level ownerとして宣言します。現在checkpointを使う
// sourceがなくても常に収集し、通常log、failure archive、spoolなどに同じdirectoryやその
// 親子directoryが割り当てられることを防ぎます。
func (a AppConfig) newArchiveCheckpointDirectoryClaim() (storageDirectoryClaim, error) {
	// 利用者が指定するapplication保存先までリンクを解決し、固定名の
	// archive-checkpoints自体はリンクで別の保存先へ振り分けさせません。
	directory := appstatepath.ArchiveCheckpointDirectory(a.FsstoreConfig.AppStatePath, a.RuntimeConfig.Target)
	resolved, err := pathidentity.ResolveAndValidateManagedDirectory(filepath.Dir(directory), filepath.Base(directory))
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{Directory: "archive checkpoint", Err: err}
	}
	return storageDirectoryClaim{
		source: archiveCheckpointDirectoryClaimSource,
		path:   pathidentity.ComparisonKey(resolved),
	}, nil
}

// canonicalStoragePathは、既存祖先のsymlinkを解決した保存先の比較keyを返します。
// 未作成の下位directoryは作らず、空白も保存先名として保持し、runtimeと同じ実体を比較します。
func canonicalStoragePath(path string) (string, error) {
	resolved, err := pathidentity.ResolveDirectory(path)
	if err != nil {
		return "", err
	}
	return pathidentity.ComparisonKey(resolved), nil
}

// newFileLogDirectoryClaimsは、一つのFileLoggerSetで実際に使用するdefault、error、
// debugの保存先について、directory所有権宣言を生成します。正規化後のpathが同じroleは
// 一つの宣言へまとめます。
//
// debugLogDirは共有・分離flagとinternal overrideを適用済みの保存先です。
// directoryの共有はdefaultを含む場合だけ許可し、defaultとは別のdirectoryを
// error/debugだけで共有する設定は拒否します。
// 許可する共有ではfile name prefixで出力fileを分離し、同じpathのroleを一つの所有権宣言へまとめます。
// 一方、LoggerContractKindをまたぐ統合はここでは行わず、sync、async best-effort、delivery間の衝突は後段で
// 検出します。
//
// logPathとtargetを明示的に受け取るため、global layoutを使う通常loggerと、保存先identityを
// 上書きしたinternal-log専用FileLoggerSetの両方をruntimeと同じ結合順で処理できます。
func newFileLogDirectoryClaims(
	source string,
	logPath string,
	target string,
	logDir string,
	errorLogDir string,
	debugLogDir string,
	separateErrorLogs bool,
	separateDebugLogs bool,
) ([]storageDirectoryClaim, error) {
	uses := []struct {
		role    string
		dir     string
		enabled bool
	}{
		{role: "default", dir: logDir, enabled: true},
		{role: "error", dir: errorLogDir, enabled: separateErrorLogs},
		{role: "debug", dir: debugLogDir, enabled: separateDebugLogs},
	}

	claims := make([]storageDirectoryClaim, 0, 3)
	claimIndexByPath := make(map[string]int, 3)
	resolvedPaths := make(map[string]string, len(uses))
	var errs []error

	for _, use := range uses {
		if !use.enabled {
			continue
		}
		resolved, err := canonicalStoragePath(filepath.Join(logPath, target, use.dir))
		if err != nil {
			errs = append(errs, &StorageDirectoryResolutionError{Directory: source + "." + use.role, Err: err})
			continue
		}
		resolvedPaths[use.role] = resolved
		claim := storageDirectoryClaim{source: source + "." + use.role, path: resolved}

		if index, exists := claimIndexByPath[claim.path]; exists {
			claims[index].source += "+" + use.role
			continue
		}

		claimIndexByPath[claim.path] = len(claims)
		claims = append(claims, claim)
	}

	if separateErrorLogs && separateDebugLogs && len(errs) == 0 &&
		resolvedPaths["error"] == resolvedPaths["debug"] &&
		resolvedPaths["default"] != resolvedPaths["error"] {
		errs = append(errs, &SharedRoleLogDirectoryError{
			Source:           source,
			Directory:        resolvedPaths["error"],
			DefaultDirectory: resolvedPaths["default"],
		})
	}

	return claims, errors.Join(errs...)
}

// newArchiveDirectoryClaimは、一つのfailure archive handlerがroot directoryを
// 専有するという所有権宣言を生成します。retryable/permanentのterminal directoryだけを
// 宣言すると、異なるownerが同じrootをterminal名だけ変えて共有できてしまうため、
// archive全体のrootを所有範囲とします。
//
// 同一owner内でもretryable/permanentは別々にrotation、purge、uploadされるため、
// terminal directory同士の一致・包含関係はここで引き続き拒否します。
func (a AppConfig) newArchiveDirectoryClaim(
	source string,
	cfg failure.FileArchiveConfig,
) (storageDirectoryClaim, error) {
	archiveRoot := fsstorefailurearchive.ResolveDirectoryPath(
		a.FsstoreConfig.AppStatePath,
		a.RuntimeConfig.Target,
		cfg.Dir,
		"",
	)
	// runtimeと同じJoin/Cleanで分類名を正規化し、管理rootからの相対パスで検査します。
	retryableRelative, err := filepath.Rel(archiveRoot, filepath.Join(archiveRoot, cfg.DirCaseRetryable))
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure archive retryable",
			Err:       err,
		}
	}
	retryableDirectory, err := pathidentity.ResolveAndValidateManagedDirectory(archiveRoot, retryableRelative)
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure archive retryable",
			Err:       err,
		}
	}
	permanentRelative, err := filepath.Rel(archiveRoot, filepath.Join(archiveRoot, cfg.DirCasePermanent))
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure archive permanent",
			Err:       err,
		}
	}
	permanentDirectory, err := pathidentity.ResolveAndValidateManagedDirectory(archiveRoot, permanentRelative)
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure archive permanent",
			Err:       err,
		}
	}

	retryablePath := pathidentity.ComparisonKey(retryableDirectory)
	permanentPath := pathidentity.ComparisonKey(permanentDirectory)
	if storagePathsOverlap(retryablePath, permanentPath) {
		return storageDirectoryClaim{}, newStorageDirectoryCollisionError(
			storageDirectoryClaim{source: source + ".failure_archive.retryable", path: retryablePath},
			storageDirectoryClaim{source: source + ".failure_archive.permanent", path: permanentPath},
		)
	}

	rootPath, err := canonicalStoragePath(archiveRoot)
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure archive root",
			Err:       err,
		}
	}

	return storageDirectoryClaim{
		source: source + ".failure_archive",
		path:   rootPath,
	}, nil
}

// newSpoolDirectoryClaimは、一つのspool storeがroot directoryを専有するという
// 所有権宣言を生成します。pathの解決にはspool store本体と同じNewRootDirを使用します。
// NewRootDirではtargetとdirをpath要素としてsanitizeするため、例えば`a/b`と`a_b`の
// ようにraw値が異なっていても同じrootになる設定を正しく検出できます。
func (a AppConfig) newSpoolDirectoryClaim(
	source string,
	cfg failure.SpoolConfig,
) (storageDirectoryClaim, error) {
	root, err := fsstorespool.NewRootDir(a.FsstoreConfig.SpoolPath, a.RuntimeConfig.Target, cfg.Dir)
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure spool",
			Err:       err,
		}
	}
	resolved, err := canonicalStoragePath(root.String())
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: source + " failure spool",
			Err:       err,
		}
	}

	return storageDirectoryClaim{
		source: source + ".failure_spool",
		path:   resolved,
	}, nil
}

// newObjectStorageCompressedLogFileUploadFailureArchiveDirectoryClaimは、一つのobject storage serviceとlogger契約が共有する
// 通常ログとbackend送信失敗archiveの圧縮ファイルに共通する、upload失敗時のarchive rootを宣言します。
// claimは宛先IDごとの子directoryを含む共通rootを対象にします。
// default/error/debugのfile classは同じrootを使うため、別claimにしません。
func (a AppConfig) newObjectStorageCompressedLogFileUploadFailureArchiveDirectoryClaim(
	contract pkglogger.LoggerContractKind,
	cfg objectstoragecompressedlog.FileArchiveConfig,
	service objectstorage.UploadTarget,
) (storageDirectoryClaim, error) {
	root, err := compressedlogarchive.NewRoot(
		filepath.Join(
			a.FsstoreConfig.AppStatePath,
			a.RuntimeConfig.Target,
			cfg.DirName,
		),
		service,
		contract,
	)
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: contract.String() + " compressed log file upload failure archive",
			Err:       err,
		}
	}
	resolved, err := canonicalStoragePath(root.Path())
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: contract.String() + " compressed log file upload failure archive",
			Err:       err,
		}
	}
	return storageDirectoryClaim{
		source: objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(
			contract,
			"file_archive."+string(service),
		),
		path: resolved,
	}, nil
}

// newObjectStorageCompressedLogFileUploadSpoolDirectoryClaimは、一つのobject storage serviceと
// logger契約が共有するcompressed log file upload failure spoolのactive、processing、done、
// dead、quarantineを含むroot全体を宣言します。実際のstoreと同じNewRootで
// pathを解決します。
func (a AppConfig) newObjectStorageCompressedLogFileUploadSpoolDirectoryClaim(
	contract pkglogger.LoggerContractKind,
	service objectstorage.UploadTarget,
) (storageDirectoryClaim, error) {
	root, err := objectstoragespool.NewRoot(
		a.FsstoreConfig.SpoolPath,
		a.RuntimeConfig.Target,
		service,
		contract,
	)
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: contract.String() + " compressed log file upload failure spool",
			Err:       err,
		}
	}
	resolved, err := canonicalStoragePath(root.Path())
	if err != nil {
		return storageDirectoryClaim{}, &StorageDirectoryResolutionError{
			Directory: contract.String() + " compressed log file upload failure spool",
			Err:       err,
		}
	}
	return storageDirectoryClaim{
		source: objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(
			contract,
			"spool."+string(service),
		),
		path: resolved,
	}, nil
}

func objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(
	contract pkglogger.LoggerContractKind,
	store string,
) string {
	return contract.String() + ".object_storage_compressed_log." + store
}
