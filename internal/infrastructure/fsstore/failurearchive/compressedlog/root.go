// Package compressedlogarchive は、object storageへ送信できなかった圧縮log fileを
// 内容を変更せずfilesystemへ永続保存します。既存failure archiveと同じく再送queueでは
// ない単純保存を担当し、圧縮fileをJSONLへ再encodeせずbyte-for-byteで保持します。
package compressedlogarchive

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const (
	temporaryFilePrefix  = ".compressed-log-"
	temporaryFileSuffix  = ".tmp"
	temporaryFilePattern = temporaryFilePrefix + "*" + temporaryFileSuffix
)

var (
	ErrInvalidRoot                = errors.New("invalid compressed log file upload failure archive root")
	ErrInvalidCompressedLogPath   = errors.New("invalid object storage compressed log file path")
	ErrInvalidFailureCategory     = errors.New("invalid object storage upload failure category")
	ErrCompressedLogAlreadyExists = errors.New("object storage compressed log file already exists")
	ErrUnsupportedCompressedLog   = errors.New("unsupported object storage compressed log file")
)

// Rootは一つのobject storage serviceとLoggerContractKindが所有するcompressed log file archiveです。
// NewRootで共通rootを解決し、ForDestinationで宛先別の保存先を選びます。
// 圧縮log fileはその下の分類directoryへ保存します。failure archiveは再送用metadataを必要としないため、bundle
// directoryやmanifestを作りません。各分類directoryはこのapplicationの専有とし、
// 直下の正規なcompressed log file名をpurgeの管理対象として扱います。同名衝突や
// upload途中経過の矛盾で管理者確認が必要なfileは、宛先root直下の.preservedへ集約し、
// 分類別purgeの容量・削除対象から外します。
//
//	<rootPath>/<service>/<logger_contract>/<destination-id>/<retryable|permanent>/<source-file-name>
//	<rootPath>/<service>/<logger_contract>/<destination-id>/.preserved/<preserved-file-name>
type Root struct {
	path           string
	service        objectstorage.UploadTarget
	loggerContract pkglogger.LoggerContractKind
	destinationID  objectstorage.DestinationID
}

// ForDestinationは宛先IDをarchive rootの子ディレクトリにした保存先を返します。
// 宛先ごとに同名ファイルを独立して保存・保持整理します。
func (r Root) ForDestination(id objectstorage.DestinationID) (Root, error) {
	if err := r.validate(); err != nil {
		return Root{}, err
	}
	if r.destinationID != "" {
		return Root{}, fmt.Errorf("%w: root already belongs to a destination", ErrInvalidRoot)
	}
	if err := validateDestinationDirectoryName(string(id)); err != nil {
		return Root{}, fmt.Errorf("%w: %w", ErrInvalidRoot, err)
	}
	path, err := pathidentity.ResolveAndValidateManagedDirectory(r.path, string(id))
	if err != nil {
		return Root{}, fmt.Errorf("%w: resolve destination archive %q: %w", ErrInvalidRoot, id, err)
	}
	r.path, r.destinationID = path, id
	return r, nil
}

func (r Root) DestinationID() objectstorage.DestinationID { return r.destinationID }

// NewRootはfilesystemへ書き込まず、object storage serviceとlogger契約を含むarchive rootを解決します。
// 既存のrootまたは祖先のaliasは実体pathへ固定し、管理下分類directoryのlinkは初期化時に拒否します。
func NewRoot(
	rootPath string,
	service objectstorage.UploadTarget,
	loggerContract pkglogger.LoggerContractKind,
) (Root, error) {
	if strings.TrimSpace(rootPath) == "" {
		return Root{}, fmt.Errorf("%w: root path is empty", ErrInvalidRoot)
	}
	if strings.TrimSpace(rootPath) != rootPath {
		return Root{}, fmt.Errorf("%w: root path has leading or trailing whitespace", ErrInvalidRoot)
	}
	if err := validateObjectStorageService(service); err != nil {
		return Root{}, fmt.Errorf("%w: %w", ErrInvalidRoot, err)
	}
	if !isSupportedLoggerContract(loggerContract) {
		return Root{}, fmt.Errorf(
			"%w: unsupported logger contract %q",
			ErrInvalidRoot,
			loggerContract,
		)
	}

	resolved, err := pathidentity.ResolveAndValidateManagedDirectory(rootPath, filepath.Join(string(service), loggerContract.String()))
	if err != nil {
		return Root{}, fmt.Errorf("%w: resolve archive root: %w", ErrInvalidRoot, err)
	}
	return Root{
		path:           resolved,
		service:        service,
		loggerContract: loggerContract,
	}, nil
}

// Pathは解決済みのarchive rootを返します。ForDestination後は宛先IDを含みます。
func (r Root) Path() string {
	return r.path
}

func (r Root) Service() objectstorage.UploadTarget {
	return r.service
}

// FailureCategoryDirはupload失敗の分類に対応する保存directoryを返します。
func (r Root) FailureCategoryDir(failureCategory uploader.FailureCategory) (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	if !failureCategory.IsValid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidFailureCategory, failureCategory)
	}
	return filepath.Join(r.path, string(failureCategory)), nil
}

func (r Root) RetryableDir() string {
	return filepath.Join(r.path, string(uploader.FailureCategoryRetryable))
}

func (r Root) PermanentDir() string {
	return filepath.Join(r.path, string(uploader.FailureCategoryPermanent))
}

func (r Root) archivedFilePath(
	failureCategory uploader.FailureCategory,
	sourceFileName string,
) (string, error) {
	if err := validateArchivedFileName(r.path, sourceFileName); err != nil {
		return "", err
	}
	failureCategoryDir, err := r.FailureCategoryDir(failureCategory)
	if err != nil {
		return "", err
	}
	return filepath.Join(failureCategoryDir, sourceFileName), nil
}

func (r Root) validate() error {
	if strings.TrimSpace(r.path) == "" ||
		validateObjectStorageService(r.service) != nil ||
		!isSupportedLoggerContract(r.loggerContract) {
		return fmt.Errorf("%w: zero or unsupported root", ErrInvalidRoot)
	}
	return nil
}

// 一時file名は完成済みの「*.log.<codec>」「*.jsonl.<codec>」と重ならない形に限定します。
func isArchiveTemporaryFileName(name string) bool {
	if !strings.HasPrefix(name, temporaryFilePrefix) || !strings.HasSuffix(name, temporaryFileSuffix) {
		return false
	}
	token := strings.TrimSuffix(strings.TrimPrefix(name, temporaryFilePrefix), temporaryFileSuffix)
	return token != "" && !strings.Contains(token, ".")
}

func validateObjectStorageService(service objectstorage.UploadTarget) error {
	value := string(service)
	if err := validatePathElement(value, "object storage service"); err != nil {
		return err
	}
	if value != strings.ToLower(value) {
		return fmt.Errorf("object storage service %q must use lowercase canonical form", value)
	}
	return nil
}

func validateDestinationDirectoryName(name string) error {
	if err := validatePathElement(name, "destination id"); err != nil {
		return err
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' || character == '_') {
			return fmt.Errorf("destination id %q must contain lowercase letters, digits, hyphens or underscores", name)
		}
	}
	return nil
}

func isSupportedLoggerContract(kind pkglogger.LoggerContractKind) bool {
	switch kind {
	case pkglogger.LoggerContractSync,
		pkglogger.LoggerContractAsyncBestEffort,
		pkglogger.LoggerContractDelivery:
		return true
	default:
		return false
	}
}

// validateArchivedFileNameは、directory配下に保存・走査する名前を、そのdirectoryの
// 大小文字の規則（pathidentity.DirectoryNameCase）で検証します。
func validateArchivedFileName(directory, name string) error {
	return validateArchivedFileNameForNameCase(name, pathidentity.DirectoryNameCase(directory))
}

// validateArchivedFileNameForNameCaseは、判定済みの規則nameCaseで保存名を検証します。
func validateArchivedFileNameForNameCase(name string, nameCase pathidentity.NameCase) error {
	if err := validatePathElement(name, "archived file name"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCompressedLogPath, err)
	}
	if _, err := compressedLogFileSuffixForNameCase(name, nameCase); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCompressedLogPath, err)
	}
	return nil
}

// compressedLogFileSuffixは、directory配下へ保存する名前の圧縮ログsuffixを、
// そのdirectoryの大小文字の規則（pathidentity.DirectoryNameCase）で求めます。
func compressedLogFileSuffix(directory, name string) (string, error) {
	return compressedLogFileSuffixForNameCase(name, pathidentity.DirectoryNameCase(directory))
}

// compressedLogFileSuffixForNameCaseは、判定済みの規則nameCaseでbase extensionの
// 大小文字を比較し、元の表記を保った圧縮ログsuffixを返します。
func compressedLogFileSuffixForNameCase(name string, nameCase pathidentity.NameCase) (string, error) {
	if err := validatePathElement(name, "compressed log file name"); err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnsupportedCompressedLog, err)
	}
	// 通常ログの.logと、backend送信失敗レコードを保存した.jsonlを受け入れます。
	// 独自codecも対象にできるよう、元の拡張子の直後に安全な一段の圧縮拡張子が
	// ある完成済みファイルかを調べます。圧縮途中の.tmpは受け入れません。
	compressionExtension := filepath.Ext(name)
	if pkgfsstore.IsValidCompressionFileExtension(compressionExtension) {
		sourceName := strings.TrimSuffix(name, compressionExtension)
		for _, baseExtension := range []string{"log", "jsonl"} {
			stem, extension, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(sourceName, baseExtension, nameCase)
			if ok && stem != "" && extension == "."+baseExtension {
				// 比較は保存先directoryのfile名規則に合わせ、保存名のsuffixは元の表記を保ちます。
				return name[len(stem):], nil
			}
		}
	}
	return "", fmt.Errorf("%w: %q", ErrUnsupportedCompressedLog, name)
}

func validatePathElement(value string, label string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s has leading or trailing whitespace", label)
	}
	if value == "." || value == ".." || filepath.Base(value) != value {
		return fmt.Errorf("%s must be one path element", label)
	}
	if strings.ContainsAny(value, `/\`+"\x00") {
		return fmt.Errorf("%s contains a path separator or NUL", label)
	}
	return nil
}

func ensureDirectChildRegularFile(parent string, path string) error {
	parent = filepath.Clean(parent)
	path = filepath.Clean(path)
	relative, err := filepath.Rel(parent, path)
	if err != nil {
		return fmt.Errorf("%w: resolve relative path: %w", ErrInvalidCompressedLogPath, err)
	}
	if relative == "." || filepath.Dir(relative) != "." {
		return fmt.Errorf(
			"%w: %q is not a direct child of %q",
			ErrInvalidCompressedLogPath,
			path,
			parent,
		)
	}
	if err := validateArchivedFileName(parent, relative); err != nil {
		return err
	}
	if _, err := fsstore.InspectRegularFile(path); err != nil {
		return fmt.Errorf("%w: inspect archived file %q: %w", ErrInvalidCompressedLogPath, path, err)
	}
	return nil
}
