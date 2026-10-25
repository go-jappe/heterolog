// Package objectstoragespool は、object storageへ送信する圧縮log fileを
// filesystem上で永続管理します。record用NDJSON spoolとは保存形式を共有しません。
package objectstoragespool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const (
	objectStorageDirectoryName = "objectstorage"
	stagingDirectoryName       = ".staging"
)

var (
	ErrInvalidRoot                  = errors.New("invalid compressed log file upload failure spool root")
	ErrInvalidCompressedLogFileName = errors.New("invalid object storage compressed log file name")
	ErrInvalidCompressedLogPath     = errors.New("invalid object storage compressed log file path")
	ErrCompressedLogAlreadyExists   = errors.New("object storage compressed log file already exists")
	ErrInvalidStateTransition       = errors.New("invalid object storage compressed log file state transition")
	ErrNilContext                   = errors.New("context is nil")
)

// Rootは一つのobject storage serviceとLoggerContractKindが所有するcompressed log file spoolです。
// pathは次の形式です。
//
//	<SpoolPath>/<Target>/spool/objectstorage/<service>/<logger_contract>
//
// serviceにはs3等のobject storage service名、logger_contractにはlogger、
// async_logger、delivery_loggerのいずれかが入ります。
// ForDestinationで宛先IDの子ディレクトリを選び、その下にactiveなどのstateを置きます。
// bucket等の接続設定は保存せず、IDと現在の設定を照合して再送します。
// 宛先root直下の.preservedは通常stateとは別の保全先です。不正なエントリや同名衝突の
// ファイル、checkpointの保存結果の矛盾に備えた元ファイルのコピーを置き、自動再送・purgeから外します。
// 保全ファイルは管理者が確認し、必要な復元や削除を手動で行います。
type Root struct {
	path           string
	service        objectstorage.UploadTarget
	loggerContract pkglogger.LoggerContractKind
	destinationID  objectstorage.DestinationID
}

// ForDestinationは宛先IDを子ディレクトリにした保存先を返します。
// IDは設定から生成した値で、送信設定そのものは保存しません。
func (r Root) ForDestination(id objectstorage.DestinationID) (Root, error) {
	if err := r.validate(); err != nil {
		return Root{}, err
	}
	if r.destinationID != "" {
		return Root{}, fmt.Errorf("%w: root already belongs to a destination", ErrInvalidRoot)
	}
	if err := validateDestinationDirectoryName(string(id)); err != nil {
		return Root{}, err
	}
	path, err := pathidentity.ResolveAndValidateManagedDirectory(r.path, string(id))
	if err != nil {
		return Root{}, fmt.Errorf("resolve destination spool %q: %w", id, err)
	}
	r.path, r.destinationID = path, id
	return r, nil
}

func (r Root) DestinationID() objectstorage.DestinationID { return r.destinationID }

func validateDestinationDirectoryName(name string) error {
	if name == "" || pkgspool.State(name).IsValid() {
		return fmt.Errorf("%w: invalid destination directory %q", ErrInvalidRoot, name)
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' || character == '_') {
			return fmt.Errorf("%w: destination id %q must contain lowercase letters, digits, hyphens or underscores", ErrInvalidRoot, name)
		}
	}
	return validatePathElement(name, "destination id")
}

// NewRootは利用者基点spoolPath/targetのaliasを解決し、その下の固定spool layoutのlinkを拒否します。
// 利用者基点の既存aliasは実体pathへ固定し、管理下stateのlinkはlayout初期化時に拒否します。
// Targetは一つのpath要素として扱い、separatorや相対移動を含む値を拒否します。
func NewRoot(
	spoolPath string,
	target string,
	service objectstorage.UploadTarget,
	loggerContract pkglogger.LoggerContractKind,
) (Root, error) {
	base := strings.TrimSpace(spoolPath)
	if base == "" {
		return Root{}, fmt.Errorf("%w: spool path is empty", ErrInvalidRoot)
	}
	if err := validatePathElement(target, "target"); err != nil {
		return Root{}, fmt.Errorf("%w: %w", ErrInvalidRoot, err)
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

	resolved, err := pathidentity.ResolveAndValidateManagedDirectory(
		filepath.Join(base, target),
		filepath.Join(spool.DirectoryName, objectStorageDirectoryName, string(service), loggerContract.String()),
	)
	if err != nil {
		return Root{}, fmt.Errorf("%w: resolve spool root: %w", ErrInvalidRoot, err)
	}
	return Root{
		path:           resolved,
		service:        service,
		loggerContract: loggerContract,
	}, nil
}

// Pathはserviceとlogger契約を含めて解決したspoolのroot directoryを返します。
func (r Root) Path() string {
	return r.path
}

func (r Root) LoggerContract() pkglogger.LoggerContractKind {
	return r.loggerContract
}

func (r Root) Service() objectstorage.UploadTarget {
	return r.service
}

// StateDirは指定状態の圧縮log fileを直に配置するdirectoryを返します。
func (r Root) StateDir(state pkgspool.State) (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	if !state.IsValid() {
		return "", fmt.Errorf("%w: unsupported state %q", ErrInvalidRoot, state)
	}
	return filepath.Join(r.path, string(state)), nil
}

func (r Root) ActiveDir() string {
	return filepath.Join(r.path, string(pkgspool.StateActive))
}

func (r Root) ProcessingDir() string {
	return filepath.Join(r.path, string(pkgspool.StateProcessing))
}

func (r Root) DoneDir() string {
	return filepath.Join(r.path, string(pkgspool.StateDone))
}

func (r Root) DeadDir() string {
	return filepath.Join(r.path, string(pkgspool.StateDead))
}

func (r Root) QuarantineDir() string {
	return filepath.Join(r.path, string(pkgspool.StateQuarantine))
}

func (r Root) stagingDir() string {
	return filepath.Join(r.path, stagingDirectoryName)
}

// preservedDirは、元データの保全コピー、同名・別内容の既存ファイル、
// state内で見つけた不正名の通常ファイルやdirectory・symlink等の保管先です。
// state directoryの外なので、再送と通常のpurgeはこの中のファイルに触れません。
func (r Root) preservedDir() string {
	return filepath.Join(r.path, infrafsstore.PreservedDirectoryName)
}

func (r Root) compressedLogFilePath(state pkgspool.State, fileName string) (string, error) {
	if err := validateCompressedLogFileName(r.path, fileName); err != nil {
		return "", err
	}
	stateDir, err := r.StateDir(state)
	if err != nil {
		return "", err
	}
	return filepath.Join(stateDir, fileName), nil
}

func (r Root) validate() error {
	if strings.TrimSpace(r.path) == "" ||
		validateObjectStorageService(r.service) != nil ||
		!isSupportedLoggerContract(r.loggerContract) {
		return fmt.Errorf("%w: zero or unsupported root", ErrInvalidRoot)
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

// 初期化時のバリデーション
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

// validateCompressedLogFileNameは、directory配下へ保存する名前が圧縮ログの命名規則に合うかを、
// そのdirectoryの大小文字の規則（pathidentity.DirectoryNameCase）で検証します。
// spoolへ保存・走査する名前には、spool rootまたはその配下のdirectoryを渡します。
func validateCompressedLogFileName(directory, fileName string) error {
	return validateCompressedLogFileNameForNameCase(fileName, pathidentity.DirectoryNameCase(directory))
}

// validateCompressedLogFileNameForNameCaseは、判定済みの規則nameCaseで
// base extensionの大小文字を比較し、圧縮ログの名前を検証します。
func validateCompressedLogFileNameForNameCase(fileName string, nameCase pathidentity.NameCase) error {
	if err := validatePathElement(fileName, "compressed log file name"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCompressedLogFileName, err)
	}
	// 通常ログの.logとbackend送信失敗archiveの.jsonlについて、独自codecを含む
	// 一段の圧縮拡張子を認めます。未圧縮ファイルと圧縮途中の.tmpは対象外です。
	for _, baseExtension := range []string{"log", "jsonl"} {
		stem, generationExtension, ok := infrafsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(
			fileName,
			baseExtension,
			nameCase,
		)
		if ok && stem != "" && generationExtension != "."+baseExtension {
			return nil
		}
	}
	return fmt.Errorf(
		"%w: %q is not a compressed .log or .jsonl file",
		ErrInvalidCompressedLogFileName,
		fileName,
	)
}

// 初期化時のバリデーション
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
	rel, err := filepath.Rel(parent, path)
	if err != nil {
		return fmt.Errorf("%w: resolve relative path: %w", ErrInvalidCompressedLogPath, err)
	}
	if rel == "." || filepath.Dir(rel) != "." {
		return fmt.Errorf("%w: %q is not a direct child of %q", ErrInvalidCompressedLogPath, path, parent)
	}
	if err := validateCompressedLogFileName(parent, rel); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCompressedLogPath, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: inspect compressed log file %q: %w", ErrInvalidCompressedLogPath, path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %q is not a regular file", ErrInvalidCompressedLogPath, path)
	}
	return nil
}
