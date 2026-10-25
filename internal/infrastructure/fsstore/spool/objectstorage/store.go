package objectstoragespool

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const (
	directoryMode      = 0o750
	payloadMode        = 0o640
	stagingFilePrefix  = ".compressed-log-"
	stagingFileSuffix  = ".tmp"
	stagingFilePattern = stagingFilePrefix + "*" + stagingFileSuffix
)

// Storeはobject storageへのuploadに失敗した圧縮log fileを、元のbasenameの
// ままstate directory直下へ永続化します。active/processingの削除操作は提供しません。
type Store struct {
	root Root
	// operationMuは同じStore instanceの探索とfilesystem mutationを直列化し、
	// 同名fileを複数stateへ同時に公開しません。複数processで同じrootを共有する
	// 排他は提供せず、applicationがrootを専有する前提です。
	operationMu infrafsstore.ContextMutex
	// layoutMuは初期化処理とlayoutEnsuredを保護します。EnsureLayoutは
	// operationMu取得中にも呼ぶため、初期化用のロックを分けます。
	layoutMu infrafsstore.ContextMutex
	// layoutEnsuredは全保存先の初期化に一度成功したことを表します。
	// 初期化に失敗した場合はfalseのままにし、次の呼び出しで再試行します。
	layoutEnsured bool
	// preservationWarningsは、不正名や非通常ファイルのエントリを.preservedへ移した通知です。
	// operationMuで保護し、周期処理の終了後に取り出してlock外でOnWarnへ渡します。
	preservationWarnings []error
}

// CompressedLogFileは配置pathから確定した圧縮log fileの現在状態です。Pathは
// upload対象となる圧縮file本体を指します。
type CompressedLogFile struct {
	DestinationID objectstorage.DestinationID
	FileName      string
	State         pkgspool.State
	Path          string
	// ConflictRenamesはこの呼び出し中に同名の既存ファイルを保全した移動先です。
	// 通常の再送一覧から得たファイルでは空です。呼び出し元は成功・失敗に
	// かかわらず、この移動を管理者へ通知してください。
	ConflictRenames []infrafsstore.ConflictRename
}

func NewStore(root Root) (*Store, error) {
	if err := root.validate(); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

func (s *Store) Root() Root {
	return s.root
}

// EnsureLayoutは全state directoryと非公開staging directoryを初期化します。
// lock待ちと各directoryの作成前はctxでキャンセルできます。
// 初期化時はsymlinkを拒否し、新規directoryの親を同期します。失敗はerrorとして返し、
// 次の呼び出しで未作成のdirectoryの初期化を再試行します。同期できなかった親は記録し、
// 別のStoreからの呼び出しも含め、その親の同期を確認できるまでは既存のdirectoryでも
// 初期化を失敗として返します。
// 一度成功したStoreでは、以後の呼び出しはfilesystemを調べずに戻ります。
// 初期化後の外部操作によるdirectoryの変更は検査・修復せず、保存・列挙などの
// 実際の操作で失敗した場合に、その操作のerrorとして返します。
func (s *Store) EnsureLayout(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := s.layoutMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.layoutMu.Unlock()
	return s.ensureLayoutLocked(ctx)
}

// ensureLayoutLockedの呼び出し元はlayoutMuを取得します。
func (s *Store) ensureLayoutLocked(ctx context.Context) error {
	if s.layoutEnsured {
		return nil
	}
	directories := []string{
		s.root.ActiveDir(),
		s.root.ProcessingDir(),
		s.root.DoneDir(),
		s.root.DeadDir(),
		s.root.QuarantineDir(),
		s.root.stagingDir(),
		s.root.preservedDir(),
	}
	for _, directory := range directories {
		if err := contextError(ctx); err != nil {
			return err
		}
		if err := infrafsstore.EnsureDirectoryNoSymlinksContext(ctx, directory, directoryMode); err != nil {
			return fmt.Errorf("create compressed log file spool directory %q: %w", directory, err)
		}
	}
	s.layoutEnsured = true
	return nil
}

// SaveRetryableFailureは再送可能なupload失敗fileをactiveへコピー保存します。
// 公開前にコピー先を読み戻して元ファイルとbyte単位で比較します。圧縮形式には依存せず、
// 比較失敗・不一致なら保存失敗として未公開fileを片付けます。元ファイルは削除しません。
func (s *Store) SaveRetryableFailure(ctx context.Context, sourcePath string) (CompressedLogFile, error) {
	return s.saveToState(ctx, sourcePath, pkgspool.StateActive)
}

// SavePermanentFailureは恒久的なupload失敗fileをdeadへコピー保存します。
// SaveRetryableFailureと同じく、公開前に内容を照合し、元ファイルは削除しません。
func (s *Store) SavePermanentFailure(ctx context.Context, sourcePath string) (CompressedLogFile, error) {
	return s.saveToState(ctx, sourcePath, pkgspool.StateDead)
}

func (s *Store) saveToState(
	ctx context.Context,
	sourcePath string,
	state pkgspool.State,
) (result CompressedLogFile, err error) {
	if s == nil {
		return CompressedLogFile{}, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if state != pkgspool.StateActive && state != pkgspool.StateDead {
		return CompressedLogFile{}, fmt.Errorf("%w: cannot publish to %q", ErrInvalidStateTransition, state)
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	defer s.operationMu.Unlock()
	if strings.TrimSpace(sourcePath) == "" {
		return CompressedLogFile{}, fmt.Errorf("%w: source path is empty", ErrInvalidCompressedLogPath)
	}
	fileName := filepath.Base(sourcePath)
	// 保存先のspool rootの大小文字の規則で、保存する名前を検証します。
	if err := validateCompressedLogFileName(s.root.Path(), fileName); err != nil {
		return CompressedLogFile{}, err
	}
	if err := validateRegularSource(sourcePath); err != nil {
		return CompressedLogFile{}, err
	}
	if err := s.EnsureLayout(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	var conflictRenames []infrafsstore.ConflictRename
	defer func() {
		// rename後にコピーや公開が失敗しても、保全先を通知できるよう返します。
		result.ConflictRenames = append(result.ConflictRenames, conflictRenames...)
	}()

	if existing, found, err := s.adoptOrPreserveExistingFiles(ctx, sourcePath, fileName, &conflictRenames); err != nil {
		return CompressedLogFile{}, err
	} else if found {
		return existing, nil
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, err
	}

	temporary, err := os.CreateTemp(s.root.stagingDir(), stagingFilePattern)
	if err != nil {
		return CompressedLogFile{}, fmt.Errorf("create staged compressed log file: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen := true
	published := false
	defer func() {
		if temporaryOpen {
			if closeErr := temporary.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close staged compressed log file during cleanup: %w", closeErr))
			}
		}
		// キャンセル時も、今回の保存処理が作った未公開tmpだけを除去します。
		// 公開済みfileは残し、再試行時に同内容なら採用します。
		if !published {
			if removeErr := os.Remove(temporaryPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove staged compressed log file %q: %w", temporaryPath, removeErr))
			}
		}
	}()

	if err := infrafsstore.SetPrivateFilePermissions(temporary, payloadMode); err != nil {
		return CompressedLogFile{}, fmt.Errorf("set staged compressed log file mode: %w", err)
	}
	if err := copySourceToFile(ctx, sourcePath, temporary); err != nil {
		return CompressedLogFile{}, err
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	if err := temporary.Sync(); err != nil {
		return CompressedLogFile{}, fmt.Errorf("%w: sync staged compressed log file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	if err := temporary.Close(); err != nil {
		temporaryOpen = false
		return CompressedLogFile{}, fmt.Errorf("close staged compressed log file: %w", err)
	}
	temporaryOpen = false

	// 元ファイルを残したまま、同期・close済みのコピーを読み戻して照合します。
	// 独自codecの内容も展開せず比較し、不一致のfileを再送対象として公開しません。
	equal, err := infrafsstore.EqualRegularFileContents(ctx, sourcePath, temporaryPath)
	if err != nil {
		return CompressedLogFile{}, fmt.Errorf("verify staged compressed log file copy: %w", err)
	}
	if !equal {
		return CompressedLogFile{}, fmt.Errorf("staged compressed log file %w", infrafsstore.ErrCopyDiffersFromSource)
	}

	if existing, found, err := s.adoptOrPreserveExistingFiles(ctx, sourcePath, fileName, &conflictRenames); err != nil {
		return CompressedLogFile{}, err
	} else if found {
		return existing, nil
	}

	finalPath, err := s.root.compressedLogFilePath(state, fileName)
	if err != nil {
		return CompressedLogFile{}, err
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	if renameErr := infrafsstore.RenameFileNoReplace(temporaryPath, finalPath); renameErr != nil {
		if !errors.Is(renameErr, os.ErrExist) {
			return CompressedLogFile{}, fmt.Errorf("publish compressed log file: %w", renameErr)
		}
		// 外部から同じ名前が現れた場合も、内容を確認してから扱います。
		if existing, found, err := s.adoptOrPreserveExistingFiles(ctx, sourcePath, fileName, &conflictRenames); err != nil {
			return CompressedLogFile{}, err
		} else if found {
			return existing, nil
		}
		if err := infrafsstore.RenameFileNoReplace(temporaryPath, finalPath); err != nil {
			return CompressedLogFile{}, fmt.Errorf("publish compressed log file after conflict preservation: %w", err)
		}
	}
	published = true
	file := CompressedLogFile{DestinationID: s.root.DestinationID(), FileName: fileName, State: state, Path: finalPath}
	for _, directory := range []string{filepath.Dir(finalPath), s.root.stagingDir()} {
		if err := contextError(ctx); err != nil {
			return CompressedLogFile{}, err
		}
		if err := infrafsstore.SyncDirectory(directory); err != nil {
			return CompressedLogFile{}, fmt.Errorf("sync published compressed log file: %w", err)
		}
	}
	return file, nil
}

// RemoveAbandonedStagingFilesは、process停止で公開前のまま残ったprivate staging
// fileを削除します。進行中publishとの競合を避けるため、shared coordinatorがこのrootの
// 最初のownerになるときだけ呼び出します。
func (s *Store) RemoveAbandonedStagingFiles(ctx context.Context) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer s.operationMu.Unlock()
	if err := s.EnsureLayout(ctx); err != nil {
		return 0, err
	}
	entries, err := infrafsstore.ReadDirectoryEntries(s.root.stagingDir())
	if err != nil {
		return 0, fmt.Errorf("list compressed log file staging directory: %w", err)
	}
	removed := 0
	var errs []error
	for _, entry := range entries {
		if err := contextError(ctx); err != nil {
			errs = append(errs, err)
			break
		}
		name := entry.Name()
		path := filepath.Join(s.root.stagingDir(), name)
		if len(name) <= len(stagingFilePrefix)+len(stagingFileSuffix) ||
			!strings.HasPrefix(name, stagingFilePrefix) ||
			!strings.HasSuffix(name, stagingFileSuffix) {
			errs = append(errs, &UnexpectedStagingEntryError{Path: path})
			continue
		}
		info, statErr := os.Lstat(path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			errs = append(errs, fmt.Errorf("inspect abandoned staging file %q: %w", path, statErr))
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			errs = append(errs, fmt.Errorf("abandoned staging path %q is not a regular file", path))
			continue
		}
		if removeErr := os.Remove(path); removeErr != nil {
			errs = append(errs, fmt.Errorf("remove abandoned staging file %q: %w", path, removeErr))
			continue
		}
		removed++
	}
	if removed > 0 {
		if syncErr := infrafsstore.SyncDirectory(s.root.stagingDir()); syncErr != nil {
			errs = append(errs, fmt.Errorf("sync abandoned staging file removal: %w", syncErr))
		}
	}
	return removed, errors.Join(errs...)
}

// refreshReusedFileModificationTimeは、同内容の既存ファイルを今回の退避先として再利用するため、
// 更新時刻を現在時刻へ更新し、ファイルと親ディレクトリを同期します。
// 容量purgeに対する保存直後の保護期間の起点を更新し、呼び出し元が最後の元ファイルを
// 削除する間に、古いdead・quarantineのファイルが容量purgeで消されることを防ぎます。
func (s *Store) refreshReusedFileModificationTime(ctx context.Context, existing CompressedLogFile) error {
	path, err := s.refreshCompressedLogFileModificationTime(ctx, existing)
	if err != nil {
		return fmt.Errorf("refresh reused compressed log file modification time: %w", err)
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := infrafsstore.SyncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync existing compressed log file state directory: %w", err)
	}
	return nil
}

// refreshCompressedLogFileModificationTimeは、圧縮内容を変更せず、再送順序や保持期限の
// 基準となる更新時刻を現在時刻へ更新します。開いたファイルを同期し、後続のstate間の
// 移動に先立って、更新したファイル情報を永続化します。成功時は検証したパスを返します。
func (s *Store) refreshCompressedLogFileModificationTime(
	ctx context.Context,
	compressedLogFile CompressedLogFile,
) (string, error) {
	path, err := s.validateCompressedLogFile(compressedLogFile)
	if err != nil {
		return "", err
	}
	// WindowsのFile.Syncは書き込み権限を要求します。既存payloadは変更せず、
	// mtimeだけを同期するため、作成・切り詰めなしで開きます。
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return "", fmt.Errorf("open compressed log file for modification time refresh: %w", err)
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return "", fmt.Errorf("inspect compressed log file for modification time refresh: %w", err)
	}
	if !openedInfo.Mode().IsRegular() {
		_ = file.Close()
		return "", fmt.Errorf("%w: modification time refresh target %q is not a regular file", ErrInvalidCompressedLogPath, path)
	}
	if runtime.GOOS == "windows" {
		if err := infrafsstore.SetPrivateFilePermissions(file, payloadMode); err != nil {
			_ = file.Close()
			return "", fmt.Errorf("set compressed log file permissions before modification time refresh: %w", err)
		}
	}

	if err := contextError(ctx); err != nil {
		return "", errors.Join(err, file.Close())
	}
	now := time.Now().UTC()
	if err := os.Chtimes(path, now, now); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("refresh compressed log file modification time: %w", err)
	}
	currentInfo, err := os.Lstat(path)
	if err != nil {
		_ = file.Close()
		return "", fmt.Errorf("inspect compressed log file after modification time refresh: %w", err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 ||
		!currentInfo.Mode().IsRegular() ||
		!os.SameFile(openedInfo, currentInfo) {
		_ = file.Close()
		return "", fmt.Errorf("%w: compressed log file %q changed while refreshing its modification time", ErrInvalidCompressedLogPath, path)
	}
	if err := contextError(ctx); err != nil {
		return "", errors.Join(err, file.Close())
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return "", fmt.Errorf("%w: sync compressed log file modification time: %w", pkgfsstore.ErrDurabilityUnconfirmed, errors.Join(syncErr, closeErr))
	}
	if closeErr != nil {
		return "", fmt.Errorf("close compressed log file after modification time sync: %w", closeErr)
	}
	return path, nil
}

// ClaimOldestActiveはactive直下の最古fileを一件だけprocessingへ移し、workerの
// 所有権を取得します。移動後のdirectory同期に失敗した場合も、processingのfileと
// claimed=trueを返します。errorがある場合は送信せず、次回の回復処理へ残してください。
func (s *Store) ClaimOldestActive(ctx context.Context) (CompressedLogFile, bool, error) {
	if s == nil {
		return CompressedLogFile{}, false, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, false, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return CompressedLogFile{}, false, err
	}
	defer s.operationMu.Unlock()
	if err := s.EnsureLayout(ctx); err != nil {
		return CompressedLogFile{}, false, err
	}
	candidates, err := s.listCompressedLogFileCandidatesLocked(ctx, pkgspool.StateActive)
	if err != nil {
		return CompressedLogFile{}, false, err
	}
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			return CompressedLogFile{}, false, err
		}
		fromPath, err := s.root.compressedLogFilePath(pkgspool.StateActive, candidate.fileName)
		if err != nil {
			return CompressedLogFile{}, false, err
		}
		if err := ensureDirectChildRegularFile(s.root.ActiveDir(), fromPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return CompressedLogFile{}, false, fmt.Errorf(
				"inspect active compressed log file %q: %w",
				candidate.fileName,
				err,
			)
		}
		toPath, err := s.root.compressedLogFilePath(pkgspool.StateProcessing, candidate.fileName)
		if err != nil {
			return CompressedLogFile{}, false, err
		}
		if err := infrafsstore.RenameFileNoReplace(fromPath, toPath); err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrExist) {
				continue
			}
			return CompressedLogFile{}, false, fmt.Errorf("claim active compressed log file %q: %w", candidate.fileName, err)
		}
		claimed := CompressedLogFile{
			DestinationID: s.root.DestinationID(),
			FileName:      candidate.fileName,
			State:         pkgspool.StateProcessing,
			Path:          toPath,
		}
		if err := syncStateMove(s.root.ActiveDir(), s.root.ProcessingDir()); err != nil {
			return claimed, true, fmt.Errorf("sync claimed compressed log file %q: %w", candidate.fileName, err)
		}
		return claimed, true, nil
	}
	return CompressedLogFile{}, false, nil
}

// ListFilesは指定state直下の正規fileを古い順に返します。不正名の通常ファイルや
// 非通常ファイルのエントリは.preservedへ隔離して列挙を続け、移動結果は
// DestinationStores.TakePreservationWarningsで通知します。隔離できなければエラーを返します。
func (s *Store) ListFiles(
	ctx context.Context,
	state pkgspool.State,
) ([]CompressedLogFile, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.operationMu.Unlock()
	stateDirectory, err := s.root.StateDir(state)
	if err != nil {
		return nil, err
	}
	if err := s.EnsureLayout(ctx); err != nil {
		return nil, err
	}
	candidates, err := s.listCompressedLogFileCandidatesLocked(ctx, state)
	if err != nil {
		return nil, err
	}
	files := make([]CompressedLogFile, 0, len(candidates))
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		path, err := s.root.compressedLogFilePath(state, candidate.fileName)
		if err != nil {
			return nil, err
		}
		if err := ensureDirectChildRegularFile(stateDirectory, path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf(
				"inspect %s compressed log file %q: %w",
				state,
				candidate.fileName,
				err,
			)
		}
		files = append(files, CompressedLogFile{
			DestinationID: s.root.DestinationID(),
			FileName:      candidate.fileName,
			State:         state,
			Path:          path,
		})
	}
	return files, nil
}

// RecoverProcessingはprocess停止時にprocessingへ残ったfileをactiveへ戻します。
// 回収時刻をmtimeとして先に永続化するため、以前からactiveに待機しているfileを
// 再起動直後の回収fileが追い越しません。mtime更新またはrenameに失敗したfileは
// processingに残ります。rename後のdirectory同期に失敗したfileはactiveに移動済みで、
// recovered件数にも含まれた状態でerrorを返します。
func (s *Store) RecoverProcessing(ctx context.Context) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer s.operationMu.Unlock()
	if err := s.EnsureLayout(ctx); err != nil {
		return 0, err
	}
	candidates, err := s.listCompressedLogFileCandidatesLocked(ctx, pkgspool.StateProcessing)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			return recovered, err
		}
		fromPath, err := s.root.compressedLogFilePath(pkgspool.StateProcessing, candidate.fileName)
		if err != nil {
			return recovered, err
		}
		processingFile := CompressedLogFile{
			DestinationID: s.root.DestinationID(),
			FileName:      candidate.fileName,
			State:         pkgspool.StateProcessing,
			Path:          fromPath,
		}
		if _, err := s.refreshCompressedLogFileModificationTime(ctx, processingFile); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return recovered, fmt.Errorf(
				"refresh recovered processing compressed log file %q: %w",
				candidate.fileName,
				err,
			)
		}
		toPath, err := s.root.compressedLogFilePath(pkgspool.StateActive, candidate.fileName)
		if err != nil {
			return recovered, err
		}
		if err := infrafsstore.RenameFileNoReplace(fromPath, toPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return recovered, fmt.Errorf("recover processing compressed log file %q: %w", candidate.fileName, err)
		}
		recovered++
		if err := syncStateMove(s.root.ProcessingDir(), s.root.ActiveDir()); err != nil {
			return recovered, fmt.Errorf("sync recovered compressed log file %q: %w", candidate.fileName, err)
		}
	}
	return recovered, nil
}

// RequeueProcessingは再試行するprocessing fileの圧縮内容を変更せず、現在時刻を
// queue順序用のmtimeとして永続化してからactiveへ戻します。mtime更新・file同期・
// renameのいずれかに失敗した場合、fileはprocessingに残ります。rename後の
// directory同期に失敗した場合だけ、返却fileはactiveを示し、errorも返します。
func (s *Store) RequeueProcessing(
	ctx context.Context,
	file CompressedLogFile,
) (CompressedLogFile, error) {
	if s == nil {
		return CompressedLogFile{}, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if file.State != pkgspool.StateProcessing {
		return CompressedLogFile{}, fmt.Errorf(
			"%w: source state is %q, want processing",
			ErrInvalidStateTransition,
			file.State,
		)
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	defer s.operationMu.Unlock()
	fromPath, err := s.refreshCompressedLogFileModificationTime(ctx, file)
	if err != nil {
		return CompressedLogFile{}, fmt.Errorf("refresh requeued compressed log file: %w", err)
	}
	toPath, err := s.root.compressedLogFilePath(pkgspool.StateActive, file.FileName)
	if err != nil {
		return CompressedLogFile{}, err
	}
	if err := infrafsstore.RenameFileNoReplace(fromPath, toPath); err != nil {
		return CompressedLogFile{}, fmt.Errorf("requeue processing compressed log file: %w", err)
	}
	requeued := CompressedLogFile{
		DestinationID: s.root.DestinationID(),
		FileName:      file.FileName,
		State:         pkgspool.StateActive,
		Path:          toPath,
	}
	if err := syncStateMove(s.root.ProcessingDir(), s.root.ActiveDir()); err != nil {
		return requeued, fmt.Errorf("sync requeued compressed log file: %w", err)
	}
	return requeued, nil
}

// TransitionProcessingはworkerが所有するprocessing fileを次stateへ移します。
// 移動前に失敗した場合は空のfileを返します。移動後のdirectory同期に失敗した場合は、
// 移動先のfileとerrorを返します。返却fileのStateは同期の完了を保証しません。
func (s *Store) TransitionProcessing(
	ctx context.Context,
	file CompressedLogFile,
	toState pkgspool.State,
) (CompressedLogFile, error) {
	if s == nil {
		return CompressedLogFile{}, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if file.State != pkgspool.StateProcessing {
		return CompressedLogFile{}, fmt.Errorf(
			"%w: source state is %q, want processing",
			ErrInvalidStateTransition,
			file.State,
		)
	}
	switch toState {
	case pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine:
	default:
		return CompressedLogFile{}, fmt.Errorf("%w: processing to %q", ErrInvalidStateTransition, toState)
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return CompressedLogFile{}, err
	}
	defer s.operationMu.Unlock()
	fromPath, err := s.validateCompressedLogFile(file)
	if err != nil {
		return CompressedLogFile{}, err
	}
	toPath, err := s.root.compressedLogFilePath(toState, file.FileName)
	if err != nil {
		return CompressedLogFile{}, err
	}
	if err := infrafsstore.RenameFileNoReplace(fromPath, toPath); err != nil {
		return CompressedLogFile{}, fmt.Errorf("transition processing compressed log file to %q: %w", toState, err)
	}
	transitioned := CompressedLogFile{DestinationID: s.root.DestinationID(), FileName: file.FileName, State: toState, Path: toPath}
	toDirectory, _ := s.root.StateDir(toState)
	if err := syncStateMove(s.root.ProcessingDir(), toDirectory); err != nil {
		return transitioned, fmt.Errorf("sync compressed log file transition to %q: %w", toState, err)
	}
	return transitioned, nil
}

// RemoveTerminalはretentionが選んだdone/dead/quarantine fileだけを削除します。
func (s *Store) RemoveTerminal(ctx context.Context, file CompressedLogFile) error {
	if s == nil {
		return fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	switch file.State {
	case pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine:
	default:
		return fmt.Errorf("%w: state %q is not removable", ErrInvalidStateTransition, file.State)
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.operationMu.Unlock()
	path, err := s.validateCompressedLogFile(file)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove terminal compressed log file %q: %w", file.FileName, err)
	}
	stateDirectory, _ := s.root.StateDir(file.State)
	if err := infrafsstore.SyncDirectory(stateDirectory); err != nil {
		return fmt.Errorf("sync terminal compressed log file removal: %w", err)
	}
	return nil
}

func (s *Store) validateCompressedLogFile(file CompressedLogFile) (string, error) {
	if file.DestinationID != s.root.DestinationID() {
		return "", fmt.Errorf("%w: file belongs to another destination", ErrInvalidCompressedLogPath)
	}
	if !file.State.IsValid() {
		return "", fmt.Errorf("%w: invalid state %q", ErrInvalidCompressedLogPath, file.State)
	}
	expectedPath, err := s.root.compressedLogFilePath(file.State, file.FileName)
	if err != nil {
		return "", err
	}
	if filepath.Clean(file.Path) != expectedPath {
		return "", fmt.Errorf(
			"%w: compressed log file path %q does not match %q",
			ErrInvalidCompressedLogPath,
			file.Path,
			expectedPath,
		)
	}
	stateDirectory, _ := s.root.StateDir(file.State)
	if err := ensureDirectChildRegularFile(stateDirectory, expectedPath); err != nil {
		return "", err
	}
	return expectedPath, nil
}

// findPublishedFilesは、同名の既存ファイルを調べ、不正なエントリは先に保全します。
// 呼び出し元はoperationMuを保持します。保全済みの移動結果は後続の保存成否にかかわらず返します。
func (s *Store) findPublishedFiles(ctx context.Context, fileName string, conflictRenames *[]infrafsstore.ConflictRename) ([]CompressedLogFile, error) {
	states := [...]pkgspool.State{
		pkgspool.StateActive,
		pkgspool.StateProcessing,
		pkgspool.StateDone,
		pkgspool.StateDead,
		pkgspool.StateQuarantine,
	}
	var found []CompressedLogFile
	for _, state := range states {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		path, err := s.root.compressedLogFilePath(state, fileName)
		if err != nil {
			return nil, err
		}
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect existing compressed log file %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			reason := fmt.Errorf("%w: %q is not a regular file: mode=%s", ErrInvalidCompressedLogPath, path, info.Mode())
			rename, err := s.preserveInvalidEntryLocked(ctx, state, fileName, info, reason)
			if rename.RetainedPath != "" {
				*conflictRenames = append(*conflictRenames, rename)
			}
			if err != nil {
				return nil, fmt.Errorf("preserve invalid existing spool entry %q: %w", path, err)
			}
			continue
		}
		found = append(found, CompressedLogFile{DestinationID: s.root.DestinationID(), FileName: fileName, State: state, Path: path})
	}
	return found, nil
}

func validateRegularSource(sourcePath string) error {
	info, err := os.Lstat(sourcePath)
	if err != nil {
		return fmt.Errorf("inspect compressed log file source: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: source %q is not a regular file", ErrInvalidCompressedLogPath, sourcePath)
	}
	return nil
}

func copySourceToFile(ctx context.Context, sourcePath string, destination *os.File) error {
	sourceInfo, err := os.Lstat(sourcePath)
	if err != nil {
		return fmt.Errorf("inspect compressed log file source: %w", err)
	}
	if sourceInfo.Mode()&os.ModeSymlink != 0 || !sourceInfo.Mode().IsRegular() {
		return fmt.Errorf("%w: source %q is not a regular file", ErrInvalidCompressedLogPath, sourcePath)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open compressed log file source: %w", err)
	}
	defer source.Close()
	openedInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("stat opened compressed log file source: %w", err)
	}
	if !os.SameFile(sourceInfo, openedInfo) {
		return errors.New("compressed log file source changed while opening")
	}
	written, err := copyWithContext(ctx, destination, source)
	if err != nil {
		return fmt.Errorf("copy compressed log file source: %w", err)
	}
	afterInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("stat compressed log file source after copy: %w", err)
	}
	if written != sourceInfo.Size() ||
		afterInfo.Size() != sourceInfo.Size() ||
		!afterInfo.ModTime().Equal(sourceInfo.ModTime()) {
		return errors.New("compressed log file source changed while copying")
	}
	return nil
}

func filesHaveEqualContent(ctx context.Context, firstPath string, secondPath string) (bool, error) {
	firstDigest, firstSize, err := digestRegularFile(ctx, firstPath)
	if err != nil {
		return false, err
	}
	secondDigest, secondSize, err := digestRegularFile(ctx, secondPath)
	if err != nil {
		return false, err
	}
	return firstSize == secondSize && firstDigest == secondDigest, nil
}

func digestRegularFile(ctx context.Context, path string) ([sha256.Size]byte, int64, error) {
	var empty [sha256.Size]byte
	info, err := os.Lstat(path)
	if err != nil {
		return empty, 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return empty, 0, fmt.Errorf("%w: %q is not a regular file", ErrInvalidCompressedLogPath, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return empty, 0, err
	}
	hash := sha256.New()
	written, copyErr := copyWithContext(ctx, hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return empty, 0, errors.Join(copyErr, closeErr)
	}
	if written != info.Size() {
		return empty, 0, errors.New("compressed log file changed while hashing")
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, written, nil
}

func copyWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 128*1024)
	var total int64
	for {
		if err := contextError(ctx); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			if err := contextError(ctx); err != nil {
				return total, err
			}
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, contextError(ctx)
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

type compressedLogFileCandidate struct {
	fileName string
	modTime  time.Time
}

// listCompressedLogFileCandidatesLockedは、不正名や非通常ファイルのエントリを隔離してから
// 再送・purgeの候補を列挙します。呼び出し元はoperationMuを保持してください。
func (s *Store) listCompressedLogFileCandidatesLocked(ctx context.Context, state pkgspool.State) ([]compressedLogFileCandidate, error) {
	// 指定stateのディレクトリ直下だけを列挙します。子ディレクトリの中は走査しません。
	directory, err := s.root.StateDir(state)
	if err != nil {
		return nil, err
	}
	entries, err := infrafsstore.ReadDirectoryEntries(directory)
	if err != nil {
		return nil, fmt.Errorf("list compressed log files: %w", err)
	}
	candidates := make([]compressedLogFileCandidate, 0, len(entries))
	var invalidEntries []error
	// 名前ごとにfilesystemへ問い合わせないよう、state directoryの規則を一度だけ判定します。
	nameCase := pathidentity.DirectoryNameCase(directory)
	for _, entry := range entries {
		// 中止要求があれば走査を止め、それまでの保全失敗も併せて返します。
		if err := contextError(ctx); err != nil {
			return nil, errors.Join(append(invalidEntries, err)...)
		}
		path := filepath.Join(directory, entry.Name())
		// リンク先を辿らず、エントリ自体の種類と更新時刻を取得します。
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			// 一覧取得後に消えたエントリだけは、通知せずスキップします。
			// 残っているエントリは、以下で候補か保全対象かを判定します。
			continue
		}
		if err != nil {
			// その他の検査エラーでは、エントリの種類を確認できないため走査を中止します。
			return nil, fmt.Errorf("inspect compressed log file %q: %w", entry.Name(), err)
		}
		// 通常ファイルかつ圧縮ログの命名規則に合うものだけを候補にします。
		// 圧縮内容の検証はここでは行いません。
		var invalidReason error
		if !info.Mode().IsRegular() {
			// ディレクトリやシンボリックリンクなどは、名前に関係なく保全対象とします。
			invalidReason = fmt.Errorf(
				"%w: state entry %q is not a regular file: mode=%s",
				ErrInvalidCompressedLogPath,
				path,
				info.Mode(),
			)
		} else {
			// 通常ファイルの場合だけ、名前が圧縮ログの命名規則に合うか検査します。
			// 名前が正しければnil、不正なら保全理由となるエラーを受け取ります。
			invalidReason = validateCompressedLogFileNameForNameCase(entry.Name(), nameCase)
		}
		if invalidReason != nil {
			// 不正なエントリは.preservedへ移し、候補には含めません。
			// 保全に失敗しても残りを検査し、失敗は走査終了後にまとめて返します。
			if _, err := s.preserveInvalidEntryLocked(ctx, state, entry.Name(), info, invalidReason); err != nil {
				invalidEntries = append(invalidEntries, fmt.Errorf("preserve invalid state entry %q: %w", path, err))
			}
			continue
		}
		candidates = append(candidates, compressedLogFileCandidate{
			fileName: entry.Name(),
			modTime:  info.ModTime(),
		})
	}
	// 保全に失敗した場合は、部分的な候補一覧を返さず、集約したエラーを返します。
	if err := errors.Join(invalidEntries...); err != nil {
		return nil, fmt.Errorf("inspect compressed log file state directory %q: %w", directory, err)
	}
	// 更新時刻が古い順に並べ、同時刻ならファイル名順にします。
	// spoolの待機順であり、ログファイル名の日付・世代番号順ではありません。
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].fileName < candidates[j].fileName
		}
		return candidates[i].modTime.Before(candidates[j].modTime)
	})
	return candidates, nil
}

func syncStateMove(fromDirectory string, toDirectory string) error {
	if err := infrafsstore.SyncDirectory(toDirectory); err != nil {
		return err
	}
	if filepath.Clean(fromDirectory) == filepath.Clean(toDirectory) {
		return nil
	}
	return infrafsstore.SyncDirectory(fromDirectory)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
