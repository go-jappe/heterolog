package compressedlogarchive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"

	fsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

const (
	directoryMode    = 0o750
	archivedFileMode = 0o640
)

// Storeは圧縮log fileを分類directory直下へ一fileのまま保存します。Storeはsourceを
// 削除せず、Save成功後にsourceの所有権を解放するかは呼び出し元が決定します。
type Store struct {
	root Root
	// operationMuは同じStore instanceのSave同士を直列化し、同名fileの探索から
	// 公開までにretryable/permanentの両方へ二重生成させません。purgeとも排他します。
	// 複数ownerを合成したhandoff全体は、上位coordinatorがroot単位で管理します。
	operationMu fsstore.ContextMutex
	// directorySetupMuは初期化処理とdirectoriesReadyを保護します。EnsureDirectoriesは
	// operationMu取得中にも呼ぶため、初期化用のロックを分けます。
	directorySetupMu fsstore.ContextMutex
	// directoriesReadyは全保存先の初期化に一度成功したことを表します。
	// 初期化に失敗した場合はfalseのままにし、次の呼び出しで再試行します。
	directoriesReady bool
}

// SaveInputは単純保存する圧縮log fileとupload失敗の集約分類です。宛先別の
// 再送状態を持たないfailure archiveなので、routeやdestination metadataは受け取りません。
type SaveInput struct {
	SourcePath      string
	FailureCategory uploader.FailureCategory
}

// ArchivedFileはfailure category directoryへ公開済みの圧縮log fileを表します。
type ArchivedFile struct {
	FailureCategory uploader.FailureCategory
	Path            string
	// ConflictRenamesは同名・別内容の旧ファイルを退避した結果です。保存が後で
	// 失敗しても返し、呼び出し側が旧名と退避先を通知できるようにします。
	ConflictRenames []fsstore.ConflictRename
}

// NewStoreはForDestinationで宛先を選んだRootだけを受け付けます。
func NewStore(root Root) (*Store, error) {
	if err := root.validate(); err != nil {
		return nil, err
	}
	if root.DestinationID() == "" {
		return nil, fmt.Errorf("%w: archive destination is not selected", ErrInvalidRoot)
	}
	return &Store{root: root}, nil
}

func (s *Store) Root() Root {
	return s.root
}

// EnsureDirectoriesは保存先のretryable、permanent directoryを初期化します。
// lock待ちと各directoryの作成前はctxでキャンセルできます。
// 初期化時はsymlinkを拒否し、新規directoryの親を同期します。失敗はerrorとして返し、
// 次の呼び出しで未作成のdirectoryの初期化を再試行します。同期できなかった親は記録し、
// 別のStoreからの呼び出しも含め、その親の同期を確認できるまでは既存のdirectoryでも
// 初期化を失敗として返します。
// 一度成功したStoreでは、以後の呼び出しはfilesystemを調べずに戻ります。
// 初期化後の外部操作によるdirectoryの変更は検査・修復せず、保存・列挙などの
// 実際の操作で失敗した場合に、その操作のerrorとして返します。
func (s *Store) EnsureDirectories(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.directorySetupMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.directorySetupMu.Unlock()
	if s.directoriesReady {
		return nil
	}
	directories := []string{
		s.root.RetryableDir(),
		s.root.PermanentDir(),
	}
	for _, directory := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fsstore.EnsureDirectoryNoSymlinksContext(ctx, directory, directoryMode); err != nil {
			return fmt.Errorf("create compressed log file archive directory %q: %w", directory, err)
		}
	}
	s.directoriesReady = true
	return nil
}

// Saveはsourceを保存先directory内の専用名の一時fileへcopyしてfsyncします。
// 読み戻した内容が元ファイルとbyte単位で一致したら、上書きせず完成名へ変更します。
// 圧縮形式には依存しません。一時fileは完成済みfileの走査・purgeから除外します。
// 同じ元file名で同じbytesが既に保存済みなら最初の分類を正本として
// 再利用します。同名でbytesが異なる既存fileはroot直下の.preservedへ退避し、結果の
// ConflictRenamesで通知します。退避に失敗した場合は元fileを残してerrorを返します。
// cancel時は次の処理へ進まず、自分が作った一時fileだけを掃除して返します。
// 保存済みfileは保持し、再試行時に再利用します。実行中のOS I/Oは強制中断しません。
func (s *Store) Save(ctx context.Context, input SaveInput) (saved ArchivedFile, err error) {
	if s == nil {
		return ArchivedFile{}, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return ArchivedFile{}, err
	}
	defer s.operationMu.Unlock()
	if !input.FailureCategory.IsValid() {
		return ArchivedFile{}, fmt.Errorf("%w: %q", ErrInvalidFailureCategory, input.FailureCategory)
	}
	if input.SourcePath == "" {
		return ArchivedFile{}, fmt.Errorf("%w: source path is empty", ErrInvalidCompressedLogPath)
	}
	sourceFileName := filepath.Base(input.SourcePath)
	// 保存先のarchive rootの大小文字の規則で、保存する名前を検証します。
	if _, err := compressedLogFileSuffix(s.root.Path(), sourceFileName); err != nil {
		return ArchivedFile{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArchivedFile{}, err
	}

	if err := s.EnsureDirectories(ctx); err != nil {
		return ArchivedFile{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArchivedFile{}, err
	}
	existing, found, renamed, err := s.findOrPreserveExistingArchivedFiles(ctx, input)
	saved.ConflictRenames = append(saved.ConflictRenames, renamed...)
	if err != nil {
		return saved, err
	} else if found {
		if err := s.reuseEquivalentArchivedFile(ctx, existing.Path); err != nil {
			return saved, err
		}
		existing.ConflictRenames = saved.ConflictRenames
		return existing, nil
	}

	finalPath, err := s.root.archivedFilePath(
		input.FailureCategory,
		sourceFileName,
	)
	if err != nil {
		return saved, err
	}
	temporaryPath, err := createTemporaryCompressedLogDurably(ctx, input.SourcePath, filepath.Dir(finalPath))
	if err != nil {
		return saved, err
	}
	defer func() {
		err = errors.Join(err, removeTemporaryCompressedLog(temporaryPath))
	}()

	if err := ctx.Err(); err != nil {
		return saved, err
	}
	// 別goroutine/processが同じfileを先に公開した場合も、既存fileを置換しません。
	if renameErr := fsstore.RenameFileNoReplace(temporaryPath, finalPath); renameErr != nil {
		if !errors.Is(renameErr, fs.ErrExist) {
			return saved, fmt.Errorf("save compressed log file archive: %w", renameErr)
		}
		existing, found, renamed, existingErr := s.findOrPreserveExistingArchivedFiles(ctx, input)
		saved.ConflictRenames = append(saved.ConflictRenames, renamed...)
		if existingErr != nil {
			return saved, existingErr
		} else if found {
			if syncErr := s.reuseEquivalentArchivedFile(ctx, existing.Path); syncErr != nil {
				return saved, syncErr
			}
			existing.ConflictRenames = saved.ConflictRenames
			return existing, nil
		}
		if len(renamed) == 0 {
			return saved, fmt.Errorf("save compressed log file archive: %w", renameErr)
		}
		if err := fsstore.RenameFileNoReplace(temporaryPath, finalPath); err != nil {
			return saved, fmt.Errorf("save compressed log file archive after preserving name conflict: %w", err)
		}
	}
	// 同じdirectory内で名前を変更したので、保存先directoryだけを同期します。
	saved.FailureCategory = input.FailureCategory
	saved.Path = finalPath
	if err := s.syncArchivedFileDirectory(ctx, saved.Path); err != nil {
		return saved, err
	}
	return saved, nil
}

// findOrPreserveExistingArchivedFilesは両分類の同名ファイルを調べます。内容が
// 異なる既存ファイルだけをroot直下の.preservedへ退避し、同じ内容のものがあれば再利用します。
// 退避済みの結果は後続処理が失敗した場合も返します。
func (s *Store) findOrPreserveExistingArchivedFiles(
	ctx context.Context,
	input SaveInput,
) (foundFile ArchivedFile, found bool, renamed []fsstore.ConflictRename, err error) {
	sourceName := filepath.Base(input.SourcePath)
	for _, failureCategory := range archiveFailureCategories {
		path, err := s.root.archivedFilePath(failureCategory, sourceName)
		if err != nil {
			return ArchivedFile{}, false, renamed, err
		}
		_, err = os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return ArchivedFile{}, false, renamed, fmt.Errorf("inspect existing compressed log file %q: %w", path, err)
		}
		failureCategoryDir, _ := s.root.FailureCategoryDir(failureCategory)
		if err := ensureDirectChildRegularFile(failureCategoryDir, path); err != nil {
			return ArchivedFile{}, false, renamed, err
		}
		equal, err := fsstore.EqualRegularFileContents(ctx, input.SourcePath, path)
		if err != nil {
			return ArchivedFile{}, false, renamed, fmt.Errorf("compare existing compressed log file %q: %w", path, err)
		}
		if equal {
			if !found {
				foundFile = ArchivedFile{FailureCategory: failureCategory, Path: path}
				found = true
			}
			continue
		}
		conflict, err := preserveConflictingArchiveFile(
			ctx,
			s.root.Path(),
			failureCategory,
			path,
			fsstore.RenameFileNoReplace,
		)
		if conflict.RetainedPath != "" {
			renamed = append(renamed, conflict)
		}
		if err != nil {
			return ArchivedFile{}, false, renamed, err
		}
	}
	return foundFile, found, renamed, nil
}

// reuseEquivalentArchivedFileは、同名・同bytesの既存archiveを今回のdurable
// handoffとして再採用します。Saveが保持するoperationMuはPurger.Runも取得するため、
// 既存ファイルの確認から同期完了までpurgeと競合しません。
func (s *Store) reuseEquivalentArchivedFile(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	archived, archivedInfo, err := fsstore.OpenRegularFile(path, os.O_RDWR)
	if err != nil {
		return fmt.Errorf("open equivalent compressed log file archive: %w", err)
	}
	if runtime.GOOS == "windows" {
		if err := fsstore.SetPrivateFilePermissions(archived, archivedFileMode); err != nil {
			_ = archived.Close()
			return fmt.Errorf("set equivalent compressed log file archive permissions: %w", err)
		}
	}

	// 再保存した時刻を更新日時に反映します。保持期限と削除順はファイル名で決まり、
	// この更新によって保持期限が延びたり、削除順が変わったりすることはありません。
	if err := ctx.Err(); err != nil {
		_ = archived.Close()
		return err
	}
	now := time.Now().UTC()
	if err := os.Chtimes(path, now, now); err != nil {
		_ = archived.Close()
		return fmt.Errorf("refresh equivalent compressed log file archive retention time: %w", err)
	}
	refreshedInfo, err := os.Lstat(path)
	if err != nil {
		_ = archived.Close()
		return fmt.Errorf("inspect refreshed equivalent compressed log file archive: %w", err)
	}
	if refreshedInfo.Mode()&os.ModeSymlink != 0 ||
		!refreshedInfo.Mode().IsRegular() ||
		!os.SameFile(archivedInfo, refreshedInfo) {
		_ = archived.Close()
		return errors.New("equivalent compressed log file archive changed while refreshing retention time")
	}
	// Chtimesで変更したinode metadataを、handoff成功を返す前に永続化します。
	if err := ctx.Err(); err != nil {
		_ = archived.Close()
		return err
	}
	syncErr := archived.Sync()
	closeErr := archived.Close()
	if syncErr != nil {
		return fmt.Errorf("%w: sync equivalent compressed log file archive metadata: %w", pkgfsstore.ErrDurabilityUnconfirmed, errors.Join(syncErr, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("close equivalent compressed log file archive: %w", closeErr)
	}
	return s.syncArchivedFileDirectory(ctx, path)
}

func (s *Store) syncArchivedFileDirectory(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fsstore.SyncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync compressed log file archive directory: %w", err)
	}
	return nil
}

func createTemporaryCompressedLogDurably(
	ctx context.Context,
	sourcePath string,
	directory string,
) (string, error) {
	destination, err := os.CreateTemp(directory, temporaryFilePattern)
	if err != nil {
		return "", fmt.Errorf("create compressed log file archive temporary file: %w", err)
	}
	temporaryPath := destination.Name()
	writeErr := writeTemporaryCompressedLog(ctx, sourcePath, destination)
	closeErr := destination.Close()
	if closeErr != nil {
		writeErr = errors.Join(writeErr, fmt.Errorf("close temporary compressed log file archive: %w", closeErr))
	}
	if writeErr == nil {
		// 元ファイルが残っているうちに照合します。不一致・読取失敗の場合は公開せず、
		// 今回の一時fileだけを片付け、呼び出し元へ保存失敗を返します。
		equal, err := fsstore.EqualRegularFileContents(ctx, sourcePath, temporaryPath)
		if err != nil {
			writeErr = fmt.Errorf("verify temporary compressed log file archive copy: %w", err)
		} else if !equal {
			writeErr = fmt.Errorf("temporary compressed log file archive %w", fsstore.ErrCopyDiffersFromSource)
		}
	}
	if writeErr != nil {
		return "", errors.Join(writeErr, removeTemporaryCompressedLog(temporaryPath))
	}
	return temporaryPath, nil
}

// cleanupはcancel後も行い、今回作った非公開file以外は削除しません。
func removeTemporaryCompressedLog(temporaryPath string) error {
	if err := os.Remove(temporaryPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove compressed log file archive temporary file: %w", err)
	}
	if err := fsstore.SyncDirectory(filepath.Dir(temporaryPath)); err != nil {
		return fmt.Errorf("sync compressed log file archive temporary cleanup: %w", err)
	}
	return nil
}

func writeTemporaryCompressedLog(ctx context.Context, sourcePath string, destination *os.File) error {
	sourceInfo, err := fsstore.InspectRegularFile(sourcePath)
	if err != nil {
		return fmt.Errorf("inspect compressed log file source: %w", err)
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
	if err := fsstore.SetPrivateFilePermissions(destination, archivedFileMode); err != nil {
		return fmt.Errorf("set compressed log file archive permissions: %w", err)
	}
	written, err := copyWithContext(ctx, destination, source)
	if err != nil {
		return fmt.Errorf("copy compressed log file archive: %w", err)
	}
	if written != sourceInfo.Size() {
		return fmt.Errorf(
			"compressed log file source size changed while copying: copied %d, expected %d",
			written,
			sourceInfo.Size(),
		)
	}
	afterInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("stat compressed log file source after copy: %w", err)
	}
	if afterInfo.Size() != sourceInfo.Size() || !afterInfo.ModTime().Equal(sourceInfo.ModTime()) {
		return errors.New("compressed log file source changed while copying")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := destination.Sync(); err != nil {
		return fmt.Errorf("%w: sync temporary compressed log file archive: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	return nil
}

func copyWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 128*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			if err := ctx.Err(); err != nil {
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
			return total, ctx.Err()
		}
		if readErr != nil {
			return total, readErr
		}
	}
}
