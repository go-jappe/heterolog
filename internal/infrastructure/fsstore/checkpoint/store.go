package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const (
	fileMode               = 0o600
	documentFileSuffix     = ".log-file-uploads.json"
	registrationFileSuffix = ".destinations.json"
)

var (
	ErrInvalidDirectory              = errors.New("log file upload checkpoint directory is invalid")
	ErrCorruptCheckpoint             = errors.New("log file upload checkpoint is corrupt")
	ErrRegisteredCheckpointMissing   = errors.New("initialized log file upload checkpoint is missing")
	ErrRegisteredDestinationsMissing = errors.New("log file upload destination registrations are missing")
	// ErrUnreadableCheckpointは、存在するcheckpoint文書を読み取れなかったことを表します。
	// 内容の破損とは区別し、原因のfilesystem errorもwrapします。
	ErrUnreadableCheckpoint = errors.New("read log file upload checkpoint")
	// ErrCheckpointSourceMismatchは、checkpoint文書・宛先登録が別のsource IDのものであることを表します。
	// ErrCorruptCheckpointと併せて返します。
	ErrCheckpointSourceMismatch = errors.New("log file upload checkpoint source does not match")
)

// FilesystemStoreOptionはcheckpointの初期化・保存再試行・通知方法を指定します。
type FilesystemStoreOption func(*filesystemStoreOptions)

type filesystemStoreOptions struct {
	recoverMissing   bool
	onError          func(error)
	disableSaveRetry bool
}

// WithMissingCheckpointRecoveryは初期化済みsourceのcheckpoint欠落時の動作を指定します。
// true（既定値）なら完了記録を空にして再作成し、残存ファイルを再送対象にします。
// falseなら欠落エラーを返します。破損や読み取りエラーは設定によらずエラーを返します。
func WithMissingCheckpointRecovery(enabled bool) FilesystemStoreOption {
	return func(options *filesystemStoreOptions) { options.recoverMissing = enabled }
}

// WithOnErrorは保存再試行のエラー通知先を構築時に指定します。nilなら通知しません。
// callbackは再試行goroutineからStoreのlock外で呼ばれます。
// 通知先がStoreや所有者のCloseを呼べるよう、callbackは別goroutineへ通知を登録してください。
// 同期実行すると保存ループ自身の終了待ちになることがあります。
// 初回の更新操作や構築時のエラーは戻り値で返し、このcallbackには通知しません。
func WithOnError(onError func(error)) FilesystemStoreOption {
	return func(options *filesystemStoreOptions) { options.onError = onError }
}

// WithSaveRetryDisabledは、停止中のアプリに対する手動復旧など、一回だけ実行する
// 所有者向けに保存失敗後の自動再試行を無効にします。更新済みのメモリは維持し、
// 操作とCloseは未保存のエラーを返します。通常のloggerでは指定しません。
// このmodeで保存が失敗したStoreは、I/O復旧後も保存を再試行しません。Closeして
// 再構築し、残存する登録・cursor文書を読み直してから手動処理を再実行してください。
func WithSaveRetryDisabled() FilesystemStoreOption {
	return func(options *filesystemStoreOptions) { options.disableSaveRetry = true }
}

// Storeは一つのログ系列について、宛先ごとのuploadが不要になった最後の世代と、
// 未完了ファイルの送信・退避の途中経過を保存します。
// cursor以前の世代はすべてuploadが不要と判断するため、呼び出し元は未完了の古い世代を
// 飛び越えずにAdvanceCompletionCursorを呼びます。宛先の登録前から存在したため対象外とした
// 範囲はcursorに別に保存し、Checkpoint.OutOfScopeDestinationIDsで送信・退避済みと区別します。
// RegisterDestinations・AdvanceCompletionCursorに渡す宛先ID一覧は上流で検証済みとし、再検証しません。
// 保存済みJSONの宛先IDは読み込み時に検証します。
// 宛先登録の公開後に同期が失敗した場合は、IDと空cursorを保持して保存を再試行します。
// 同期待ちの間、要求されたcursor/progress更新は受理せず通常errorで返します。
// 更新受理後の保存失敗はPendingSaveErrorです。最新のmemoryを使用して処理を継続でき、
// Storeごとに一つの復旧処理が最新stateの保存を再試行します。
type Store interface {
	// RegisterDestinationsは新宛先の初期cursorを保存します。登録済みIDは変更しません。
	// existingFileKeysは系列の全既存名です。最終世代に未圧縮ログがあれば対象に残します。
	// 初期cursor以前は対象外の範囲として記録し、送信・退避済みの世代とは区別します。
	RegisterDestinations(ctx context.Context, destinationIDs []objectstorage.DestinationID, existingFileKeys []CheckpointKey) error
	DestinationCursors(ctx context.Context) ([]DestinationCursor, error)
	// Close前に新規操作・利用側の再試行を止めてください。受理済みの最新stateが保存されるまで待ちます。
	Close() error
	// Loadは完了cursorからkeyのuploadが不要な宛先を導出します。
	// 稼働中はmemoryが正本で、ファイルの読み直しや保存は行いません。
	Load(ctx context.Context, key CheckpointKey) (Checkpoint, error)
	// LoadUploadProgressは未完了ファイルの送信・退避結果をメモリから読みます。
	// 記録がなければゼロ値とfound=falseを返します。完了cursorは進めません。
	// found=trueでUploadResultがUploadResultSuccessなら直接送信に成功しています。
	LoadUploadProgress(ctx context.Context, key CheckpointKey, destination objectstorage.DestinationID) (progress UploadProgress, found bool, err error)
	// SaveUploadProgressは送信結果と成功した退避先を追加保存します。
	// 呼び出し元は直接送信の結果を確定してから呼びます。未試行の値は保存しません。
	// 既存の成功を消さず、保存失敗中もメモリへ反映してPendingSaveErrorを返します。
	// 同じ退避先の保存結果が食い違う場合は既存の成功記録と未解決の差分を保存し、
	// SavedTargetConflictErrorを返します。呼び出し元は元データの保全完了後に、
	// 既存の保存結果と保全した完全一致の差分をPreservedTargetConflictsへ指定して
	// 再度保存します。差分の単なる省略では解消しません。
	SaveUploadProgress(ctx context.Context, progress UploadProgress) error
	// AdvanceCompletionCursorは指定宛先のメモリ上の完了cursorをkey.Generationまで進め、
	// 更新後のcursor以前にある同じ宛先のUploadProgressを削除し、最新stateをファイルへ保存します。
	// 未圧縮・圧縮ファイルは同じ世代のcursorを共有し、古い世代の通知ではcursorを戻しません。
	// destinationIDsには送信または全指定退避先への保存が完了した宛先を渡してください。
	// 呼び出し元は古い未完了世代を飛び越えず、元ファイルの削除より先に呼びます。
	// 未解決な保存結果の矛盾を越える更新は拒否します。
	// 更新受理後の保存失敗は更新済みstateとともにPendingSaveErrorを返します。
	// 保存失敗中の追加更新はメモリだけへ反映し、保存の再試行はStoreの構築時設定に従います。
	// ファイル削除後も完了cursorは保持します。
	AdvanceCompletionCursor(ctx context.Context, key CheckpointKey, destinationIDs []objectstorage.DestinationID) (Checkpoint, error)
}

// FilesystemStoreは宛先別の完了cursorと未完了ファイルの途中経過を一つのJSONへ保存します。
// 別の登録JSONには宛先IDだけを保持します。cursor欠落時は設定に従い、登録済み宛先を
// 空cursorへ戻して残存ログを再送するか、起動を失敗させます。
// 構築時に一度読み、稼働中はstateを正本とします。通常は更新直後に保存し、保存失敗中の
// 追加更新はmemoryだけに反映します。一つの復旧ループが最新state全体を保存します。
// cursorはファイル削除後も保持し、途中経過はその世代の完了時に削除します。
// 同じdirectory×sourceを複数Storeや複数processから同時更新することは対象外です。
type FilesystemStore struct {
	directory string

	// stateOrSaveMuはstate・保存エラーの読み書きとcheckpointファイルへの保存を
	// 排他します。通常の呼び出しと、Store内部の保存再試行で共有します。
	stateOrSaveMu infrafsstore.ContextMutex
	// stateは検証を通過した最新のcheckpoint内容です。保存に失敗しても古い内容には戻しません。
	state checkpointDocument
	// registrationsは一度登録した宛先IDの一覧です。cursor欠落後に新宛先と誤認しません。
	registrations destinationRegistrationDocument
	// registrationsPendingは公開済みmanifestのdirectory同期が未完了であることを表します。
	// cursor更新は受理せず、保存復旧はmanifestを先に同期してからstateを保存します。
	registrationsPending bool
	// syncDirectoryはこのStoreのatomic保存に使うI/Oです。stateOrSaveMu内で呼びます。
	syncDirectory func(string) error
	// lastSaveErrは最新stateが未保存である間の最後の保存エラーです。保存成功時に解除します。
	lastSaveErr error
	// saveRecoveryは最新stateの保存を再試行します。ループの起動・停止は共通部品に委ねます。
	saveRecovery *SaveRecovery
	// disableSaveRetryは手動実行の保存失敗を常駐再試行へ渡さないための構築時設定です。
	disableSaveRetry bool
}

type checkpointDocument struct {
	// Sourceは、同じ保存先・命名規則で日付や番号を変えて作られるログ群を識別するIDです。
	// アプリではArchiveCheckpointSourceIDで生成します。元ファイルのパスではありません。
	// Linuxでの例: logger_file_error-27e9badba2c077f575c30f13aaba7f6d864052db8db301645c4e2cf3586171c2
	// この例のハッシュ元は、保存先=/var/log/App、prefix=error_、target=App、元の拡張子=.logです。
	// 日付・番号は各Checkpoint.Keyで区別し、このIDはcheckpointの保存ファイル名にも使います。
	Source         string              `json:"source"`
	Cursors        []DestinationCursor `json:"cursors"`
	UploadProgress []UploadProgress    `json:"upload_progress,omitempty"`
}

var _ Store = (*FilesystemStore)(nil)

// NewFilesystemStoreは保存先の親の既存aliasを実体pathへ固定し、source identityを検証します。
// 最終checkpointディレクトリ自体のリンクやディレクトリ以外のpathは拒否します。
// 保存先は起動時に作成・権限設定・同期を完了しておく必要があります。
// このconstructorは未作成の保存先を拒否し、directoryの作成や権限変更は行いません。
// checkpointと宛先登録情報はここで読み込み・検証します。初回は空の宛先登録情報を同期してから
// 空documentを保存します。登録情報だけ残る場合は設定に従って空cursorから復旧します。
// cursorだけが残る場合は登録情報の欠落としてエラーを返します。
// 読み込み・検証・初期化の保存に失敗した場合はStoreを返しません。
func NewFilesystemStore(directory, sourceID string, options ...FilesystemStoreOption) (*FilesystemStore, error) {
	if strings.TrimSpace(directory) == "" {
		return nil, ErrInvalidDirectory
	}
	if err := ValidateSourceID(sourceID); err != nil {
		return nil, err
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve log file upload checkpoint directory: %w", ErrInvalidDirectory, err)
	}
	resolved, err := pathidentity.ResolveAndValidateManagedDirectory(filepath.Dir(directory), filepath.Base(directory))
	if err != nil {
		return nil, fmt.Errorf("%w: resolve log file upload checkpoint directory: %w", ErrInvalidDirectory, err)
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect prepared directory %q: %w", ErrInvalidDirectory, resolved, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %q is not a directory", ErrInvalidDirectory, resolved)
	}
	// 読み込んだSourceを採用する前に、引数のIDと一致することを確認します。
	// 構築後の系列IDはstate.Sourceだけに保持します。
	store := &FilesystemStore{directory: resolved, state: newDocument(sourceID), syncDirectory: infrafsstore.SyncDirectory}
	settings := filesystemStoreOptions{recoverMissing: true}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if err := store.initialize(settings.recoverMissing); err != nil {
		return nil, err
	}
	store.saveRecovery = NewSaveRecovery(&store.stateOrSaveMu, store.retrySaveLocked, settings.onError)
	store.disableSaveRetry = settings.disableSaveRetry
	return store, nil
}

// DirectoryとSourceIDは、このStoreの保存先とログ系列を返します。
func (s *FilesystemStore) Directory() string { return s.directory }
func (s *FilesystemStore) SourceID() string {
	s.stateOrSaveMu.Lock()
	defer s.stateOrSaveMu.Unlock()
	return s.state.Source
}

func (s *FilesystemStore) initialize(recoverMissing bool) error {
	registrations, initialized, err := s.readRegistrations()
	if err != nil {
		return err
	}
	document, exists, err := s.readDocumentState()
	if err != nil {
		return err
	}
	if initialized && !exists && !recoverMissing {
		return fmt.Errorf("%w: source=%q path=%q", ErrRegisteredCheckpointMissing, s.state.Source, s.documentPath())
	}
	if !initialized && exists {
		return fmt.Errorf("%w: source=%q path=%q", ErrRegisteredDestinationsMissing, s.state.Source, s.registrationsPath())
	}
	for _, cursor := range document.Cursors {
		if !slices.Contains(registrations.DestinationIDs, cursor.DestinationID) {
			return fmt.Errorf("%w: cursor destination %q is not registered", ErrCorruptCheckpoint, cursor.DestinationID)
		}
	}
	// 登録情報だけ保存された中断やcursor欠落は、過去の対象外も含めて再送可能な
	// 空cursorから復旧します。現在のfile一覧による初期位置の再計算はしません。
	restored := false
	for _, destination := range registrations.DestinationIDs {
		if cursorIndex(document.Cursors, destination) >= 0 {
			continue
		}
		if !recoverMissing {
			return fmt.Errorf("%w: source=%q destination=%q", ErrRegisteredCheckpointMissing, s.state.Source, destination)
		}
		document.Cursors = append(document.Cursors, DestinationCursor{DestinationID: destination})
		restored = true
	}
	sortCursors(document.Cursors)
	s.state = document
	s.registrations = registrations
	if exists && initialized && !restored {
		return nil
	}
	// 初回の中断と、運用中に登録情報だけが失われた状態を混同しないよう、
	// 登録情報を先に公開します。成功記録の欠落は設定に従って復旧します。
	if !initialized {
		if _, err := s.persistRegistrations(context.Background(), registrations); err != nil {
			return fmt.Errorf("initialize log file upload destination registrations: %w", err)
		}
	}
	if err := s.persistLocked(context.Background()); err != nil {
		return fmt.Errorf("initialize log file upload checkpoint: %w", err)
	}
	return nil
}

// 欠落だけを復旧対象にします。リンク切れも不正な保存先なので、欠落として置換しません。
func readCheckpointFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: checkpoint path %q is not a regular file", ErrCorruptCheckpoint, path)
	}
	return infrafsstore.ReadPrivateFile(path, fileMode)
}

// Loadは完了cursorから、指定世代のuploadが不要な宛先を求めて返します。
// 保存が未完了でもメモリ上の最新値を使用し、登録・保存・ファイルの読み直しは行いません。
func (s *FilesystemStore) Load(ctx context.Context, key CheckpointKey) (Checkpoint, error) {
	if s == nil {
		return Checkpoint{}, ErrInvalidDirectory
	}
	if err := validateCheckpointGeneration(key.Generation); err != nil {
		return Checkpoint{}, err
	}
	if err := s.lockContext(ctx); err != nil {
		return Checkpoint{}, err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	return s.checkpointSnapshot(key), nil
}

func emptyCheckpoint(key CheckpointKey) Checkpoint {
	return Checkpoint{
		Key:                     key,
		CompletedDestinationIDs: make([]objectstorage.DestinationID, 0),
	}
}

// AdvanceCompletionCursorは、指定宛先の完了cursorと、その位置までの途中経過を
// 一つの更新としてメモリへ反映し、checkpointファイルへ保存します。
// 完了cursorはkey.Generationより先にある場合も戻さず、そのcursor以前の同じ宛先の
// UploadProgressを整理します。未圧縮・圧縮ファイルで完了位置を共有します。
// 保存失敗時も更新済みstateを正本として維持し、PendingSaveErrorを返します。
// 保存失敗中の追加更新はメモリだけへ反映し、保存の再試行は構築時設定に従います。
// 変更がなければファイルへ書き直さず、現在の完了記録と保存待ちのエラーを返します。
// 対象までの世代に未解決の保存結果の矛盾があれば、SavedTargetConflictErrorを返します。
// 呼び出し側による.preservedへの保全と差分の解消が終わるまで、cursorは進めません。
// destinationIDsは上流で空のID・重複がないことを検証済みの一覧を渡します。
func (s *FilesystemStore) AdvanceCompletionCursor(
	ctx context.Context, key CheckpointKey, destinationIDs []objectstorage.DestinationID,
) (Checkpoint, error) {
	if s == nil {
		return Checkpoint{}, ErrInvalidDirectory
	}
	destinations := sortedDestinationIDs(destinationIDs)
	if err := validateCheckpointGeneration(key.Generation); err != nil {
		return Checkpoint{}, err
	}
	if err := s.lockContext(ctx); err != nil {
		return Checkpoint{}, err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	for _, progress := range s.state.UploadProgress {
		if !slices.Contains(destinations, progress.DestinationID) ||
			len(progress.SavedTargetConflicts) == 0 ||
			CompareGenerations(progress.Key.Generation, key.Generation) > 0 {
			continue
		}
		return Checkpoint{}, &SavedTargetConflictError{
			Key:           progress.Key,
			DestinationID: progress.DestinationID,
			Conflicts:     cloneSavedTargetConflicts(progress.SavedTargetConflicts),
		}
	}
	// 直接利用でも登録済みIDを先に確定し、cursor欠落時に新宛先へ戻りません。
	if err := s.registerDestinationIDsLocked(ctx, s.unregisteredDestinationIDs(destinations)); err != nil {
		return Checkpoint{}, err
	}
	candidate := cloneDocument(s.state)
	changed := false
	for _, destination := range destinations {
		index := cursorIndex(candidate.Cursors, destination)
		if index < 0 {
			candidate.Cursors = append(candidate.Cursors, DestinationCursor{DestinationID: destination})
			index = len(candidate.Cursors) - 1
		}
		completed := &candidate.Cursors[index].Generation
		if *completed == "" || CompareGenerations(key.Generation, *completed) > 0 {
			*completed = key.Generation
			changed = true
		}
		remaining := slices.DeleteFunc(candidate.UploadProgress, func(progress UploadProgress) bool {
			return progress.DestinationID == destination && CompareGenerations(progress.Key.Generation, *completed) <= 0
		})
		if len(remaining) != len(candidate.UploadProgress) {
			candidate.UploadProgress = remaining
			changed = true
		}
	}
	if !changed {
		return s.checkpointSnapshot(key), s.pendingSaveErrorLocked()
	}
	sortCursors(candidate.Cursors)
	err := s.updateStateAndSaveLocked(ctx, candidate)
	if err != nil && !IsPendingSaveOnly(err) {
		return Checkpoint{}, err
	}
	return s.checkpointSnapshot(key), err
}

func (s *FilesystemStore) lockContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("log file upload checkpoint context is nil")
	}
	return s.stateOrSaveMu.LockContext(ctx)
}

func (s *FilesystemStore) readDocumentState() (checkpointDocument, bool, error) {
	path := s.documentPath()
	payload, err := readCheckpointFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return newDocument(s.state.Source), false, nil
	}
	if err != nil {
		return checkpointDocument{}, false, fmt.Errorf("%w: %w", ErrUnreadableCheckpoint, err)
	}
	decoded, err := decodeDocument(payload)
	if err != nil {
		return checkpointDocument{}, false, fmt.Errorf("%w: path=%q: %w", ErrCorruptCheckpoint, path, err)
	}
	if err := validateDocument(decoded, s.state.Source); err != nil {
		return checkpointDocument{}, false, fmt.Errorf("%w: path=%q: %w", ErrCorruptCheckpoint, path, err)
	}
	return decoded, true, nil
}

// updateStateAndSaveLockedはメモリ上の正本を更新し、checkpointファイルへ保存します。
// 保存失敗中は正本の更新だけ行い、保存は既存の復旧処理に任せます。
// 呼び出し元はstateOrSaveMuを保持してください。
func (s *FilesystemStore) updateStateAndSaveLocked(ctx context.Context, candidate checkpointDocument) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// candidateは検証済みの入力と既存stateから内部で組み立てたものです。
	s.state = cloneDocument(candidate)
	if s.lastSaveErr != nil {
		return s.pendingSaveErrorLocked()
	}
	return s.saveLocked(ctx)
}

// persistLockedは公開済み宛先登録の同期を先に完了し、現在のstate全体を
// 親ディレクトリの同期まで書き出します。
// 呼び出し元はs.stateOrSaveMuを保持します。通常の更新と保存復旧が共用します。
func (s *FilesystemStore) persistLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.registrationsPending {
		if _, err := s.persistRegistrations(ctx, s.registrations); err != nil {
			return fmt.Errorf("save published log file upload destination registrations: %w", err)
		}
		s.registrationsPending = false
	}
	payload, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode log file upload checkpoint: %w", err)
	}
	payload = append(payload, '\n')
	if _, err := writeAtomically(ctx, s.directory, s.documentPath(), payload, s.syncDirectory); err != nil {
		return err
	}
	return nil
}

func (s *FilesystemStore) documentPath() string {
	return filepath.Join(s.directory, s.state.Source+documentFileSuffix)
}

// DocumentPathは、構築時に解決した実体directoryにあるcheckpoint文書のpathを返します。
// 保存失敗の通知や診断で、どの文書の保存に失敗したかを示すために使います。
func (s *FilesystemStore) DocumentPath() string {
	if s == nil {
		return ""
	}
	return s.documentPath()
}

func newDocument(sourceID string) checkpointDocument {
	return checkpointDocument{Source: sourceID, Cursors: make([]DestinationCursor, 0)}
}

func cloneDocument(source checkpointDocument) checkpointDocument {
	result := checkpointDocument{Source: source.Source, Cursors: make([]DestinationCursor, len(source.Cursors))}
	copy(result.Cursors, source.Cursors)
	if len(source.UploadProgress) > 0 {
		result.UploadProgress = make([]UploadProgress, len(source.UploadProgress))
		for index, progress := range source.UploadProgress {
			result.UploadProgress[index] = cloneUploadProgress(progress)
		}
	}
	return result
}

func cursorIndex(cursors []DestinationCursor, destination objectstorage.DestinationID) int {
	for index, cursor := range cursors {
		if cursor.DestinationID == destination {
			return index
		}
	}
	return -1
}

func sortCursors(cursors []DestinationCursor) {
	sort.Slice(cursors, func(i, j int) bool { return cursors[i].DestinationID < cursors[j].DestinationID })
}

func decodeDocument(payload []byte) (checkpointDocument, error) {
	var decoded checkpointDocument
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return checkpointDocument{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return checkpointDocument{}, errors.New("multiple JSON values")
		}
		return checkpointDocument{}, fmt.Errorf("trailing JSON data: %w", err)
	}
	return decoded, nil
}

// ValidateSourceIDはcheckpointファイル名に使えるsource IDだけを受理します。
// Storeを開かないupload無効時にも、同じ入力検証を行うために使います。
func ValidateSourceID(sourceID string) error {
	if strings.TrimSpace(sourceID) == "" {
		return ErrInvalidSourceID
	}
	for _, character := range sourceID {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '_' || character == '-' {
			continue
		}
		return fmt.Errorf("%w: %q", ErrInvalidSourceID, sourceID)
	}
	return nil
}
