// Package retryafterは、spoolの送信先がRetry-Afterで指定した再送の下限時刻を、
// その送信先を担当するrunnerのspoolディレクトリ内へ永続化します。
//
// markerは送信先ごとに一つのJSON fileで、次の位置に置きます。
//
//	通常ログのspool:              <spool root>/.retry-after/<backend>.json
//	object storageの圧縮ログspool: <宛先別directory>/.retry-after/<宛先ID>.json
//
// どちらもstate directoryの外にあるため、再送・purge・容量統計の対象になりません。
// file名には既存の送信先ID（backend名・object storageの宛先ID）を使います。
package retryafter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// DirectoryNameは、markerを置くdirectoryの名前です。
const DirectoryName = ".retry-after"

const (
	markerFileExtension = ".json"
	markerVersion       = 1
	// markerFileModeとmarkerDirectoryModeは、他のspool fileと同じく所有者だけに公開する権限です。
	// Windowsでは既存の補助関数により、実行ユーザー・SYSTEM・AdministratorsだけのACLを設定します。
	markerFileMode      = 0o600
	markerDirectoryMode = 0o700
	// maxMarkerFileSizeを超えるfileは、markerとして書いた内容ではないため壊れたmarkerとして扱います。
	maxMarkerFileSize      = 4096
	maxDestinationIDLength = 128
)

var (
	// ErrInvalidDestinationIDは、送信先IDをmarkerのfile名として安全に使えないことを表します。
	ErrInvalidDestinationID = errors.New("invalid spool retry-after marker destination id")
	// ErrInvalidMarkerは、markerの内容がこの送信先の待機期限として読めないことを表します。
	// 呼び出し元はmarkerがないものとして扱い、次の保存で置き換えます。
	ErrInvalidMarker = errors.New("invalid spool retry-after marker")
	// ErrMarkerOwnerDirectoryは、markerを置く送信先のdirectoryが存在しないか、実体のdirectoryでないことを表します。
	ErrMarkerOwnerDirectory = errors.New("spool retry-after marker owner directory is unavailable")
)

// MarkerOperationは、失敗したmarkerの操作です。
type MarkerOperation string

const (
	MarkerOperationLoad   MarkerOperation = "load"
	MarkerOperationSave   MarkerOperation = "save"
	MarkerOperationRemove MarkerOperation = "remove"
	// MarkerOperationCleanupは、保存の途中で停止したprocessが残した一時fileの削除です。
	MarkerOperationCleanup MarkerOperation = "cleanup"
)

// temporaryMarkerSuffixは、保存中のmarkerの一時file名の末尾です。一時file名は
// "."+送信先ID+".json."+os.CreateTempの数字+".tmp"です。
const temporaryMarkerSuffix = ".tmp"

// MarkerErrorは、markerの読み込み・保存・削除の失敗を表します。原因はerrors.Is・errors.Asで辿れます。
// 保存後のfile・directoryの同期失敗はpkgfsstore.ErrDurabilityUnconfirmedを含みます。
type MarkerError struct {
	Operation MarkerOperation
	Path      string
	Err       error
}

func (e *MarkerError) Error() string {
	return fmt.Sprintf("%s spool retry-after marker %q: %v", e.Operation, e.Path, e.Err)
}

func (e *MarkerError) Unwrap() error { return e.Err }

// Stateは、一つの送信先に保存する待機の状態です。
type State struct {
	// NotBeforeは、送信先へ次に送信してよい時刻です。上限までに短縮済みの値を保存します。
	NotBefore time.Time
	// AppliedThroughは、spoolのrecordに保存された再送下限時刻（SpoolRecord.NotBefore）のうち、
	// NotBeforeへ反映済みの最も遅い時刻です。この時刻以前の保存済み期限は、再起動後も待機を
	// 延ばしません。recordに期限を保存しないspoolではzero valueです。
	AppliedThrough time.Time
}

// Expiredは、nowの時点で送信を制限せず、反映済みの保存期限もすべて過ぎたかを返します。
func (s State) Expired(now time.Time) bool {
	return !now.Before(s.NotBefore) && !now.Before(s.AppliedThrough)
}

// markerDocumentはmarker fileの保存形式です。
type markerDocument struct {
	Version        int       `json:"version"`
	DestinationID  string    `json:"destination_id"`
	NotBefore      time.Time `json:"not_before"`
	AppliedThrough time.Time `json:"applied_through,omitzero"`
}

// Storeは一つの送信先のmarkerを読み書きします。同じmarkerを使う操作は呼び出し元が直列化します。
// spoolのdirectory leaseなどにより、一つのprocessの一つのrunnerだけが使う前提です。
type Store struct {
	owner         string
	directory     string
	path          string
	destinationID string

	// prepareDirectoryは、保存前にmarkerのdirectoryを用意します。
	prepareDirectory func() error
	// syncFileとsyncDirectoryは、試験で同期の失敗を再現するために差し替えます。
	syncFile      func(*os.File) error
	syncDirectory func(string) error
}

// NewLogSpoolStoreは、通常ログのspool root直下の.retry-afterにmarkerを置くStoreを返します。
// rootにはspool.NewRootDirで解決したspool root、destinationIDにはbackend名を渡します。
// directoryは保存時にspool rootの他のdirectoryと同じ補助関数で用意し、親の同期失敗の記録と
// pkgfsstore.ErrDurabilityUnconfirmedの扱いも共有します。
func NewLogSpoolStore(root, destinationID string) (*Store, error) {
	store, err := newStore(root, destinationID)
	if err != nil {
		return nil, err
	}
	store.prepareDirectory = func() error {
		return spooldir.EnsureDirectoryNoSymlinks(store.owner, store.directory)
	}
	return store, nil
}

// NewObjectStorageStoreは、圧縮ログspoolの宛先別directory直下の.retry-afterにmarkerを置く
// Storeを返します。destinationRootには宛先別directory、destinationIDにはその宛先IDを渡します。
// 宛先別directoryは作成せず、保存時に存在しなければ保存を失敗にします。
func NewObjectStorageStore(destinationRoot, destinationID string) (*Store, error) {
	store, err := newStore(destinationRoot, destinationID)
	if err != nil {
		return nil, err
	}
	store.prepareDirectory = func() error {
		info, err := os.Lstat(store.owner)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrMarkerOwnerDirectory, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %q is not a directory", ErrMarkerOwnerDirectory, store.owner)
		}
		return infrafsstore.EnsureDirectoryNoSymlinks(store.directory, markerDirectoryMode)
	}
	return store, nil
}

func newStore(owner, destinationID string) (*Store, error) {
	if err := ValidateDestinationID(destinationID); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("%w: owner directory is empty", ErrMarkerOwnerDirectory)
	}
	directory := filepath.Join(owner, DirectoryName)
	return &Store{
		owner:         owner,
		directory:     directory,
		path:          filepath.Join(directory, destinationID+markerFileExtension),
		destinationID: destinationID,
		syncFile:      (*os.File).Sync,
		syncDirectory: infrafsstore.SyncDirectory,
	}, nil
}

// ValidateDestinationIDは、送信先IDをmarkerのfile名に使えるかを検証します。
// 英小文字・数字で始まり、英小文字・数字・hyphen・underscoreだけからなる128文字以下の
// IDを受け付けます。大文字小文字を区別しないfilesystemでも別のIDと同じ名前にならず、
// Windowsの予約デバイス名（con・nulなど）も拒否します。
func ValidateDestinationID(id string) error {
	if id == "" || len(id) > maxDestinationIDLength {
		return fmt.Errorf("%w: %q must have 1 to %d characters", ErrInvalidDestinationID, id, maxDestinationIDLength)
	}
	for index, character := range id {
		alphanumeric := character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
		if !alphanumeric && (index == 0 || character != '-' && character != '_') {
			return fmt.Errorf(
				"%w: %q must start with a lowercase letter or digit and contain only lowercase letters, digits, hyphens or underscores",
				ErrInvalidDestinationID, id,
			)
		}
	}
	if isWindowsReservedName(id) {
		return fmt.Errorf("%w: %q is a reserved Windows device name", ErrInvalidDestinationID, id)
	}
	return nil
}

// isWindowsReservedNameは、拡張子を付けても予約名として扱われるWindowsのデバイス名かを返します。
// ValidateDestinationIDが英小文字に限定した後で呼びます。
func isWindowsReservedName(id string) bool {
	switch id {
	case "con", "prn", "aux", "nul":
		return true
	}
	if len(id) == 4 && (id[:3] == "com" || id[:3] == "lpt") && id[3] >= '0' && id[3] <= '9' {
		return true
	}
	return false
}

// Pathはmarker fileのpathを返します。
func (s *Store) Path() string { return s.path }

// Loadはmarkerを読み、再起動時の規則を適用した状態を返します。processで最初に一度だけ呼び、
// 保存の途中で停止したprocessが残したこの送信先の一時fileも削除します。他のfileには触れません。
// 戻り値のerrorは警告で、errorがあっても戻り値の状態を待機に使います。
//   - markerがない場合は、zero valueを返します。
//   - 読めない・内容が壊れている場合は、markerがないものとしてzero valueとerrorを返します。
//     fileは削除せず、次のSaveで置き換えます。
//   - Expiredなら、fileを削除してzero valueを返します。
//   - NotBeforeがnowからmaxWaitより先なら、now+maxWaitへ短縮した状態を保存してから、短縮した
//     状態と短縮内容を返します。保存に失敗した場合も短縮した状態を返します。
//
// errorは*MarkerError、または複数の*MarkerErrorをerrors.Joinしたものです。
func (s *Store) Load(now time.Time, maxWait time.Duration) (State, *pkgspool.RetryAfterClamp, error) {
	var cleanupErr error
	if err := s.removeStaleTemporaryFiles(); err != nil {
		cleanupErr = &MarkerError{Operation: MarkerOperationCleanup, Path: s.directory, Err: err}
	}
	state, clamp, err := s.load(now, maxWait)
	return state, clamp, errors.Join(cleanupErr, err)
}

func (s *Store) load(now time.Time, maxWait time.Duration) (State, *pkgspool.RetryAfterClamp, error) {
	state, found, err := s.read()
	if err != nil {
		return State{}, nil, &MarkerError{Operation: MarkerOperationLoad, Path: s.path, Err: err}
	}
	if !found {
		return State{}, nil, nil
	}
	if state.Expired(now) {
		if err := s.Remove(); err != nil {
			return State{}, nil, err
		}
		return State{}, nil, nil
	}
	if requested := state.NotBefore.Sub(now); maxWait > 0 && requested > maxWait {
		// 短縮した期限を保存し、再起動のたびに上限まで待ち直すことを防ぎます。
		// AppliedThroughは短縮前の期限以上なので、反映済みの保存期限も待機を延ばしません。
		state.NotBefore = now.Add(maxWait).Round(0)
		clamp := &pkgspool.RetryAfterClamp{
			Requested:     requested,
			MaxRetryAfter: maxWait,
			NotBefore:     state.NotBefore,
			Stored:        true,
		}
		return state, clamp, s.Save(state)
	}
	return state, nil, nil
}

// removeStaleTemporaryFilesは、この送信先の保存中に停止したprocessが残した一時fileだけを
// 削除し、削除した場合はdirectoryを同期します。名前が一時fileの形式に一致する通常fileだけを
// 対象とし、markerや他の送信先のfile、directory・linkなどには触れません。markerのdirectoryが
// linkなどの場合は辿らず、読み込み時に壊れたmarkerとして扱います。
func (s *Store) removeStaleTemporaryFiles() error {
	if _, err := pathidentity.ResolveAndValidateManagedDirectory(s.owner, DirectoryName); err != nil {
		var obstruction *pathidentity.ManagedDirectoryObstructionError
		if errors.As(err, &obstruction) {
			return nil
		}
		return err
	}
	entries, err := infrafsstore.ReadDirectoryEntries(s.directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	prefix := "." + s.destinationID + markerFileExtension + "."
	var errs []error
	removed := false
	for _, entry := range entries {
		if !isTemporaryMarkerName(entry.Name(), prefix) {
			continue
		}
		path := filepath.Join(s.directory, entry.Name())
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
			continue
		}
		removed = true
	}
	if removed {
		if err := s.syncDirectory(s.directory); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// isTemporaryMarkerNameは、nameがprefixで始まり、os.CreateTempが付ける数字だけを挟んで
// temporaryMarkerSuffixで終わるかを返します。
func isTemporaryMarkerName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, temporaryMarkerSuffix) {
		return false
	}
	random := name[len(prefix):]
	random = random[:len(random)-len(temporaryMarkerSuffix)]
	if random == "" {
		return false
	}
	for _, character := range random {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

// read はmarkerを検証して読みます。markerのdirectoryやfileがリンクなどの場合は辿りません。
func (s *Store) read() (State, bool, error) {
	if _, err := pathidentity.ResolveAndValidateManagedDirectory(s.owner, DirectoryName); err != nil {
		var obstruction *pathidentity.ManagedDirectoryObstructionError
		if errors.As(err, &obstruction) {
			return State{}, false, fmt.Errorf("%w: %w", ErrInvalidMarker, err)
		}
		return State{}, false, err
	}
	file, info, err := infrafsstore.OpenRegularFile(s.path, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if errors.Is(err, infrafsstore.ErrNotRegularFile) {
		return State{}, false, fmt.Errorf("%w: %w", ErrInvalidMarker, err)
	}
	if err != nil {
		return State{}, false, err
	}
	defer file.Close()
	if info.Size() > maxMarkerFileSize {
		return State{}, false, fmt.Errorf("%w: size %d exceeds %d bytes", ErrInvalidMarker, info.Size(), maxMarkerFileSize)
	}
	body, err := io.ReadAll(io.LimitReader(file, maxMarkerFileSize+1))
	if err != nil {
		return State{}, false, err
	}
	var document markerDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return State{}, false, fmt.Errorf("%w: decode: %w", ErrInvalidMarker, err)
	}
	switch {
	case document.Version != markerVersion:
		return State{}, false, fmt.Errorf("%w: unsupported version %d", ErrInvalidMarker, document.Version)
	case document.DestinationID != s.destinationID:
		return State{}, false, fmt.Errorf("%w: destination id %q does not match %q", ErrInvalidMarker, document.DestinationID, s.destinationID)
	case document.NotBefore.IsZero():
		return State{}, false, fmt.Errorf("%w: not_before is missing", ErrInvalidMarker)
	}
	state := State{NotBefore: document.NotBefore, AppliedThrough: document.AppliedThrough}
	if state.AppliedThrough.Before(state.NotBefore) {
		state.AppliedThrough = state.NotBefore
	}
	return state, true, nil
}

// Saveはstateを一時fileへ書いて同期してから既存のmarkerと置き換え、directoryも同期します。
// 置き換えの前に失敗した場合は既存のmarkerを維持します。置き換え後の同期失敗では保存した
// markerを残し、pkgfsstore.ErrDurabilityUnconfirmedを含むerrorを返します。
// errorは*MarkerErrorです。
func (s *Store) Save(state State) error {
	if err := s.write(state); err != nil {
		return &MarkerError{Operation: MarkerOperationSave, Path: s.path, Err: err}
	}
	return nil
}

func (s *Store) write(state State) (err error) {
	if state.NotBefore.IsZero() {
		return fmt.Errorf("%w: not_before is zero", ErrInvalidMarker)
	}
	document := markerDocument{
		Version:        markerVersion,
		DestinationID:  s.destinationID,
		NotBefore:      state.NotBefore.Round(0).UTC(),
		AppliedThrough: state.AppliedThrough.Round(0).UTC(),
	}
	if !document.AppliedThrough.After(document.NotBefore) {
		document.AppliedThrough = time.Time{}
	}
	body, err := json.Marshal(document)
	if err != nil {
		return err
	}
	if err := s.prepareDirectory(); err != nil {
		return fmt.Errorf("prepare marker directory: %w", err)
	}
	temporary, err := os.CreateTemp(s.directory, "."+s.destinationID+markerFileExtension+".*"+temporaryMarkerSuffix)
	if err != nil {
		return fmt.Errorf("create temporary marker: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen, published := true, false
	defer func() {
		if temporaryOpen {
			_ = temporary.Close()
		}
		if !published {
			_ = os.Remove(temporaryPath)
		}
	}()
	// 内容より先に権限を確定し、所有者以外が読める期間を作りません。
	if err := infrafsstore.SetPrivateFilePermissions(temporary, markerFileMode); err != nil {
		return fmt.Errorf("set marker permissions: %w", err)
	}
	if _, err := temporary.Write(body); err != nil {
		return fmt.Errorf("write temporary marker: %w", err)
	}
	if err := s.syncFile(temporary); err != nil {
		return fmt.Errorf("%w: sync temporary marker: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	temporaryOpen = false
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary marker: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("publish marker: %w", err)
	}
	published = true
	if err := s.syncDirectory(s.directory); err != nil {
		if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
			err = fmt.Errorf("%w: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
		}
		return fmt.Errorf("sync marker directory: %w", err)
	}
	return nil
}

// Removeはmarkerを削除し、directoryを同期します。markerがなければ何もしません。
// 通常fileでない項目は削除せず、ErrInvalidMarkerを含むerrorを返します。errorは*MarkerErrorです。
func (s *Store) Remove() error {
	info, err := os.Lstat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("%w: %q is not a regular file", ErrInvalidMarker, s.path)
	}
	if err == nil {
		err = infrafsstore.RemoveFileAndSyncDirectory(s.path)
	}
	if err != nil {
		return &MarkerError{Operation: MarkerOperationRemove, Path: s.path, Err: err}
	}
	return nil
}
