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

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// 登録済みIDだけをcursorと別に残し、欠落復旧を新宛先の追加と取り違えないようにします。
// cursorを失った登録済み宛先は最初から再送し、現在のファイル一覧で初期位置を決め直しません。
type destinationRegistrationDocument struct {
	Source         string                        `json:"source"`
	DestinationIDs []objectstorage.DestinationID `json:"destination_ids"`
}

// DestinationCursorsは現在の完了位置のコピーを返します。保存未完了でもmemoryを使用します。
func (s *FilesystemStore) DestinationCursors(ctx context.Context) ([]DestinationCursor, error) {
	if s == nil {
		return nil, ErrInvalidDirectory
	}
	if err := s.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]DestinationCursor{}, s.state.Cursors...), nil
}

// initialDestinationCursorは新宛先の登録時点で対象外とする最後の世代を返します。
// 最終世代に未圧縮のkey、つまりまだ追記され得る現在の期間の元ログがあれば、書き込み中の
// 可能性があるため、その一つ前までを対象外とします。過去の期間の元ログは追記されないため、
// ScanRegistrationFileKeysが完結した世代として渡し、ここでは他の既存世代と同じく対象外です。
// 観測した世代がなければ空文字を返し、最初に作成される世代から送信します。
// 返す世代は初期の完了位置と、送信・退避済みと区別する対象外の範囲の両方に使います。
// keysはRegisterDestinationsで検証済みです。
func initialDestinationCursor(keys []CheckpointKey) string {
	latest := ""
	for _, key := range keys {
		if latest == "" || CompareGenerations(key.Generation, latest) > 0 {
			latest = key.Generation
		}
	}
	active := false
	for _, key := range keys {
		if !key.Compressed && key.Generation == latest {
			active = true
		}
	}
	if !active {
		return latest
	}
	// 世代番号0も許されるため、番号を減らすのではなく、実在する直前の世代を使います。
	boundary := ""
	for _, key := range keys {
		generation := key.Generation
		if CompareGenerations(generation, latest) < 0 && (boundary == "" || CompareGenerations(generation, boundary) > 0) {
			boundary = generation
		}
	}
	return boundary
}

// RegisterDestinationsは新宛先の初期完了位置を保存します。登録済みIDは決め直しません。
// 登録前から存在した世代はuploadが不要ですが、送信・退避済みではありません。cursorには
// 同じ世代を対象外の範囲として記録し、upload完了を理由にした元ファイルの削除と区別します。
// existingFileKeysには、ScanRegistrationFileKeysのように、まだ追記され得る元ログだけを
// 未圧縮のkeyとして渡します。最新の世代が未圧縮のkeyなら、その世代を新しい宛先の対象に残します。
// 呼び出し元は系列のlifecycle lockを保持して走査・登録を行い、処理開始前に完了させます。
// destinationIDsは上流で空のID・重複がないことを検証済みの一覧を渡します。
func (s *FilesystemStore) RegisterDestinations(ctx context.Context, destinationIDs []objectstorage.DestinationID, existingFileKeys []CheckpointKey) error {
	if s == nil {
		return ErrInvalidDirectory
	}
	destinations := sortedDestinationIDs(destinationIDs)
	for index, key := range existingFileKeys {
		if err := validateCheckpointGeneration(key.Generation); err != nil {
			return fmt.Errorf("existing file key[%d]: %w", index, err)
		}
	}
	if err := s.lockContext(ctx); err != nil {
		return err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	newIDs := s.unregisteredDestinationIDs(destinations)
	initial := ""
	if len(newIDs) > 0 {
		initial = initialDestinationCursor(existingFileKeys)
	}
	if err := s.registerDestinationIDsLocked(ctx, newIDs); err != nil {
		return err
	}
	candidate := cloneDocument(s.state)
	changed := false
	for _, destination := range s.registrations.DestinationIDs {
		if cursorIndex(candidate.Cursors, destination) >= 0 {
			continue
		}
		cursor := DestinationCursor{DestinationID: destination}
		if slices.Contains(newIDs, destination) {
			cursor.Generation, cursor.OutOfScopeGeneration = initial, initial
		}
		// 以前の登録だけが完了している場合、現在の一覧を使わず空から復旧します。
		candidate.Cursors = append(candidate.Cursors, cursor)
		changed = true
	}
	if !changed {
		return nil
	}
	sortCursors(candidate.Cursors)
	return s.updateStateAndSaveLocked(ctx, candidate)
}

func (s *FilesystemStore) unregisteredDestinationIDs(destinations []objectstorage.DestinationID) []objectstorage.DestinationID {
	var result []objectstorage.DestinationID
	for _, destination := range destinations {
		if !slices.Contains(s.registrations.DestinationIDs, destination) {
			result = append(result, destination)
		}
	}
	return result
}

// 登録情報をcursorより先に保存します。ここで停止しても、再起動後は登録済み宛先として
// 空cursorから復旧でき、新しい初期位置によって残存ログを送信対象外にしません。
func (s *FilesystemStore) registerDestinationIDsLocked(ctx context.Context, newIDs []objectstorage.DestinationID) error {
	if s.registrationsPending {
		// 要求されたcursor/progressの更新は未受理です。PendingSaveErrorにすると、
		// 利用側が更新済みと判断して元ログの削除などを続行してしまいます。
		return fmt.Errorf("log file upload destination registration save pending: %w", s.lastSaveErr)
	}
	if len(newIDs) == 0 {
		return nil
	}
	candidate := destinationRegistrationDocument{
		Source:         s.registrations.Source,
		DestinationIDs: append(append([]objectstorage.DestinationID{}, s.registrations.DestinationIDs...), newIDs...),
	}
	slices.Sort(candidate.DestinationIDs)
	published, err := s.persistRegistrations(ctx, candidate)
	if err != nil {
		registrationErr := fmt.Errorf("register log file upload destinations: %w", err)
		if published {
			// 公開済みのIDを忘れると、次の登録で新しいfile一覧から初期cursorを
			// 決め直して未送信世代を飛び越えます。再起動時と同じ空cursorを保持します。
			s.registrations = candidate
			for _, destination := range newIDs {
				if cursorIndex(s.state.Cursors, destination) < 0 {
					s.state.Cursors = append(s.state.Cursors, DestinationCursor{DestinationID: destination})
				}
			}
			sortCursors(s.state.Cursors)
			s.registrationsPending = true
			s.lastSaveErr = registrationErr
			if !s.disableSaveRetry {
				s.saveRecovery.StartLocked()
			}
		}
		return registrationErr
	}
	s.registrations = candidate
	return nil
}

func (s *FilesystemStore) checkpointSnapshot(key CheckpointKey) Checkpoint {
	result := emptyCheckpoint(key)
	for _, cursor := range s.state.Cursors {
		if cursor.Covers(key) {
			result.CompletedDestinationIDs = append(result.CompletedDestinationIDs, cursor.DestinationID)
		}
		// 対象外の範囲は完了位置以前にあり、完了した宛先の一部としてだけ返します。
		if cursor.OutOfScope(key) {
			result.OutOfScopeDestinationIDs = append(result.OutOfScopeDestinationIDs, cursor.DestinationID)
		}
	}
	return result
}

func (s *FilesystemStore) registrationsPath() string {
	return filepath.Join(s.directory, s.state.Source+registrationFileSuffix)
}

func (s *FilesystemStore) persistRegistrations(ctx context.Context, candidate destinationRegistrationDocument) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	payload, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		return false, fmt.Errorf("encode log file upload destination registrations: %w", err)
	}
	payload = append(payload, '\n')
	return writeAtomically(ctx, s.directory, s.registrationsPath(), payload, s.syncDirectory)
}

func (s *FilesystemStore) readRegistrations() (destinationRegistrationDocument, bool, error) {
	path := s.registrationsPath()
	payload, err := readCheckpointFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return destinationRegistrationDocument{Source: s.state.Source, DestinationIDs: []objectstorage.DestinationID{}}, false, nil
	}
	if err != nil {
		return destinationRegistrationDocument{}, false, fmt.Errorf("read log file upload destination registrations: %w", err)
	}
	decoded, err := decodeRegistrations(payload, s.state.Source)
	if err != nil {
		return destinationRegistrationDocument{}, false, fmt.Errorf("%w: path=%q: %w", ErrCorruptCheckpoint, path, err)
	}
	return decoded, true, nil
}

func decodeRegistrations(payload []byte, sourceID string) (destinationRegistrationDocument, error) {
	var decoded destinationRegistrationDocument
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return decoded, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return decoded, errors.New("multiple JSON values")
		}
		return decoded, fmt.Errorf("trailing JSON data: %w", err)
	}
	if decoded.Source != sourceID {
		return decoded, fmt.Errorf("%w: source %q does not match %q", ErrCheckpointSourceMismatch, decoded.Source, sourceID)
	}
	if decoded.DestinationIDs == nil {
		return decoded, errors.New("destination_ids field is missing or null")
	}
	if err := validateDestinationIDs(decoded.DestinationIDs); err != nil {
		return decoded, err
	}
	decoded.DestinationIDs = sortedDestinationIDs(decoded.DestinationIDs)
	return decoded, nil
}
