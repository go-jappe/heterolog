package checkpoint

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// validateDocumentはJSONから読み込んだ系列ID・完了cursor・途中経過の整合性を検証します。
func validateDocument(candidate checkpointDocument, sourceID string) error {
	if candidate.Source != sourceID {
		return fmt.Errorf("%w: source %q does not match %q", ErrCheckpointSourceMismatch, candidate.Source, sourceID)
	}
	if candidate.Cursors == nil {
		return errors.New("cursors field is missing or null")
	}
	destinations := make([]objectstorage.DestinationID, 0, len(candidate.Cursors))
	// 世代キーの形式はDestinationCursor.UnmarshalJSONで検証済みです。
	for _, cursor := range candidate.Cursors {
		destinations = append(destinations, cursor.DestinationID)
	}
	if err := validateDestinationIDs(destinations); err != nil {
		return err
	}
	type progressKey struct {
		key         CheckpointKey
		destination objectstorage.DestinationID
	}
	seen := make(map[progressKey]struct{}, len(candidate.UploadProgress))
	for _, progress := range candidate.UploadProgress {
		if err := validateUploadProgress(progress); err != nil {
			return err
		}
		index := cursorIndex(candidate.Cursors, progress.DestinationID)
		if index < 0 {
			return fmt.Errorf("upload progress destination %q has no cursor", progress.DestinationID)
		}
		if candidate.Cursors[index].Covers(progress.Key) {
			return fmt.Errorf("upload progress for %q is already completed by destination %q", progress.Key.Generation, progress.DestinationID)
		}
		key := progressKey{key: progress.Key, destination: progress.DestinationID}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("upload progress for %q destination %q is duplicated", progress.Key.Generation, progress.DestinationID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// validateDestinationIDは一つの宛先IDを検証します。空文字・空白のみの値を拒否し、値は変更しません。
func validateDestinationID(destination objectstorage.DestinationID) error {
	if strings.TrimSpace(string(destination)) == "" {
		return fmt.Errorf("%w: destination is empty", ErrInvalidDestinationID)
	}
	return nil
}

// validateDestinationIDsは宛先一覧の各IDと重複の有無を検証します。
// 空文字・空白のみのIDと重複IDを拒否します。0件の一覧は許可し、並び順は変更しません。
func validateDestinationIDs(destinations []objectstorage.DestinationID) error {
	seen := make(map[objectstorage.DestinationID]struct{}, len(destinations))
	for index, destination := range destinations {
		if err := validateDestinationID(destination); err != nil {
			return fmt.Errorf("destination[%d]: %w", index, err)
		}
		if _, duplicate := seen[destination]; duplicate {
			return fmt.Errorf(
				"%w: destination %q is duplicated",
				ErrInvalidDestinationID,
				destination,
			)
		}
		seen[destination] = struct{}{}
	}
	return nil
}
