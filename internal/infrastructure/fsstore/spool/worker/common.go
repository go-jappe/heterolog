package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const tmpFileSuffix = ".tmp"

// 状態遷移の共通処理

func (s *StateStore) stateDir(state pkgspool.State) (spooldir.StateDir, error) {
	switch state {
	case pkgspool.StateActive:
		return s.RootDir.ActiveDir(), nil
	case pkgspool.StateProcessing:
		return s.RootDir.ProcessingDir(), nil
	case pkgspool.StateDone:
		return s.RootDir.DoneDir(), nil
	case pkgspool.StateDead:
		return s.RootDir.DeadDir(), nil
	case pkgspool.StateQuarantine:
		return s.RootDir.QuarantineDir(), nil
	default:
		return spooldir.StateDir{}, fmt.Errorf("invalid spool state: %q", state)
	}
}

// 指定した state 配下へファイルを移動する
func (s *StateStore) moveToState(
	ctx context.Context,
	fromPath string,
	fromState pkgspool.State,
	toState pkgspool.State,
) (MoveResult, error) {
	_ = ctx

	fromPath, rel, err := s.validateSpoolFilePathUnderState(fromState, fromPath)
	if err != nil {
		return MoveResult{}, err
	}

	toDir, err := s.stateDir(toState)
	if err != nil {
		return MoveResult{}, fmt.Errorf("failed to resolve to state dir: %w", err)
	}

	// state を切り替えた移動先パスを組み立てる
	toPath := filepath.Join(toDir.Path(), rel)

	// 移動先ディレクトリがなければ作成する
	if err := spooldir.EnsureDirectoryNoSymlinks(s.RootDir.String(), filepath.Dir(toPath)); err != nil {
		return MoveResult{}, fmt.Errorf("failed to create destination dir: %w", err)
	}

	// ファイルを移動する
	if err := os.Rename(fromPath, toPath); err != nil {
		return MoveResult{}, fmt.Errorf("failed to move file: %w", err)
	}

	// move 自体は成功しているため、後始末は best effort にする
	_ = s.pruneEmptyStateParents(fromState, filepath.Dir(fromPath))

	return MoveResult{
		FromPath: fromPath,
		ToPath:   toPath,
	}, nil
}

// startDir から state 直下まで空ディレクトリを削除する。
// state ルート自身は削除しない。
func (s *StateStore) pruneEmptyStateParents(startState pkgspool.State, startDir string) error {
	baseStateDir, err := s.stateDir(startState)
	if err != nil {
		return fmt.Errorf("failed to resolve state dir: %w", err)
	}

	baseDir := filepath.Clean(baseStateDir.Path())
	dir := filepath.Clean(startDir)

	rel, err := filepath.Rel(baseDir, dir)
	if err != nil {
		return fmt.Errorf("failed to resolve relative cleanup dir: %w", err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil
	}

	for {
		if dir == baseDir {
			return nil
		}

		err := os.Remove(dir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				dir = filepath.Dir(dir)
				continue
			}

			// 空でない、権限不足、その他の理由でも
			// これ以上は消せないので終了
			return nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

// 指定した state 配下から古い順に limit 件のファイル path を取得する。
func (s *StateStore) listOldestStatePaths(
	ctx context.Context,
	state pkgspool.State,
	limit int,
) ([]string, error) {
	candidates, err := s.listStateFileCandidatesAfter(ctx, state, "", limit)
	if err != nil {
		return nil, err
	}
	paths := make([]string, len(candidates))
	for i, candidate := range candidates {
		paths[i] = candidate.Path
	}
	return paths, nil
}

// 指定 state 配下の spool file を削除する。
// state ルート外の path と spool 拡張子でない path は削除しない。
func (s *StateStore) removeFileUnderState(
	ctx context.Context,
	state pkgspool.State,
	filePath string,
) error {
	_ = ctx

	filePath, _, err := s.validateSpoolFilePathUnderState(state, filePath)
	if err != nil {
		return err
	}

	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove %s spool file: %w", state, err)
	}

	// A retry after unlink succeeded must still finish syncing the deletion.
	// Earlier successful cleanup may have pruned the bucket, so sync the nearest
	// existing parent within this spool root before pruning any more directories.
	if err := syncExistingSpoolParent(s.RootDir.String(), filepath.Dir(filePath)); err != nil {
		return fmt.Errorf("sync removed %s spool file directory: %w", state, err)
	}
	_ = s.pruneEmptyStateParents(state, filepath.Dir(filePath))

	return nil
}

func syncExistingSpoolParent(root, directory string) error {
	for {
		info, err := os.Lstat(directory)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("spool parent is not a directory: %s", directory)
			}
			return infrafsstore.SyncDirectory(directory)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if directory == filepath.Clean(root) {
			// A store whose whole root is absent has no retained source to remove.
			return nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return err
		}
		directory = parent
	}
}

// 指定 state 配下の spool file path であることを検証する。
// 戻り値の cleanPath は正規化済みのフルパス、rel は state ルートからの相対パス。
func (s *StateStore) validateSpoolFilePathUnderState(
	state pkgspool.State,
	filePath string,
) (cleanPath string, rel string, err error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return "", "", errors.New("file path is empty")
	}

	stateDir, err := s.stateDir(state)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve state dir: %w", err)
	}

	baseDir := filepath.Clean(stateDir.Path())
	cleanPath = filepath.Clean(filePath)

	rel, err = filepath.Rel(baseDir, cleanPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve relative path from %s dir: %w", state, err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("path is outside %s dir", state)
	}

	if filepath.Ext(cleanPath) != spooldir.SpoolExt {
		return "", "", fmt.Errorf("path is not a spool file: %s", cleanPath)
	}

	return cleanPath, rel, nil
}
func decodeSpoolRecords(body []byte) ([]pkgspool.SpoolRecord, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, pkgspool.ErrEmptySpoolFile
	}

	lines := bytes.Split(body, []byte{'\n'})
	records := make([]pkgspool.SpoolRecord, 0, len(lines))

	for i, line := range lines {
		lineNo := i + 1

		// 最後の trailing newline は許容する。
		//
		// 例:
		//   {"OccurredAt": "...", "Payload": {...}}\n
		//
		// bytes.Split すると最後に空要素ができるため、それだけ skip する。
		if i == len(lines)-1 && len(line) == 0 {
			continue
		}

		record, err := decodeSpoolRecordLine(line, lineNo)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	if len(records) == 0 {
		return nil, pkgspool.ErrEmptySpoolFile
	}

	return records, nil
}

func decodeSpoolRecordLine(
	line []byte,
	lineNo int,
) (pkgspool.SpoolRecord, error) {
	var record pkgspool.SpoolRecord

	if len(bytes.TrimSpace(line)) == 0 {
		return record, fmt.Errorf("%w: line %d is empty",
			pkgspool.ErrMalformedSpoolJSON,
			lineNo,
		)
	}

	// まず JSON / NDJSON として壊れていないかを見る。
	// ここで false なら SpoolRecord 以前に JSON として不正。
	if !json.Valid(line) {
		return record, fmt.Errorf("%w: line %d is not valid JSON",
			pkgspool.ErrMalformedSpoolJSON,
			lineNo,
		)
	}

	// JSON としては valid だが、SpoolRecord として復元できない場合は
	// ErrInvalidSpoolRecord に分類する。
	if err := json.Unmarshal(line, &record); err != nil {
		// json.Unmarshalのerrorは保存されたrecordの値（*time.ParseErrorのValueなど）を
		// 保持し得るため、errors.Asで取り出せないよう原因としては包まず、文言だけを残す。
		return record, fmt.Errorf("%w: line %d: %v",
			pkgspool.ErrInvalidSpoolRecord,
			lineNo,
			err,
		)
	}

	if err := validateSpoolRecord(record); err != nil {
		return record, fmt.Errorf("%w: line %d: %w",
			pkgspool.ErrInvalidSpoolRecord,
			lineNo,
			err,
		)
	}

	return record, nil
}

// validateSpoolRecordが返す、SpoolRecordとして不正な理由です。recordの値は含みません。
var (
	errSpoolRecordOccurredAtZero = errors.New("occurredAt is zero")
	errSpoolRecordPayloadEmpty   = errors.New("payload is empty")
	errSpoolRecordPayloadNotJSON = errors.New("payload is not valid JSON")
)

func validateSpoolRecord(record pkgspool.SpoolRecord) error {
	if record.OccurredAt.IsZero() {
		return errSpoolRecordOccurredAtZero
	}

	if len(bytes.TrimSpace(record.Payload)) == 0 {
		return errSpoolRecordPayloadEmpty
	}

	if !json.Valid(record.Payload) {
		return errSpoolRecordPayloadNotJSON
	}

	return nil
}

// buildSpoolNDJSON は、records を 1 行 1 record の NDJSON に変換します。
func buildSpoolNDJSON(records []pkgspool.SpoolRecord) ([]byte, error) {
	var buf bytes.Buffer

	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal spool record: %w", err)
		}

		buf.Write(line)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}
