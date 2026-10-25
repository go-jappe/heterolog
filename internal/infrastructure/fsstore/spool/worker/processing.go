package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// ListOldestProcessingFilePaths は、processing 配下の spool file path を
// 古い順に最大 limit 件返します。
//
// limit 件に満たない場合でも error にはせず、取得できた分だけ返します。
func (s *StateStore) ListOldestProcessingFilePaths(
	ctx context.Context,
	limit int,
) ([]string, error) {
	return s.listOldestStatePaths(ctx, pkgspool.StateProcessing, limit)
}

// ListProcessingFileCandidatesAfter は、processing 配下を古い順に調べ、
// afterRelativePath より後の通常 spool ファイルを最大 limit 件返します。
//
// 空の境界は先頭から列挙します。境界は状態ディレクトリ相対の filepath 表記で、
// 境界のファイルやディレクトリが削除済みでも使えます。境界以前に追加された
// ファイルは次の先頭からの走査で検出するため、厳密なページ分割ではありません。
func (s *StateStore) ListProcessingFileCandidatesAfter(
	ctx context.Context,
	afterRelativePath string,
	limit int,
) ([]pkgspool.FileCandidate, error) {
	return s.listStateFileCandidatesAfter(ctx, pkgspool.StateProcessing, afterRelativePath, limit)
}

func (s *StateStore) ReadProcessingRecords(
	ctx context.Context,
	processingPath string,
) ([]pkgspool.SpoolRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result, err := s.ReadProcessingFile(ctx, processingPath)
	if err != nil {
		// path検証 / I/O error。spool file の形式不正ではない。
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	records, err := decodeSpoolRecords(result.Body)
	if err != nil {
		// decodeSpoolRecords 側で ErrInvalidSpoolFile 系に分類済み。
		return nil, fmt.Errorf("%w: %w", pkgspool.ErrDecodeProcessingSpoolFile, err)
	}

	return records, nil
}

// processing配下の通常ファイルを読み込む。
// OpenRegularFileでLstatとopen後の同一性を検査し、この読み込み内でCloseする。
func (s *StateStore) ReadProcessingFile(
	ctx context.Context,
	processingPath string,
) (ReadResult, error) {
	_ = ctx

	processingPath, _, err := s.validateSpoolFilePathUnderState(
		pkgspool.StateProcessing,
		processingPath,
	)
	if err != nil {
		return ReadResult{}, fmt.Errorf("%w: %w", pkgspool.ErrValidateProcessingSpoolFilePath, err)
	}

	file, _, err := infrafsstore.OpenRegularFile(processingPath, os.O_RDONLY)
	if err != nil {
		return ReadResult{}, fmt.Errorf("%w: %w", pkgspool.ErrReadProcessingSpoolFile, err)
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		return ReadResult{}, fmt.Errorf("%w: %w", pkgspool.ErrReadProcessingSpoolFile, err)
	}

	return ReadResult{
		Path: processingPath,
		Body: body,
	}, nil
}

// processing 配下から最古の spool file を1件だけ読み込む。
// 主に worker 起動時に残存 processing file を再処理する用途。
func (s *StateStore) ReadOldestProcessingFile(
	ctx context.Context,
) (ReadResult, bool, error) {
	paths, err := s.listOldestStatePaths(ctx, pkgspool.StateProcessing, 1)
	if err != nil {
		return ReadResult{}, false, fmt.Errorf("%w: %w", pkgspool.ErrListProcessingSpoolFiles, err)
	}
	if len(paths) == 0 {
		return ReadResult{}, false, nil
	}

	res, err := s.ReadProcessingFile(ctx, paths[0])
	if err != nil {
		return ReadResult{}, false, err
	}

	return res, true, nil
}

// processing -> done
//
// processing file 内の record がすべて成功した場合、または
// 失敗 record を dead/quarantine/done へ再退避し終え、
// 元 processing file を再処理対象から外してよい場合に使う。
//
// done は「この元 processing file の処理が完了した」ことを表す。
// 「全 record が成功した」ことだけを意味するわけではない。
func (s *StateStore) MoveProcessingToDone(
	ctx context.Context,
	processingPath string,
) error {
	_, err := s.moveToState(ctx, processingPath, pkgspool.StateProcessing, pkgspool.StateDone)
	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrMoveProcessingSpoolFileToDone, err)
	}
	return nil
}

// processing -> dead
//
// ファイル全体を dead 扱いできる場合のみ使う。
// 一部 record だけ permanent failure の場合は、このメソッドではなく、
// permanent record 群を新しい dead spool file として書き出す。
func (s *StateStore) MoveProcessingToDead(
	ctx context.Context,
	processingPath string,
) error {
	_, err := s.moveToState(ctx, processingPath, pkgspool.StateProcessing, pkgspool.StateDead)
	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrMoveProcessingSpoolFileToDead, err)
	}
	return nil
}

// processing -> quarantine
//
// NDJSON 破損や schema 不正など、ファイル全体を隔離したい場合に使う。
// 移動先file名へ`-quarantine-<reason>`を付け、元fileをdecodeできない場合にも
// 隔離理由を確認できるようにする。このsuffixはspool lifecycleのmanaged file命名を維持する。
// 一部 record だけ invalid の場合は、このメソッドではなく、
// invalid record 群を新しい quarantine spool file として書き出す。
func (s *StateStore) MoveProcessingToQuarantine(
	ctx context.Context,
	processingPath string,
	reason pkgspool.QuarantineReason,
) error {
	if !reason.IsValid() {
		return fmt.Errorf(
			"%w: invalid quarantine reason %q",
			pkgspool.ErrMoveProcessingSpoolFileToQuarantine,
			reason,
		)
	}

	processingPath, rel, err := s.validateSpoolFilePathUnderState(
		pkgspool.StateProcessing,
		processingPath,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrMoveProcessingSpoolFileToQuarantine, err)
	}

	quarantineDir, err := s.stateDir(pkgspool.StateQuarantine)
	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrMoveProcessingSpoolFileToQuarantine, err)
	}
	toPath := filepath.Join(quarantineDir.Path(), quarantineRelativePath(rel, reason))
	if err := spooldir.EnsureDirectoryNoSymlinks(s.RootDir.String(), filepath.Dir(toPath)); err != nil {
		return fmt.Errorf("%w: create destination directory: %w", pkgspool.ErrMoveProcessingSpoolFileToQuarantine, err)
	}
	if err := os.Rename(processingPath, toPath); err != nil {
		return fmt.Errorf("%w: move file: %w", pkgspool.ErrMoveProcessingSpoolFileToQuarantine, err)
	}
	_ = s.pruneEmptyStateParents(pkgspool.StateProcessing, filepath.Dir(processingPath))
	return nil
}

func quarantineRelativePath(rel string, reason pkgspool.QuarantineReason) string {
	dir := filepath.Dir(rel)
	name := filepath.Base(rel)
	extension := filepath.Ext(name)
	base := strings.TrimSuffix(name, extension)
	quarantineName := base + "-quarantine-" + string(reason) + extension
	if dir == "." {
		return quarantineName
	}
	return filepath.Join(dir, quarantineName)
}

// processing ファイルを削除する。
//
// retryable/dead/quarantine/done の再退避が完了し、
// 元 processing file を残す必要がない場合に使う。
func (s *StateStore) RemoveProcessingFile(
	ctx context.Context,
	processingPath string,
) error {
	if err := s.removeFileUnderState(ctx, pkgspool.StateProcessing, processingPath); err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrRemoveProcessingSpoolFile, err)
	}
	return nil
}

func (s *StateStore) StoreProcessingRecordsToDone(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) error {
	if err := s.StoreProcessingRecordsToState(ctx, processingPath, pkgspool.StateDone, records); err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrStoreProcessingRecordsToDone, err)
	}
	return nil
}

func (s *StateStore) StoreProcessingRecordsToDead(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) error {
	if err := s.StoreProcessingRecordsToState(ctx, processingPath, pkgspool.StateDead, records); err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrStoreProcessingRecordsToDead, err)
	}
	return nil
}

func (s *StateStore) StoreProcessingRecordsToQuarantine(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) error {
	if err := s.StoreProcessingRecordsToState(ctx, processingPath, pkgspool.StateQuarantine, records); err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrStoreProcessingRecordsToQuarantine, err)
	}
	return nil
}

// StoreProcessingRecordsToState は、processing file の一部 records を、
// 元の processing file と同じ bucket / file name で指定 state に保存します。
//
// 主に、一部成功・一部失敗のように processing file を分割して
// done / dead / quarantine に保存するために使います。
func (s *StateStore) StoreProcessingRecordsToState(
	ctx context.Context,
	processingPath string,
	toState pkgspool.State,
	records []pkgspool.SpoolRecord,
) error {
	_ = ctx

	if len(records) == 0 {
		return nil
	}

	switch toState {
	case pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine:
	default:
		return fmt.Errorf("%w: %s", pkgspool.ErrInvalidProcessingSplitDestinationState, toState)
	}

	_, rel, err := s.validateSpoolFilePathUnderState(
		pkgspool.StateProcessing,
		processingPath,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrValidateProcessingSpoolFilePath, err)
	}

	toDir, err := s.stateDir(toState)
	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrResolveDestinationSpoolStateDir, err)
	}

	finalPath := filepath.Join(toDir.Path(), rel)

	finalizedRecords := withFinalizedAt(records, time.Now())

	body, err := buildSpoolNDJSON(finalizedRecords)

	if err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrBuildSpoolNDJSON, err)
	}

	if err := writeSpoolFileAtPath(s.RootDir.String(), finalPath, body); err != nil {
		return fmt.Errorf("%w: %w", pkgspool.ErrWriteSplitSpoolFile, err)
	}

	return nil
}

func withFinalizedAt(
	records []pkgspool.SpoolRecord,
	finalizedAt time.Time,
) []pkgspool.SpoolRecord {
	if len(records) == 0 {
		return nil
	}

	out := make([]pkgspool.SpoolRecord, len(records))
	copy(out, records)

	for i := range out {
		if out[i].FinalizedAt.IsZero() {
			out[i].FinalizedAt = finalizedAt
		}
	}

	return out
}

// ProcessingFileExists は、processing 配下の spool file が存在するかを返す。
//
// processingPath が processing 配下の spool file path として不正な場合は error を返す。
// file が存在しない場合は error ではなく false, nil を返す。
func (s *StateStore) ProcessingFileExists(
	ctx context.Context,
	processingPath string,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	processingPath, _, err := s.validateSpoolFilePathUnderState(
		pkgspool.StateProcessing,
		processingPath,
	)
	if err != nil {
		return false, fmt.Errorf("%w: %w", pkgspool.ErrValidateProcessingSpoolFilePath, err)
	}

	exists, err := infrafsstore.PathExists(processingPath)
	if err != nil {
		return false, fmt.Errorf("failed to check processing spool file existence: %w", err)
	}

	return exists, nil
}
