package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// ErrTerminalRecoveryConflictは、保存済みのterminalの証拠をprocessing file全体と
// 整合させられないことを表します。確認のため、すべてのfileを残す必要があります。
var ErrTerminalRecoveryConflict = errors.New("processing file conflicts with saved terminal outcome")

// ErrQuarantineRecoveryConflictは、従来のquarantine用errorのidentityを維持します。
var ErrQuarantineRecoveryConflict = ErrTerminalRecoveryConflict

// ReconcileProcessingQuarantineは、従来のboolを返す入口を維持しつつ、
// すべてのterminal stateに同じ復旧規則を適用します。
func (s *StateStore) ReconcileProcessingQuarantine(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) (bool, error) {
	state, err := s.ReconcileProcessingTerminal(ctx, processingPath, records)
	return state != "", err
}

// ReconcileProcessingTerminalは、done・dead・quarantineへの書き込みが完了した後、
// processing fileの削除が中断された状態を復旧します。十分な証拠になるのは、一つの
// stateだけが完全に、同じ順序で一致する場合です。部分的な結果や複数のstateに
// またがる結果は、失われた振り分け計画を推測せず、確認のために保全します。
// errorなしで空のstateを返した場合は、通常の処理を続けてよいことを表します。
// errorなしで空でないstateを返した場合は、保存した結果を同期し、その元fileを
// 削除したことを表します。
func (s *StateStore) ReconcileProcessingTerminal(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) (pkgspool.State, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	processingPath, rel, err := s.validateSpoolFilePathUnderState(pkgspool.StateProcessing, processingPath)
	if err != nil {
		return "", err
	}
	var savedState pkgspool.State
	var savedPath string
	var savedInfo os.FileInfo
	var savedRecords []pkgspool.SpoolRecord
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		if err := ctx.Err(); err != nil {
			return savedState, err
		}
		// directoryを作成せず、aliasもたどらずに祖先を検証します。dangling symlinkを、
		// 保存済みの証拠がない状態として扱ってはいけません。
		parentRel := filepath.Join(string(state), filepath.Dir(rel))
		if _, err := pathidentity.ResolveAndValidateManagedDirectory(s.RootDir.String(), parentRel); err != nil {
			return savedState, fmt.Errorf("inspect saved %s directory: %w", state, err)
		}
		path := filepath.Join(s.RootDir.String(), string(state), rel)
		file, info, err := infrafsstore.OpenRegularFile(path, os.O_RDONLY)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return savedState, fmt.Errorf("read saved %s: %w", state, err)
		}
		body, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return state, fmt.Errorf("read saved %s: %w", state, err)
		}
		if savedState != "" {
			return savedState, fmt.Errorf("%w: results are split between %s and %s", ErrTerminalRecoveryConflict, savedState, state)
		}
		savedRecords, err = decodeSpoolRecords(body)
		if err != nil {
			// 原因は包まない。保存済み結果のdecode errorはpkgspool.ErrInvalidSpoolFileに一致し、
			// processing fileをquarantineしてよいという分類になるが、競合ではすべてのfileを残す。
			return state, fmt.Errorf("%w: decode saved %s: %v", ErrTerminalRecoveryConflict, state, err)
		}
		savedState, savedPath, savedInfo = state, path, info
	}
	if savedState == "" {
		return "", nil
	}
	if len(records) == 0 || len(records) != len(savedRecords) {
		return savedState, fmt.Errorf("%w: processing has %d records, %s has %d", ErrTerminalRecoveryConflict, len(records), savedState, len(savedRecords))
	}
	for i := range records {
		if savedRecords[i].FinalizedAt.IsZero() || savedRecords[i].ResendAttemptCount < records[i].ResendAttemptCount ||
			!sameQuarantinedRecord(records[i], savedRecords[i]) {
			return savedState, fmt.Errorf("%w: record %d does not match a finalized %s record", ErrTerminalRecoveryConflict, i, savedState)
		}
	}
	// 呼び出し元が以前に読んだ内容やfile名の一致だけに頼らず、削除する前に元fileを
	// 確認し直します。
	source, sourceInfo, err := infrafsstore.OpenRegularFile(processingPath, os.O_RDONLY)
	if err != nil {
		return savedState, fmt.Errorf("verify finalized processing file: %w", err)
	}
	sourceBody, readErr := io.ReadAll(source)
	closeErr := source.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return savedState, fmt.Errorf("verify finalized processing file: %w", err)
	}
	current, err := decodeSpoolRecords(sourceBody)
	if err != nil || !sameStoredRecords(records, current) {
		return savedState, fmt.Errorf("%w: processing contents changed before cleanup", ErrTerminalRecoveryConflict)
	}
	// 前のprocessが結果をrenameした後、最後の同期を完了していない可能性があります。
	// まだ残っている元fileを削除する前に、永続化を確認します。
	if err := syncTerminalRecoveryEvidence(ctx, savedPath, savedInfo); err != nil {
		return savedState, err
	}
	if err := ctx.Err(); err != nil {
		return savedState, err
	}
	for path, before := range map[string]os.FileInfo{processingPath: sourceInfo, savedPath: savedInfo} {
		after, err := os.Lstat(path)
		if err != nil {
			return savedState, fmt.Errorf("verify terminal recovery file: %w", err)
		}
		if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
			return savedState, fmt.Errorf("%w: file changed before cleanup: %s", ErrTerminalRecoveryConflict, path)
		}
	}
	if err := s.RemoveProcessingFile(ctx, processingPath); err != nil {
		return savedState, err
	}
	return savedState, nil
}

func syncTerminalRecoveryEvidence(ctx context.Context, path string, before os.FileInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, info, err := infrafsstore.OpenRegularFile(path, os.O_RDWR)
	if err != nil {
		return fmt.Errorf("open terminal recovery file for sync: %w", err)
	}
	if !os.SameFile(before, info) || before.Size() != info.Size() || !before.ModTime().Equal(info.ModTime()) {
		return errors.Join(fmt.Errorf("%w: saved outcome changed before sync", ErrTerminalRecoveryConflict), file.Close())
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("%w: sync saved terminal outcome: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	return infrafsstore.SyncDirectory(filepath.Dir(path))
}

func sameQuarantinedRecord(original, quarantined pkgspool.SpoolRecord) bool {
	// 再送の結果と確定処理で変わるfieldは、これらだけです。残りのrecordはmarshalして
	// 比較し、新しく追加される不変のfieldも比較に含めます。
	normalize := func(record pkgspool.SpoolRecord) pkgspool.SpoolRecord {
		record.FinalizedAt = time.Time{}
		record.FirstResendAttemptedAt = time.Time{}
		record.LastResendAttemptedAt = time.Time{}
		record.ResendAttemptCount = 0
		record.LastFailureReason = ""
		return record
	}
	return sameStoredRecords([]pkgspool.SpoolRecord{normalize(original)}, []pkgspool.SpoolRecord{normalize(quarantined)})
}

func sameStoredRecords(first, second []pkgspool.SpoolRecord) bool {
	firstBody, firstErr := json.Marshal(first)
	secondBody, secondErr := json.Marshal(second)
	return firstErr == nil && secondErr == nil && bytes.Equal(firstBody, secondBody)
}
