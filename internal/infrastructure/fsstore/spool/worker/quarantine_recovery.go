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

// ErrTerminalRecoveryConflict means saved terminal evidence cannot be reconciled
// with the whole processing file. All files must remain for inspection.
var ErrTerminalRecoveryConflict = errors.New("processing file conflicts with saved terminal outcome")

// ErrQuarantineRecoveryConflict retains the original quarantine error identity.
var ErrQuarantineRecoveryConflict = ErrTerminalRecoveryConflict

// ReconcileProcessingQuarantine retains the original boolean entry point while
// applying the same recovery rules to every terminal state.
func (s *StateStore) ReconcileProcessingQuarantine(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) (bool, error) {
	state, err := s.ReconcileProcessingTerminal(ctx, processingPath, records)
	return state != "", err
}

// ReconcileProcessingTerminal recovers a completed done, dead or quarantine write
// whose processing-file removal was interrupted. Only a single state's complete,
// ordered match is sufficient evidence. Partial or mixed results are preserved
// for inspection rather than inferring the lost redistribution plan.
// An empty state with no error means ordinary processing may continue. A nonempty
// state with no error means the saved result is synced and its source removed.
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
		// Validate ancestors without creating directories or following aliases.
		// A dangling symlink must not count as absent saved evidence.
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
	// Recheck the original before deleting it, rather than relying on a caller's
	// earlier read or on a matching filename alone.
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
	// A previous process may have renamed the result without completing its
	// final sync. Confirm durability before removing the still-available source.
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
	// These are the only fields changed by the resend outcome and finalization.
	// Marshal the remaining record so new immutable fields participate as well.
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
