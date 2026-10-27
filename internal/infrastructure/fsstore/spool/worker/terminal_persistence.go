package worker

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrTerminalSpoolConflictは、既存のterminal fileを今回の保存に再利用できないことを
// 表します。確認のため、既存の結果とprocessingの元fileの両方を保全します。
var ErrTerminalSpoolConflict = errors.New("terminal spool file conflicts with requested records")

// terminalSpoolFileModeは、terminal spool fileのUnix modeです。os.CreateTempが使う
// 所有者だけの0600を維持し、recordを他者へ公開しません。
const terminalSpoolFileMode = 0o600

// writeSpoolFileAtPathは、processing fileの相対名のままterminal stateへ保存します。
// recordを書き込む前に一時fileの権限を制限します。Unixではos.CreateTempの所有者だけの
// modeを維持し、Windowsでは現在のuser・SYSTEM・Administratorsだけを許可します。
// データと権限を同期してから公開し、呼び出し元が元fileを処分できるようにする前に
// directoryを同期します。directory同期のerrorでも公開済みのfileは残り、再試行では
// 上書きせずに検証して同期します。
func writeSpoolFileAtPath(rootDirectory, finalPath string, body []byte) error {
	if filepath.Ext(finalPath) != spooldir.SpoolExt {
		return fmt.Errorf("path is not a spool file: %s", finalPath)
	}
	dir := filepath.Dir(finalPath)
	if err := spooldir.EnsureDirectoryNoSymlinks(rootDirectory, dir); err != nil {
		return fmt.Errorf("failed to create destination dir: %w", err)
	}
	if found, err := reuseTerminalSpoolFile(finalPath, body); found || err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(finalPath)+".*"+tmpFileSuffix)
	if err != nil {
		return fmt.Errorf("failed to create temp spool file: %w", err)
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if err := infrafsstore.SetPrivateFilePermissions(f, terminalSpoolFileMode); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to set spool file permissions: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to write spool file body: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("%w: sync terminal spool file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close spool file: %w", err)
	}
	if err := infrafsstore.RenameFileNoReplace(tmpPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			if found, reuseErr := reuseTerminalSpoolFile(finalPath, body); found || reuseErr != nil {
				return reuseErr
			}
		}
		return fmt.Errorf("failed to publish terminal spool file: %w", err)
	}
	return infrafsstore.SyncDirectory(dir)
}

func reuseTerminalSpoolFile(path string, requestedBody []byte) (bool, error) {
	file, before, err := infrafsstore.OpenRegularFile(path, os.O_RDWR)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("inspect existing terminal spool file: %w", err)
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil {
		return true, fmt.Errorf("read existing terminal spool file: %w", err)
	}
	if !sameTerminalSpoolRecords(body, requestedBody) {
		return true, fmt.Errorf("%w: %s", ErrTerminalSpoolConflict, path)
	}
	if err := file.Sync(); err != nil {
		return true, fmt.Errorf("%w: sync existing terminal spool file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	if err := file.Close(); err != nil {
		return true, fmt.Errorf("close existing terminal spool file: %w", err)
	}
	after, err := os.Lstat(path)
	if err != nil {
		return true, err
	}
	if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return true, fmt.Errorf("%w: file changed while verifying %s", ErrTerminalSpoolConflict, path)
	}
	return true, infrafsstore.SyncDirectory(filepath.Dir(path))
}

func sameTerminalSpoolRecords(storedBody, requestedBody []byte) bool {
	stored, storedErr := decodeSpoolRecords(storedBody)
	requested, requestedErr := decodeSpoolRecords(requestedBody)
	if storedErr != nil || requestedErr != nil || len(stored) != len(requested) {
		return false
	}
	for i := range stored {
		if stored[i].FinalizedAt.IsZero() {
			return false
		}
		// 確定時刻には、最初に公開した結果のものを使います。再試行では新しい時刻を渡しても
		// かまいませんが、試行回数や失敗理由を含む他のfieldはすべて一致しなければなりません。
		// 同じpathで異なる結果を統合しません。
		requested[i].FinalizedAt = stored[i].FinalizedAt
	}
	return sameStoredRecords(stored, requested)
}
