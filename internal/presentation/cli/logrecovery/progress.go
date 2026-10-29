package logrecovery

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	infralogrecovery "github.com/go-jappe/heterolog/internal/infrastructure/logrecovery"
)

// progressOutputは復旧処理と保存再試行から届く進捗を直列に表示します。
// 最終結果の標準出力と分けるため、writerには標準error出力を渡します。
type progressOutput struct {
	mu     sync.Mutex
	writer io.Writer
	err    error
}

func (p *progressOutput) report(progress infralogrecovery.Progress) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	var text bytes.Buffer
	fmt.Fprintf(&text, "[recovery] %s", progressPhaseDescription(progress.Phase))
	if progress.Scope != "" {
		fmt.Fprintf(&text, " scope=%s", progress.Scope)
	}
	if progress.Attempt > 0 {
		fmt.Fprintf(&text, " attempt=%d", progress.Attempt)
	}
	if !progress.NextRetryAt.IsZero() {
		wait := max(time.Until(progress.NextRetryAt), 0).Round(time.Millisecond)
		fmt.Fprintf(&text, " retry-in=%s next-retry-at=%s", wait, progress.NextRetryAt.Format(time.RFC3339))
	}
	fmt.Fprintln(&text)
	if directory := progress.Directory; directory.DirectoryPath != "" {
		fmt.Fprintf(&text, "  %q: scanned=%d compressed=%d accepted=%d removed=%d recovered-delete-pending=%d failed=%d\n",
			directory.DirectoryPath, directory.ScannedFiles, len(directory.CompressedFiles),
			len(directory.AcceptedFiles), len(directory.RemovedFiles),
			len(directory.RecoveredDeletePendingFiles), len(directory.FailedFiles))
		for _, failure := range directory.FailedFiles {
			fmt.Fprintf(&text, "  Failed %q: %s\n", failure.Path, failure.Error)
		}
		if directory.Error != "" {
			fmt.Fprintf(&text, "  Error: %s\n", directory.Error)
		}
	}
	if progress.Err != nil {
		fmt.Fprintf(&text, "  Error: %v\n", progress.Err)
	}
	_, p.err = io.Copy(p.writer, &text)
}

func (p *progressOutput) result() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func progressPhaseDescription(phase infralogrecovery.ProgressPhase) string {
	switch phase {
	case infralogrecovery.ProgressPreparing:
		return "Preparing configuration and storage directories"
	case infralogrecovery.ProgressRunning:
		return "Processing"
	case infralogrecovery.ProgressRetryWaiting:
		return "Waiting to retry failed operations"
	case infralogrecovery.ProgressStopping:
		return "Shutdown requested. Waiting for accepted recovery operations and checkpoint saves to complete. Press Ctrl+C again to force shutdown."
	case infralogrecovery.ProgressForced:
		return "Forced shutdown requested. Waiting for in-progress OS I/O to return, if any."
	case infralogrecovery.ProgressComplete:
		return "Completed"
	default:
		return string(phase)
	}
}
