package fsarchivehandler

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

// runInlineArchivePurgeIfNeededは、送信の再試行を有効にし、除外する元ログを指定せずに
// cleanup本体を呼ぶ試験用の入口です。
func (f *FsArchiveHandler) runInlineArchivePurgeIfNeeded(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
) error {
	return f.runInlineArchivePurgeWithOptions(ctx, failureCategory, true, "")
}
