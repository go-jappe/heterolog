// Package checkpointは、ログファイルの宛先登録・送信完了記録と、そのファイル保存・再試行を管理します。
// inlineとrunnerは同じStoreと保存形式を使用します。
package checkpoint

import (
	"context"
	"fmt"
	"os"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// WriteAtomicallyは、用意済みのdirectoryに一時ファイルを書き、同期してから
// finalPathへrenameし、最後にdirectoryを同期します。入力の検証・JSONへの変換は
// 呼び出し元が行います。ファイルの権限はcheckpoint共通の0600です。
// publishedはrenameが完了したかを表します。trueでもdirectory同期のエラーを返す
// 場合があり、宛先登録などの呼び出し元はその状態を区別して扱います。
func WriteAtomically(ctx context.Context, directory, finalPath string, payload []byte) (published bool, err error) {
	return writeAtomically(ctx, directory, finalPath, payload, infrafsstore.SyncDirectory)
}

// writeAtomicallyは同じStoreのmanifest・cursor保存でdirectory同期の実装を共有します。
func writeAtomically(ctx context.Context, directory, finalPath string, payload []byte, syncDirectory func(string) error) (published bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	temporary, err := os.CreateTemp(directory, ".checkpoint-*.tmp")
	if err != nil {
		return false, fmt.Errorf("create checkpoint temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen := true
	defer func() {
		if temporaryOpen {
			_ = temporary.Close()
		}
		if !published {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := infrafsstore.SetPrivateFilePermissions(temporary, 0o600); err != nil {
		return false, fmt.Errorf("set checkpoint file permissions: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		return false, fmt.Errorf("write checkpoint temp file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return false, fmt.Errorf("%w: sync checkpoint temp file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	if err := temporary.Close(); err != nil {
		temporaryOpen = false
		return false, fmt.Errorf("close checkpoint temp file: %w", err)
	}
	temporaryOpen = false
	// rename前の中止では既存ファイルを維持します。rename後は中止要求によらず
	// directory同期まで進め、公開済みのファイルを未公開として扱いません。
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return false, fmt.Errorf("publish checkpoint: %w", err)
	}
	published = true
	if err := syncDirectory(directory); err != nil {
		return true, fmt.Errorf("sync checkpoint directory: %w", err)
	}
	return true, nil
}
