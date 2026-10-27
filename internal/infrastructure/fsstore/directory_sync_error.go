package fsstore

import (
	"fmt"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// DirectorySyncErrorは、作成したdirectoryの親のうち、永続化を確認できていないものを
// 示します。EnsureDirectoryとその派生関数は、processが続く間この親を記録し、その親
// 自身または配下のdirectoryを準備する前に親の同期を再試行します。そのため、同期が
// 成功するまでは、作成したdirectoryがすでに存在していても、後の呼び出しはこのerrorを
// 返し続けます。再試行で親自体が存在しないと分かった場合は、確認すべき作成結果が
// 残っていないため、同期の成功と同じく記録を解除します。
type DirectorySyncError struct {
	Directory string
	Err       error
}

func (e *DirectorySyncError) Error() string {
	return fmt.Sprintf("%v: sync newly created directory parent %q: %v",
		pkgfsstore.ErrDurabilityUnconfirmed, e.Directory, e.Err)
}

func (e *DirectorySyncError) Unwrap() []error {
	return []error{pkgfsstore.ErrDurabilityUnconfirmed, e.Err}
}
