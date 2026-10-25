package spool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// directoryModeは、spool rootと配下のdirectoryのUnix権限です。spool fileと同じく
// 所有者以外へ公開せず、umaskによる追加の制限は維持します。
const directoryMode os.FileMode = 0o700

// ancestorDirectoryModeは、spool rootより上位で不足するdirectoryを作成するUnix権限です。
// <SpoolPath>・<Target>・共有のspool directoryは、group読取を許すobject storageのspoolなど
// 他のstoreも通るため、以前と同じ0755とし、umaskを適用します。
const ancestorDirectoryMode os.FileMode = 0o755

// EnsureDirectoryNoSymlinksは解決済みspool root配下の書込先を用意します。
// stateや日時別ディレクトリの初使用・再作成で既存linkを辿らないよう、対象pathの祖先だけを検査します。
// directory全体の走査や、稼働中の外部からのpath置換への追従は行いません。
// spool root自身と配下で不足するdirectoryはdirectoryModeで、rootより上位で不足するdirectoryは
// ancestorDirectoryModeで作成します。Windowsでは上位を含め、以前と同じく実行ユーザー・SYSTEM・
// Administratorsだけを許可するACLで作成し、新しいspool fileは作成時からこのACLを継承します。
// 以前の版が広い権限で作った既存のdirectoryも、Unix系ではspool root自身から書込先までの
// group・otherの権限を外します。rootより上位の既存directoryと、WindowsのACLは変更しません。
// 作成したdirectoryの親を同期できなかった場合は、その親の同期を確認できるまで、
// 依存する書込先の準備は作成済みでもpkgfsstore.ErrDurabilityUnconfirmedを返します。
func EnsureDirectoryNoSymlinks(rootDirectory, directory string) error {
	release, err := lockDirectoryPreparation(context.Background(), rootDirectory)
	if err != nil {
		return err
	}
	defer release()
	return ensureDirectoryNoSymlinksLocked(rootDirectory, directory)
}

// 呼び出し元は同じrootのdirectory preparation lockを保持します。
// 作成したdirectoryの親を同期できなかった準備は、その親の同期が成功するまで失敗のままです。
// 記録はinfrafsstoreがprocess全体で保持し、別のStoreからの準備でも、その親に依存する
// partitionの作成や既存directoryの利用より先に同期を再試行します。同期できた記録は解除します。
func ensureDirectoryNoSymlinksLocked(rootDirectory, directory string) error {
	relative, err := filepath.Rel(rootDirectory, directory)
	if err != nil {
		return fmt.Errorf("resolve spool directory %q within root: %w", directory, err)
	}
	resolved, err := pathidentity.ResolveAndValidateManagedDirectory(rootDirectory, relative)
	if err != nil {
		return err
	}
	// 解決済みの書込先からrelativeの階層だけ上位へたどり、権限を狭めてよい範囲を
	// root自身と配下に限ります。rootDirectoryのaliasを再解決しません。
	root := resolved
	for range strings.Split(filepath.Clean(relative), string(filepath.Separator)) {
		root = filepath.Dir(root)
	}
	return infrafsstore.EnsureManagedDirectoryNoSymlinks(root, resolved, directoryMode, ancestorDirectoryMode)
}
