package fsstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

type directorySetupPolicy struct {
	rejectSymlinks bool
	// setDestinationPermissionsは、Unix系で保存先自身にmodeを設定します。
	// 既存の場合も適用します。新規作成時の権限設定はこの指定と無関係に行います。
	setDestinationPermissions bool
	// restrictWithinは、アプリが管理し権限を狭めてよいdirectoryです。空でなければ、
	// その自身から保存先までの既存ディレクトリから、Unix系でmodeを超える権限を外します。
	// 上位の検査結果を使うため、rejectSymlinksと併用します。
	restrictWithin string
	// ancestorModeは、restrictWithinより上位で不足するディレクトリをUnix系で作成するmodeです。
	// 0ならmodeを使います。Windowsの作成時のACLはこの指定と無関係です。
	ancestorMode os.FileMode
}

// EnsureDirectoryは、検証済みの保存先ディレクトリを用意します。
// 不足するディレクトリを権限付きで作成し、その親を同期します。作成に失敗し、何も作成されて
// いないことを確認できた場合は、親が変わっていないため同期せず、作成のエラーだけを返します。
// アプリが権限を変更してよい保存先を指定してください。
// 失敗はエラーで返し、作成済みディレクトリは残します。
// 作成したディレクトリの親を同期できなかった場合はprocess内で記録し、その親が保存先自身か
// 上位にある準備では、検査・作成より先に同期を再試行します。成功するまでは、保存先が既存でも
// pkgfsstore.ErrDurabilityUnconfirmedを含むDirectorySyncErrorを返し、その下へ新しい
// ディレクトリも作成しません。呼び出し元のStoreやリンクの検査方針が異なっても記録を共有し、
// 同期できた記録は解除します。記録のない既存の親は同期しません。記録数が上限に達している
// 間は、記録のない親への新しい作成もpkgfsstore.ErrDurabilityUnconfirmedで拒否します。
// 入力検証・エラー通知・ファイル本体の保存と同期は呼び出し側の責務です。
// Unix系では既存の保存先自身にもmodeを設定します。Windowsでは作成時にACLを設定し、
// 作成前に保存先filesystemのACL対応を確認します。既存ACLは変更せず、保存先自身が
// リンクでないことを検査します。
// 同じ保存先を共有するStoreは、呼び出し元が稼働前に順番に初期化してください。
// この関数は呼び出し同士を排他せず、別の保存先の初期化完了も待ちません。
// 別の呼び出しが作成中か同期に失敗した上位の親は、成功を返す前にこの呼び出しでも同期します。
func EnsureDirectory(directory string, mode os.FileMode) error {
	return ensureDirectoryWithPolicy(context.Background(), directory, mode, directorySetupPolicy{
		setDestinationPermissions: true,
	}, SyncDirectory)
}

// EnsureDirectoryNoSymlinksは、保存先自身からルートまでのパスを検査し、
// シンボリックリンクを拒否してから、不足するディレクトリを作成します。
// 保存先が既存の場合も上位のパスを検査します。外部から同時にパスを置換する操作への対策ではありません。
// directoryの入力検証を呼び出し元が行うという前提、親の同期範囲、エラーの返却と
// 同期失敗を記録して再試行する扱いはEnsureDirectoryと同じです。
//
// 権限は新規作成時だけ設定し、既存ディレクトリの権限は変更しません。
func EnsureDirectoryNoSymlinks(directory string, mode os.FileMode) error {
	return EnsureDirectoryNoSymlinksContext(context.Background(), directory, mode)
}

// EnsureDirectoryNoSymlinksContextは、記録済みの同期失敗の再試行、各directoryの
// 検査・作成と最後の権限設定へ進む前にキャンセルを確認します。作成を試みたdirectoryの
// 親の同期はキャンセル後も行います。実行中のOS I/Oを強制中断するものではありません。
func EnsureDirectoryNoSymlinksContext(ctx context.Context, directory string, mode os.FileMode) error {
	return ensureDirectoryWithPolicy(ctx, directory, mode, directorySetupPolicy{
		rejectSymlinks: true,
	}, SyncDirectory)
}

// EnsureManagedDirectoryNoSymlinksは、EnsureDirectoryNoSymlinksと同じくリンクを拒否して
// 不足するディレクトリを作成します。managedRoot自身と配下はmodeで、managedRootより上位は
// ancestorModeで作成し、どちらもUnix系ではprocess umaskを適用します。上位は他のStoreと
// 共有し得るため、管理下と別の権限を指定できます。加えて、Unix系ではmanagedRoot自身から
// 保存先までの既存ディレクトリにmodeを超える権限があれば、その権限だけを外します。
// 以前の版や外部操作で広くなったアプリ管理のディレクトリを狭めるためで、権限を広げることは
// ありません。managedRootには、保存先自身かその上位にある、アプリが所有し権限を変更してよい
// ディレクトリを指定してください。managedRootより上位の既存ディレクトリは変更しません。
// 権限は新規作成より先に上位から順に狭め、変更できなければエラーを返します。
// Windowsでは上位を含め、作成時に実行ユーザー・SYSTEM・Administratorsだけを許可するACLを
// 設定し、既存ディレクトリのACLは変更しません。
func EnsureManagedDirectoryNoSymlinks(managedRoot, directory string, mode, ancestorMode os.FileMode) error {
	if strings.TrimSpace(managedRoot) == "" {
		return errors.New("managed directory root must not be empty")
	}
	return ensureDirectoryWithPolicy(context.Background(), directory, mode, directorySetupPolicy{
		rejectSymlinks: true,
		restrictWithin: managedRoot,
		ancestorMode:   ancestorMode,
	}, SyncDirectory)
}

func ensureDirectoryWithPolicy(ctx context.Context, directory string, mode os.FileMode, policy directorySetupPolicy, syncDirectory func(string) error) (err error) {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return fmt.Errorf("resolve directory %q: %w", directory, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// chainは保存先からルートまでのパスと比較キーです。下位から順に並び、
	// 同期失敗の記録との照合と、作成するディレクトリの親の特定に使います。
	chain := directoryPathKeys(absolute)
	// restrictedは、既存ディレクトリの権限を狭めてよい範囲の上端のchain上の位置です。
	// 範囲がなければ-1です。
	restricted, err := restrictedDirectoryIndex(chain, policy.restrictWithin)
	if err != nil {
		return err
	}
	// 以前の呼び出しで同期できなかった親が保存先自身か上位にあれば、検査・作成より先に
	// 再試行します。同期できるまでは既存の保存先でも成功を返さず、その下へ作成もしません。
	if err := directorySyncs.retryFailed(chain, syncDirectory); err != nil {
		return err
	}
	// createdは、この呼び出しで作成を試みるディレクトリの親を、上位から順に保持します。
	// 途中で作成・権限設定に失敗しても、作成済みの変更は残るため、deferで親の同期を試みます。
	// 一つの同期がエラーになっても残りは試し、
	// errors.Joinで、元の処理エラーと各同期エラーの両方を返します。
	// 同期できなかった親は記録し、依存する次の準備で再試行させます。作成されていないと
	// 確認できたディレクトリの親は変更されていないため、同期も記録もしません。
	// 準備に成功した場合は、別の呼び出しが作成中か同期に失敗した上位の親も同期してから返します。
	// 作成が不要で、そのような親もなければ同期しません。
	var created []directoryPathKey
	defer func() {
		err = errors.Join(err, directorySyncs.syncCreated(chain, created, err == nil, syncDirectory))
	}()

	// 保存先から上位へ調べ、存在しないディレクトリのchain上の位置を集めます。
	// 作成時にはこの逆順でたどります。
	var missing []int
	// broadは、管理範囲内でmodeを超える権限を持つ既存ディレクトリのchain上の位置と情報です。
	var broad []inspectedDirectory
	for index, current := range chain {
		if err := ctx.Err(); err != nil {
			return err
		}
		root := index == len(chain)-1
		info, err := inspectDirectory(current.path, policy.rejectSymlinks)
		if err == nil {
			if !info.IsDir() || (policy.rejectSymlinks && info.Mode()&os.ModeSymlink != 0) {
				return &DirectoryObstructionError{Path: current.path}
			}
			if index <= restricted && directoryPermissionsExceed(info, mode) {
				broad = append(broad, inspectedDirectory{index: index, info: info})
			}
			// 通常は既存ディレクトリに到達したら終了します。シンボリックリンクを拒否する
			// 場合は、保存先が既存でもルートまで調べ、上位にリンクがないことを確認します。
			if !policy.rejectSymlinks || root {
				break
			}
		} else if !errors.Is(err, os.ErrNotExist) || root {
			return fmt.Errorf("inspect directory %q: %w", current.path, err)
		} else {
			missing = append(missing, index)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 管理範囲内の既存ディレクトリは、新しいディレクトリを作る前に上位から順に狭めます。
	for index := len(broad) - 1; index >= 0; index-- {
		path := chain[broad[index].index].path
		if err := restrictDirectoryPermissions(path, broad[index].info, mode); err != nil {
			return fmt.Errorf("restrict directory %q permissions: %w", path, err)
		}
	}
	// 未作成なら、最も近い既存の親が属するfilesystemを作成前に確認します。
	// WindowsでACLを保持できない保存先へ、制限のないディレクトリを作らないためです。
	existingDirectory := absolute
	if len(missing) > 0 {
		existingDirectory = chain[missing[len(missing)-1]+1].path
	}
	if err := validateDirectorySetupFilesystem(existingDirectory); err != nil {
		return err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		path, parent := chain[missing[index]].path, chain[missing[index]+1]
		// これから作成を試みるディレクトリの親を、作成前に作成中として登録します。
		// 作成や作成後の検査が失敗した場合も、作成された可能性があればこの親を同期します。
		if err := directorySyncs.beginCreation(parent); err != nil {
			return err
		}
		created = append(created, parent)
		// Unix系はmodeとumask、Windowsは実行ユーザー・SYSTEM・Administratorsに
		// アクセスを限定するACLで作成します。既存の上位ディレクトリの権限は変更しません。
		// 管理範囲より上位を作る場合は、Unix系でancestorModeを使います。
		createMode := mode
		if restricted >= 0 && missing[index] > restricted && policy.ancestorMode != 0 {
			createMode = policy.ancestorMode
		}
		if err := CreatePrivateDirectory(path, createMode); err != nil {
			info, statErr := os.Lstat(path)
			if errors.Is(statErr, os.ErrNotExist) {
				// 作成されていなければ、この呼び出しは親を変更していません。不要な同期も、
				// 永続化を確認できないという報告も行わず、mkdirのエラーだけを返します。
				directorySyncs.cancelCreation(parent)
				created = created[:len(created)-1]
			}
			// 他の作成処理が先に用意した場合は、リンクでないディレクトリだけを許可します。
			if !errors.Is(err, os.ErrExist) || statErr != nil || !info.IsDir() {
				return fmt.Errorf("create directory %q: %w", path, err)
			}
		}
		info, err := inspectDirectory(path, policy.rejectSymlinks)
		if err != nil {
			return fmt.Errorf("inspect created directory %q: %w", path, err)
		}
		if !info.IsDir() || (policy.rejectSymlinks && info.Mode()&os.ModeSymlink != 0) {
			return &DirectoryObstructionError{Path: path, Created: true}
		}
	}
	// OS別の権限設定・最終検査も、deferによる親の同期より先に行います。
	if err := ctx.Err(); err != nil {
		return err
	}
	return finishDirectorySetup(absolute, mode, policy.setDestinationPermissions)
}

// DirectoryObstructionErrorは、用意するディレクトリのpathにディレクトリ以外のエントリがあるか、
// リンクを拒否する準備でリンクがあることを表します。Pathは拒否したpathで、呼び出し元は
// errors.Asで拒否の原因になったpathを判別できます。作成直後の検査で見つかった場合は
// Createdがtrueです。
type DirectoryObstructionError struct {
	Path    string
	Created bool
	// messageは、呼び出し箇所ごとの従来の文言です。空なら既定の文言を使います。
	message string
}

func (e *DirectoryObstructionError) Error() string {
	if e.message != "" {
		return e.message
	}
	if e.Created {
		return fmt.Sprintf("created directory path %q is not a directory", e.Path)
	}
	return fmt.Sprintf("directory path %q is not a directory", e.Path)
}

// inspectedDirectoryは、検査で見つけた既存ディレクトリのchain上の位置と情報です。
type inspectedDirectory struct {
	index int
	info  os.FileInfo
}

// restrictedDirectoryIndexは、managedRootのchain上の位置を返します。managedRootが空なら-1です。
// managedRootは保存先自身か上位でなければなりません。リンクは解決せず、表記の違いは
// pathidentity.ComparisonKeyで比べます。
func restrictedDirectoryIndex(chain []directoryPathKey, managedRoot string) (int, error) {
	if managedRoot == "" {
		return -1, nil
	}
	absolute, err := filepath.Abs(managedRoot)
	if err != nil {
		return -1, fmt.Errorf("resolve managed directory root %q: %w", managedRoot, err)
	}
	key := pathidentity.ComparisonKey(absolute)
	for index, current := range chain {
		if current.key == key {
			return index, nil
		}
	}
	return -1, fmt.Errorf("directory %q is not within managed directory root %q", chain[0].path, absolute)
}

// inspectDirectoryは、パス末尾がシンボリックリンクの場合、rejectSymlinksがtrueなら
// Lstatでリンク自体を、falseならStatでリンク先を調べます。上位のパスの検査とリンクを拒否する判定は、
// 呼び出し側が行います。
func inspectDirectory(path string, rejectSymlinks bool) (os.FileInfo, error) {
	if rejectSymlinks {
		return os.Lstat(path)
	}
	return os.Stat(path)
}
