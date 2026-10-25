package appendlog

import (
	"context"
	"errors"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var ErrNilDirectoryGuardToken = errors.New("directory guard token is nil")

// DirectoryGuardTokenは、同じdirectory内でpathを保護する物理writerまたは
// lifecycle ownerを識別します。一つのownerは複数taskでtokenを共有できますが、
// 同じtoken・pathの保護はset上の一件です。同じpathを独立した並行ownerが
// 保護する場合は、ownerごとに異なるtokenを使用します。
// tokenのownerはtokenを生成してfieldに保持するinstanceであり、DirectoryGuardは
// tokenを所有せず、owner別状態を引くkeyとしてだけ使用します。
type DirectoryGuardToken struct{ _ byte }

func NewDirectoryGuardToken() *DirectoryGuardToken {
	return &DirectoryGuardToken{}
}

// guardedPathsは、一つのDirectoryGuardTokenが同じdirectory内で保護している
// active pathとretained pathを管理します。retained pathとは、activeではないものの、
// 圧縮・object storage uploadなどの処理中なのでpurgeから一時的に保護するfile pathです。
type guardedPaths struct {
	// activeは、tokenのownerが現在openしているfile pathです。rotationで旧handleを
	// 閉じた後も、新fileのopen成功またはownerのCloseまでは旧pathを保護します。
	active string
	// retainedは、圧縮・uploadなどの処理中にpurgeしてはいけない
	// identity keyから元表記のpathへのmapです。同じtoken・pathの別表記も一件として扱います。
	retained map[string]string
}

func (s *guardedPaths) retain(path string) {
	key := pathidentity.ComparisonKey(path)
	if _, exists := s.retained[key]; !exists {
		s.retained[key] = path
	}
}

// retainPathsLockedは、操作用パスを整形し、空でないパスだけを保護状態へ登録します。
// 呼び出し元は、この状態を持つDirectoryGuardのstatesMuのwrite lockを保持してください。
func (s *guardedPaths) retainPathsLocked(paths []string) {
	for _, path := range paths {
		if path = pathidentity.AbsoluteOrCleanPath(path); path != "" {
			s.retain(path)
		}
	}
}

// DirectoryGuardは、同じdirectoryを使うwriterとlifecycle処理の保護情報をprocess内で共有します。
// lifecycle lockはcleanup・圧縮・archive・purgeを直列化し、states lockはactive・retainedの登録を保護します。
// RunPurgeWithProtectionは保護一覧の取得と削除候補の登録後にstates lockを解放します。
// そのため、purgeの実削除と別世代へのactive切替は並行できます。
type DirectoryGuard struct {
	// lifecycleMuは、同じdirectoryを対象とするcleanup、圧縮、archive、purgeを直列化します。
	lifecycleMu fsstore.ContextMutex
	// statesMuは、statesByTokenと各guardedPathsのactive・retainedを保護します。
	// WithProtectedPaths等はcallback完了まで保持してactive切替も止めます。
	// RunPurgeWithProtectionはsnapshotと候補の保護登録後に解放し、
	// 追加保護の判定・purgeを別世代のactive切替と並行して実行します。
	statesMu sync.RWMutex
	// statesByTokenは、同じdirectoryを使用するwriterまたはlifecycle処理ごとの保護状態を
	// token単位で保持します。
	// 値がactive pathもretained pathも持たなくなった時点で、そのtokenの要素を削除します。
	statesByToken map[*DirectoryGuardToken]*guardedPaths
}

// RunWithLifecycleLockは、operationを同じdirectoryの他のlifecycle関連処理と
// 同時実行させないため、lifecycleMuを保持して実行します。
// 要求の重複排除や統合は行いません。
func (g *DirectoryGuard) RunWithLifecycleLock(operation func() error) error {
	g.lifecycleMu.Lock()
	defer g.lifecycleMu.Unlock()
	return operation()
}

// RunWithLifecycleLockContextは、lifecycleの排他待ちをctxで中止できる版です。
// operation開始後の中止確認はoperation側が行います。取得失敗時は実行しません。
func (g *DirectoryGuard) RunWithLifecycleLockContext(ctx context.Context, operation func() error) error {
	if err := g.lifecycleMu.LockContext(ctx); err != nil {
		return err
	}
	defer g.lifecycleMu.Unlock()
	return operation()
}

// TransitionActiveは、pathの解決・openとactive登録の間、保護一覧の取得・変更を排他します。
// transition失敗時は既存登録を変更しません。
// RunPurgeWithProtectionの実削除とは並行するため、呼び出し側で削除候補を再openしないようにしてください。
func (g *DirectoryGuard) TransitionActive(
	token *DirectoryGuardToken,
	transition func() (string, error),
) error {
	if token == nil {
		return ErrNilDirectoryGuardToken
	}
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	path, err := transition()
	if err != nil {
		return err
	}
	g.stateForTokenLocked(token).active = pathidentity.AbsoluteOrCleanPath(path)
	return nil
}

// ProtectPathsは圧縮・upload中のold pathなどをactiveとは別にretainします。
// 同じtoken・pathへの重複呼び出しは冪等で、UnprotectPathsは一回だけ必要です。
// 同じpathを複数の並行ownerが独立して保護する場合は、異なるtokenを使用します。
func (g *DirectoryGuard) ProtectPaths(token *DirectoryGuardToken, paths ...string) {
	if token == nil {
		return
	}
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	state := g.stateForTokenLocked(token)
	state.retainPathsLocked(paths)
}

// ProtectPathsIfNoOtherOwnerConflictは、pathsのいずれかを別の処理がactiveまたは
// retainedとして保護中ならfalseを返します。競合がなければ全pathをtokenへ追加します。
// 判定から登録までstates lockを保持し、falseの場合は既存の保護も変更しません。
// 自分のtokenは判定から除外します。excludedOwnerはrotation時に自分のwriterから
// taskへ保護を引き継ぐ場合だけ指定し、そのwriterの保護も除外します。通常はnilです。
func (g *DirectoryGuard) ProtectPathsIfNoOtherOwnerConflict(
	token *DirectoryGuardToken,
	excludedOwner *DirectoryGuardToken,
	paths []string,
) bool {
	if token == nil {
		return false
	}
	requestedPaths := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if key := pathidentity.ComparisonKey(path); key != "" {
			requestedPaths[key] = struct{}{}
		}
	}
	g.statesMu.Lock()
	defer g.statesMu.Unlock()

	for ownerToken, state := range g.statesByToken {
		if ownerToken == token || ownerToken == excludedOwner {
			continue
		}
		if _, conflicts := requestedPaths[pathidentity.ComparisonKey(state.active)]; conflicts {
			return false
		}
		for key := range state.retained {
			if _, conflicts := requestedPaths[key]; conflicts {
				return false
			}
		}
	}

	state := g.stateForTokenLocked(token)
	state.retainPathsLocked(paths)
	return true
}

// UnprotectPathsはtokenがretainしているpathをsetから取り除きます。
// 未登録pathの指定は無視します。
func (g *DirectoryGuard) UnprotectPaths(token *DirectoryGuardToken, paths ...string) {
	if token == nil {
		return
	}
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	state := g.statesByToken[token]
	if state == nil {
		return
	}
	for _, path := range paths {
		delete(state.retained, pathidentity.ComparisonKey(path))
	}
	g.deleteEmptyStateLocked(token, state)
}

// RemoveTokenはclose済みのactive pathだけを解除します。進行中archiveのretained pathは、
// archive処理がUnprotectPathsするまで維持します。
func (g *DirectoryGuard) RemoveToken(token *DirectoryGuardToken) {
	if token == nil {
		return
	}
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	state := g.statesByToken[token]
	if state == nil {
		return
	}
	state.active = ""
	g.deleteEmptyStateLocked(token, state)
}

// WithProtectedPathsはprotected判定からfile削除完了までactive transitionを止めます。
// snapshotは元表記のpathを保持します。別表記との照合にはpathidentity.Containsを使います。
// callback内ではfile操作だけを行い、同じguardへ再入しないでください。
func (g *DirectoryGuard) WithProtectedPaths(operation func(map[string]struct{})) {
	g.statesMu.RLock()
	defer g.statesMu.RUnlock()

	protectedPaths, _ := g.protectedAndActivePathsLocked()
	operation(protectedPaths)
}

// WithProtectedPathsAndRetainはprotected pathの確認とlocal file操作を排他中に行い、
// callbackが返したpathをlock解除前にretainします。remote uploadはcallbackの外で行い、
// 完了後にUnprotectPathsを呼び出してください。callback内から同じguardへ再入はできません。
func (g *DirectoryGuard) WithProtectedPathsAndRetain(
	token *DirectoryGuardToken,
	operation func(map[string]struct{}) []string,
) error {
	if token == nil {
		return ErrNilDirectoryGuardToken
	}

	g.statesMu.Lock()
	defer g.statesMu.Unlock()

	protectedPaths, _ := g.protectedAndActivePathsLocked()
	paths := operation(protectedPaths)
	if len(paths) == 0 {
		return nil
	}
	state := g.stateForTokenLocked(token)
	state.retainPathsLocked(paths)
	return nil
}

// protectedAndActivePathsLockedは、全ownerの状態から削除禁止パスと書き込み先の一覧を作ります。
// 同じファイルは一件にまとめ、削除禁止一覧には一時保護中のパスも含めます。
// 書き込み先は一度だけ集計し、そのパス表記を削除禁止一覧でも優先して使います。
// 返す二つのmapは、互いにもGuardの内部状態にも影響せず変更できます。
// 呼び出し元はg.statesMuのread lockまたはwrite lockを保持してください。
func (g *DirectoryGuard) protectedAndActivePathsLocked() (protectedPaths, activePaths map[string]struct{}) {
	protectedByIdentity := make(map[string]string, len(g.statesByToken)*2)
	activeByIdentity := make(map[string]string, len(g.statesByToken))
	for _, state := range g.statesByToken {
		if state.active != "" {
			key := pathidentity.ComparisonKey(state.active)
			if previous, exists := activeByIdentity[key]; !exists || state.active < previous {
				activeByIdentity[key] = state.active
			}
		}
		for key, path := range state.retained {
			if previous, exists := protectedByIdentity[key]; !exists || path < previous {
				protectedByIdentity[key] = path
			}
		}
	}
	for key, path := range activeByIdentity {
		protectedByIdentity[key] = path
	}
	return originalPathSet(protectedByIdentity), originalPathSet(activeByIdentity)
}

func originalPathSet(byIdentity map[string]string) map[string]struct{} {
	paths := make(map[string]struct{}, len(byIdentity))
	for _, path := range byIdentity {
		paths[path] = struct{}{}
	}
	return paths
}

// stateForTokenLockedはtokenに対応するownerの保護状態を返します。
// 未登録の場合はretainedの空mapを持つ状態を作成し、g.statesByTokenへ登録して返します。
// 内部状態を追加する可能性があるため、呼び出し元はg.statesMuのwrite lockを
// 取得していなければなりません。
func (g *DirectoryGuard) stateForTokenLocked(token *DirectoryGuardToken) *guardedPaths {
	state := g.statesByToken[token]
	if state == nil {
		state = &guardedPaths{retained: make(map[string]string)}
		g.statesByToken[token] = state
	}
	return state
}

// deleteEmptyStateLockedは、tokenのownerがactive pathとretained pathのどちらも
// 保護していない場合に、そのownerの状態をg.statesByTokenから削除します。
// DirectoryGuard自体をprocess内のregistryから削除する処理ではありません。
// 呼び出し元はg.statesMuのwrite lockを取得していなければなりません。
func (g *DirectoryGuard) deleteEmptyStateLocked(token *DirectoryGuardToken, state *guardedPaths) {
	if state.active == "" && len(state.retained) == 0 {
		delete(g.statesByToken, token)
	}
}
