package appendlog

import (
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorysize"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// directoryStateは、同じ正規化済みdirectory pathに対応するprocess共有部品を
// まとめます。Guardとsize Trackerは兄弟要素であり、互いの責務には含めません。
type directoryState struct {
	guard                *DirectoryGuard
	directorySizeTracker *directorysize.Tracker
}

var directoryStates = struct {
	mu sync.Mutex
	// byPathは、pathidentityで正規化したdirectory比較keyを
	// keyとして、process内で共有するdirectoryStateを保持します。
	byPath map[string]*directoryState
}{
	byPath: make(map[string]*directoryState),
}

// DirectoryGuardForPathはpathidentityで表記揺れを正規化し、process内で同じpathに
// 一つのGuardを返します。Windowsは標準のcase-insensitive directoryを対象とします。
// 呼び出し側は初期化時にResolveDirectoryで解決した実体パスを渡します。
// この関数ではfilesystemを参照せず、稼働中も同じ比較キーを使用します。
// directory数は設定数に限定されるためprocess lifetimeで保持します。
func DirectoryGuardForPath(path string) *DirectoryGuard {
	key := pathidentity.ComparisonKey(path)
	directoryStates.mu.Lock()
	defer directoryStates.mu.Unlock()

	return directoryStateForKeyLocked(key).guard
}

// DirectorySizeTrackerForPathは、同じ正規化済みdirectoryへ書くinline lifecycleが
// 共有する、MaxDirSize観測用のbyte数Trackerを返します。最初に要求されたときだけ
// 作成するため、runner・disable modeでGuardだけを使う場合は確保しません。
// pathはGuardと同じ初期化済みの実体パスを指定します。
func DirectorySizeTrackerForPath(path string) *directorysize.Tracker {
	key := pathidentity.ComparisonKey(path)
	directoryStates.mu.Lock()
	defer directoryStates.mu.Unlock()

	state := directoryStateForKeyLocked(key)
	if state.directorySizeTracker == nil {
		state.directorySizeTracker = directorysize.NewTracker()
	}
	return state.directorySizeTracker
}

// directoryStateForKeyLockedは正規化済みpathに対応するstateを返します。
// 呼び出し元はdirectoryStates.muを保持していなければなりません。
func directoryStateForKeyLocked(key string) *directoryState {
	if state := directoryStates.byPath[key]; state != nil {
		return state
	}

	state := &directoryState{
		guard: &DirectoryGuard{
			statesByToken: make(map[*DirectoryGuardToken]*guardedPaths),
		},
	}
	directoryStates.byPath[key] = state
	return state
}
