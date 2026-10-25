package fsstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// maxFailedDirectorySyncsは、同期を確認できない親の記録を保持する上限です。
// 上限に達している間は、準備のたびに記録済みの全件を再試行し、記録のない親への
// 新しいdirectoryの作成を拒否します。記録を捨てて成功を返すことはありません。
const maxFailedDirectorySyncs = 256

// directorySyncsは、EnsureDirectory系の呼び出しが作成したdirectoryの親のうち、
// 永続化を確認できていないものをprocess全体で保持します。
// 呼び出し元のStoreやリンクの検査方針が異なっても、同じ親の記録を共有します。
var directorySyncs = directorySyncRegistry{entries: make(map[string]*directorySyncEntry)}

type directorySyncRegistry struct {
	mu sync.Mutex
	// entriesのkeyはpathidentity.ComparisonKeyで、Windowsの大小文字や拡張パス表記を統合します。
	// リンクは解決しないため、呼び出し元は初期化時に解決した実体パスを渡します。
	entries map[string]*directorySyncEntry
	// failedはfailed=trueのentry数です。
	failed int
	// sequenceは失敗を記録するたびに増やし、entryを作り直しても同じ値を再利用しません。
	sequence uint64
}

type directorySyncEntry struct {
	// pathは、上限到達時にchain外の記録を再試行する際に同期する絶対パスです。
	// 最初に登録した呼び出しの表記を保持し、chain上の同期には各呼び出しの表記を使います。
	path string
	// creatorsは、この親の直下へ作成を試み、まだ同期結果を反映していない呼び出しの数です。
	// 作成中も記録を残し、同期前の作成結果を見た別の呼び出しにもこの親を同期させます。
	creators int
	// failedは、同期を確認できていない作成結果が残っていることを表します。
	failed bool
	// versionは最後に失敗を記録したときのsequenceです。同期前に読んだ値と比べ、
	// その後に記録された失敗を古い同期の成功で解除しません。
	version uint64
}

// directoryPathKeyは、保存先または上位directoryの絶対パスと比較キーの組です。
type directoryPathKey struct {
	path string
	key  string
}

// directorySyncTargetは、同期の直前に読み取った記録です。
type directorySyncTarget struct {
	directoryPathKey
	version uint64
}

// directoryPathKeysは、保存先からルートまでの組を下位から順に返します。
// 比較キーは一度だけ作り、上位はパスと同じく末尾の要素を除いて求めます。
func directoryPathKeys(absolute string) []directoryPathKey {
	path, key := absolute, pathidentity.ComparisonKey(absolute)
	var chain []directoryPathKey
	for {
		chain = append(chain, directoryPathKey{path: path, key: key})
		parent := filepath.Dir(path)
		if parent == path {
			return chain
		}
		path, key = parent, filepath.Dir(key)
	}
}

// retryFailedは、chain上の親に記録された同期失敗を上位から順に再試行します。
// 同期できた記録は解除します。親自体が既に存在しない記録も、確認すべき作成結果が
// 残っていないため解除します。失敗した親はDirectorySyncErrorで返し、記録を保持します。
// 記録数が上限に達している場合は、chain外の記録も再試行して減らします。chain外の失敗は返しません。
func (r *directorySyncRegistry) retryFailed(chain []directoryPathKey, syncDirectory func(string) error) error {
	dependencies, others := r.failedTargets(chain)
	var failures []error
	for _, target := range dependencies {
		if err := r.retry(target, syncDirectory); err != nil {
			failures = append(failures, err)
		}
	}
	for _, target := range others {
		_ = r.retry(target, syncDirectory)
	}
	return errors.Join(failures...)
}

// retryは記録済みの失敗を一件再試行します。親が存在しないerrorは同期の成功と同じく扱います。
func (r *directorySyncRegistry) retry(target directorySyncTarget, syncDirectory func(string) error) error {
	err := syncDirectory(target.path)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	r.settle(target, err)
	if err != nil {
		return &DirectorySyncError{Directory: target.path, Err: err}
	}
	return nil
}

// failedTargetsは、chain上で失敗を記録した親を上位から順に返します。
// 記録数が上限に達していれば、chain外の失敗もkey順に返します。
func (r *directorySyncRegistry) failedTargets(chain []directoryPathKey) (dependencies, others []directorySyncTarget) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed == 0 {
		return nil, nil
	}
	seen := make(map[string]bool, len(chain))
	for index := len(chain) - 1; index >= 0; index-- {
		current := chain[index]
		if seen[current.key] {
			continue
		}
		seen[current.key] = true
		if entry := r.entries[current.key]; entry != nil && entry.failed {
			dependencies = append(dependencies, directorySyncTarget{directoryPathKey: current, version: entry.version})
		}
	}
	if r.failed < maxFailedDirectorySyncs {
		return dependencies, nil
	}
	for key, entry := range r.entries {
		if entry.failed && !seen[key] {
			others = append(others, directorySyncTarget{
				directoryPathKey: directoryPathKey{path: entry.path, key: key},
				version:          entry.version,
			})
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i].key < others[j].key })
	return dependencies, others
}

// beginCreationは、parentの直下へdirectoryを作成する前に呼び出します。
// syncCreatedが同期結果を反映するまで、作成中として記録します。
func (r *directorySyncRegistry) beginCreation(parent directoryPathKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[parent.key]
	if entry == nil {
		if r.failed >= maxFailedDirectorySyncs {
			return fmt.Errorf("%w: too many directories have unconfirmed parent syncs; refusing to create a directory in %q",
				pkgfsstore.ErrDurabilityUnconfirmed, parent.path)
		}
		entry = &directorySyncEntry{path: parent.path}
		r.entries[parent.key] = entry
	}
	entry.creators++
	return nil
}

// cancelCreationは、作成を試みたdirectoryが存在しないと確認した場合に作成中の記録を解除します。
// この呼び出しはparentを変更していないため、parentを同期せず、同期失敗も記録しません。
func (r *directorySyncRegistry) cancelCreation(parent directoryPathKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[parent.key]
	if entry == nil {
		return
	}
	entry.creators--
	r.removeSettledLocked(parent.key, entry)
}

// syncCreatedは、createdの親を上位から順に同期します。一つが失敗しても残りを試し、
// 失敗した親を記録して、依存する次の準備で再試行させます。
// includeDependenciesなら、別の呼び出しが作成中または同期失敗を記録したchain上の親も同期し、
// 保存先が依存するdirectory entryの永続化を確認します。
// 同期関数がpanicした場合も、同期結果を反映できなかった作成を失敗として記録します。
func (r *directorySyncRegistry) syncCreated(chain, created []directoryPathKey, includeDependencies bool, syncDirectory func(string) error) error {
	if len(created) == 0 && !includeDependencies {
		return nil
	}
	targets := r.syncTargets(chain, created, includeDependencies)
	if len(targets) == 0 && len(created) == 0 {
		return nil
	}
	settled := make(map[string]bool, len(targets))
	defer r.finishCreations(created, settled)
	var failures []error
	for _, target := range targets {
		syncErr := syncDirectory(target.path)
		r.settle(target, syncErr)
		settled[target.key] = true
		if syncErr != nil {
			failures = append(failures, &DirectorySyncError{Directory: target.path, Err: syncErr})
		}
	}
	return errors.Join(failures...)
}

// syncTargetsは、同期するchain上の親を上位から順に返します。
// この呼び出しが作成を試みた親は、作成中として記録済みです。
func (r *directorySyncRegistry) syncTargets(chain, created []directoryPathKey, includeDependencies bool) []directorySyncTarget {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) == 0 {
		return nil
	}
	var own map[string]bool
	if len(created) > 0 {
		own = make(map[string]bool, len(created))
		for _, parent := range created {
			own[parent.key] = true
		}
	}
	var targets []directorySyncTarget
	seen := make(map[string]bool, len(chain))
	for index := len(chain) - 1; index >= 0; index-- {
		current := chain[index]
		entry := r.entries[current.key]
		if seen[current.key] || entry == nil || (!own[current.key] && !includeDependencies) {
			continue
		}
		seen[current.key] = true
		targets = append(targets, directorySyncTarget{directoryPathKey: current, version: entry.version})
	}
	return targets
}

// settleは同期結果を記録へ反映します。成功は、同期前に読み取った後に新しい失敗が
// 記録されていない場合だけ失敗を解除します。失敗は、記録がなければ作成して保持します。
func (r *directorySyncRegistry) settle(target directorySyncTarget, syncErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[target.key]
	if syncErr != nil {
		if entry == nil {
			entry = &directorySyncEntry{path: target.path}
			r.entries[target.key] = entry
		}
		r.markFailedLocked(entry)
		return
	}
	if entry == nil {
		return
	}
	if entry.failed && entry.version == target.version {
		entry.failed = false
		r.failed--
	}
	r.removeSettledLocked(target.key, entry)
}

// finishCreationsは作成中の記録を解除します。panicなどで同期結果を反映できなかった
// 作成は、失敗として残します。
func (r *directorySyncRegistry) finishCreations(created []directoryPathKey, settled map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, parent := range created {
		entry := r.entries[parent.key]
		if entry == nil {
			continue
		}
		entry.creators--
		if !settled[parent.key] {
			r.markFailedLocked(entry)
		}
		r.removeSettledLocked(parent.key, entry)
	}
}

func (r *directorySyncRegistry) markFailedLocked(entry *directorySyncEntry) {
	if !entry.failed {
		entry.failed = true
		r.failed++
	}
	r.sequence++
	entry.version = r.sequence
}

func (r *directorySyncRegistry) removeSettledLocked(key string, entry *directorySyncEntry) {
	if entry.creators == 0 && !entry.failed {
		delete(r.entries, key)
	}
}
