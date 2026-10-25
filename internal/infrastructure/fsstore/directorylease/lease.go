// Package directorylease は、同一process内で独立した構成instanceが同じfilesystem
// directoryを同時に管理しないための排他的なleaseを提供します。
package directorylease

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	// ErrDirectoryLeaseConflict は、取得要求内または有効なLeaseとの間に、同じdirectoryか
	// 親子関係のdirectoryが含まれていることを表します。
	ErrDirectoryLeaseConflict = errors.New("directory lease conflict")

	// ErrScopeLabelRequired は、Leaseを利用するLoggerの構成種別が指定されていないことを
	// 表します。
	ErrScopeLabelRequired = errors.New("directory lease scope label is required")

	// ErrDirectoryPathRequired は、Leaseを取得するdirectory pathが指定されていないことを
	// 表します。
	ErrDirectoryPathRequired = errors.New("directory lease path is required")
)

// ScopeLabel は、Leaseを取得するLoggerなどの構成種別を表す診断用の名前です。
// 個々の取得instanceを一意に識別する値ではなく、同じScopeLabelから複数回Acquireできます。
type ScopeLabel string

// AcquisitionID は、一つのregistry内で正規化済みの取得要求を識別する連番です。
// 競合診断に加え、取得成功後はactive registrationとLeaseを対応付けるkeyとして使用します。
// 0は未設定値であり、process再起動後には採番が1から再開します。
type AcquisitionID uint64

// ConflictError は、取得要求と既存の有効なLeaseが競合したことを表します。
// ScopeLabelが同じでも、RequestedAttemptIDとExistingLeaseIDによって個々の取得要求を
// 区別できます。
type ConflictError struct {
	RequestedScopeLabel ScopeLabel
	RequestedAttemptID  AcquisitionID
	RequestedPath       string

	ExistingScopeLabel ScopeLabel
	ExistingLeaseID    AcquisitionID
	ExistingPath       string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf(
		"%v: requested scope=%q attempt=%d path=%q overlaps existing scope=%q lease=%d path=%q",
		ErrDirectoryLeaseConflict,
		e.RequestedScopeLabel,
		e.RequestedAttemptID,
		e.RequestedPath,
		e.ExistingScopeLabel,
		e.ExistingLeaseID,
		e.ExistingPath,
	)
}

func (e *ConflictError) Unwrap() error {
	return ErrDirectoryLeaseConflict
}

// RequestOverlapError は、一回のAcquireへ渡された異なるpath同士が競合していることを
// 表します。既存の有効なLeaseとの競合ではありません。
type RequestOverlapError struct {
	ScopeLabel ScopeLabel
	AttemptID  AcquisitionID
	FirstPath  string
	SecondPath string
}

func (e *RequestOverlapError) Error() string {
	return fmt.Sprintf(
		"directory lease request overlaps itself: scope=%q attempt=%d path=%q overlaps path=%q",
		e.ScopeLabel,
		e.AttemptID,
		e.SecondPath,
		e.FirstPath,
	)
}

func (e *RequestOverlapError) Unwrap() error {
	return ErrDirectoryLeaseConflict
}

// Lease は、一回のAcquireで取得したdirectory群の排他的な使用権です。
// Releaseは複数回または複数goroutineから呼び出しても、一度だけ使用権を解放します。
type Lease struct {
	// registryは、このLeaseの取得情報を登録した管理先です。Release時は同じregistryから
	// idに対応する登録だけを削除します。
	registry *registry
	// idはAcquire時に発行され、registry内のregistrationとこのLeaseを対応付けます。
	// process内で再利用しないため、古いLeaseのReleaseが新しいLeaseへ影響することはありません。
	id AcquisitionID
	// onceは、複数の呼び出し元やgoroutineがReleaseしてもregistryからの削除を一度だけ
	// 実行するために使用します。
	once sync.Once
}

// ID は、取得成功時のAcquisitionIDを返します。nilのLeaseでは未設定値の0を返します。
func (l *Lease) ID() AcquisitionID {
	if l == nil {
		return 0
	}
	return l.id
}

// Release は、このLeaseが使用する全directoryをまとめて解放します。
// nilのLeaseや解放済みのLeaseに対して呼び出しても何もしません。
func (l *Lease) Release() {
	if l == nil {
		return
	}

	l.once.Do(func() {
		l.registry.release(l.id)
	})
}

type registration struct {
	scopeLabel ScopeLabel
	paths      []string
}

type registry struct {
	// muは、AcquisitionIDの採番、取得要求の競合判定、activeRegistrationsへの登録、
	// Releaseによる削除を一つの順序で実行するため、registryの両fieldを保護します。
	mu sync.Mutex
	// nextAcquisitionIDは、次の正規化済み取得要求へ発行する診断用IDです。初期値は1で、
	// 既存Leaseとの競合などにより取得に失敗した要求でも、発行済みのIDは再利用しません。
	nextAcquisitionID AcquisitionID
	// activeRegistrationsは、取得済みかつ未解放のLeaseについて、AcquisitionIDごとに
	// scope labelと、そのLeaseが排他的に使用するpath群を保持します。
	activeRegistrations map[AcquisitionID]registration
}

var processRegistry = newRegistry()

func newRegistry() *registry {
	return &registry{
		nextAcquisitionID:   1,
		activeRegistrations: make(map[AcquisitionID]registration),
	}
}

// Acquire はscopeLabelで示す構成instanceがpathsを排他的に使用するためのprocess-local
// Leaseを取得します。ScopeLabelは診断用であり、一意である必要はありません。
//
// pathの空白は保存先名の一部として保持し、既存祖先のsymlinkを解決します。同じ要求内の
// 同一identityは一つへまとめますが、親子directoryはRequestOverlapErrorとして拒否します。
// Windowsは標準のcase-insensitive directoryを対象とし、診断には元の大小文字を残します。
// すべてのpathを一括検証してから登録するため、一つでも競合すれば何も取得しません。
func Acquire(scopeLabel ScopeLabel, paths ...string) (*Lease, error) {
	return processRegistry.acquire(scopeLabel, paths...)
}

func (r *registry) acquire(scopeLabel ScopeLabel, paths ...string) (*Lease, error) {
	scopeLabel = ScopeLabel(strings.TrimSpace(string(scopeLabel)))
	if scopeLabel == "" {
		return nil, ErrScopeLabelRequired
	}

	normalizedPaths, err := normalizePaths(paths)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	attemptID := r.nextAcquisitionID
	r.nextAcquisitionID++

	if err := validateRequestedPathsDoNotOverlap(scopeLabel, attemptID, normalizedPaths); err != nil {
		return nil, err
	}
	if conflict := r.oldestConflict(scopeLabel, attemptID, normalizedPaths); conflict != nil {
		return nil, conflict
	}

	r.activeRegistrations[attemptID] = registration{
		scopeLabel: scopeLabel,
		paths:      append([]string(nil), normalizedPaths...),
	}
	return &Lease{
		registry: r,
		id:       attemptID,
	}, nil
}

// oldestConflictは、要求pathと重なる有効なLeaseのうち、最も古いLeaseを返します。
// mapの走査順に依存せず、複数のLeaseと重なる要求でも常に最小のLease IDを報告します。
func (r *registry) oldestConflict(
	scopeLabel ScopeLabel,
	attemptID AcquisitionID,
	requestedPaths []string,
) *ConflictError {
	var oldest *ConflictError
	for _, requestedPath := range requestedPaths {
		for existingID, existing := range r.activeRegistrations {
			if oldest != nil && existingID >= oldest.ExistingLeaseID {
				continue
			}
			for _, existingPath := range existing.paths {
				if !directoriesOverlap(requestedPath, existingPath) {
					continue
				}
				oldest = &ConflictError{
					RequestedScopeLabel: scopeLabel,
					RequestedAttemptID:  attemptID,
					RequestedPath:       requestedPath,
					ExistingScopeLabel:  existing.scopeLabel,
					ExistingLeaseID:     existingID,
					ExistingPath:        existingPath,
				}
				break
			}
		}
	}
	return oldest
}

func (r *registry) release(id AcquisitionID) {
	if id == 0 {
		return
	}

	r.mu.Lock()
	delete(r.activeRegistrations, id)
	r.mu.Unlock()
}

func normalizePaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, ErrDirectoryPathRequired
	}

	normalized := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for index, path := range paths {
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("%w at index %d", ErrDirectoryPathRequired, index)
		}

		absolute, err := pathidentity.ResolveDirectory(path)
		if err != nil {
			return nil, fmt.Errorf("resolve directory lease path %q: %w", path, err)
		}
		key := pathidentity.ComparisonKey(absolute)
		if _, duplicated := seen[key]; duplicated {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, absolute)
	}
	return normalized, nil
}

// validateRequestedPathsDoNotOverlapは、一回のAcquireでまとめて取得しようとしている
// 正規化済みpath同士に親子関係がないことを検証します。完全一致するpathはnormalizePathsで
// すでに一つへまとめられているため、ここでは異なるpath間の包含だけを検査します。
// 既存のactive Leaseとの競合は対象とせず、registry.oldestConflictが別に検査します。
func validateRequestedPathsDoNotOverlap(
	scopeLabel ScopeLabel,
	attemptID AcquisitionID,
	paths []string,
) error {
	for secondIndex, secondPath := range paths {
		for firstIndex := 0; firstIndex < secondIndex; firstIndex++ {
			firstPath := paths[firstIndex]
			if directoriesOverlap(secondPath, firstPath) {
				return &RequestOverlapError{
					ScopeLabel: scopeLabel,
					AttemptID:  attemptID,
					FirstPath:  firstPath,
					SecondPath: secondPath,
				}
			}
		}
	}
	return nil
}

func directoriesOverlap(left, right string) bool {
	return directoryContains(left, right) || directoryContains(right, left)
}

func directoryContains(parent, child string) bool {
	relative, err := filepath.Rel(pathidentity.ComparisonKey(parent), pathidentity.ComparisonKey(child))
	if err != nil || filepath.IsAbs(relative) {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
