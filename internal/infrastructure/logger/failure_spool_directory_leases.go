package logger

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	fsstorespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// ErrMixedFailureSpoolContractsは、一つのlogger契約のdirectory lease setを取得するplanに、
// 別のlogger契約のpipelineが含まれることを表します。
var ErrMixedFailureSpoolContracts = errors.New("failure spool directory lease set includes another contract pipeline")

// MixedFailureSpoolContractErrorは、ContractKindのdirectory lease setを取得するplanに
// PipelineContractKindのpipelineが含まれることを表します。errors.Isでは
// ErrMixedFailureSpoolContractsと一致します。
type MixedFailureSpoolContractError struct {
	ContractKind         pkglogger.LoggerContractKind
	PipelineContractKind pkglogger.LoggerContractKind
}

func (e *MixedFailureSpoolContractError) Error() string {
	return fmt.Sprintf(
		"failure spool directory lease set for %s includes %s pipeline",
		e.ContractKind,
		e.PipelineContractKind,
	)
}

func (e *MixedFailureSpoolContractError) Is(target error) bool {
	return target == ErrMixedFailureSpoolContracts
}

// failureSpoolDirectoryLeaseSetは、一つのlogger契約について、構築時に取得した
// backendごとのspool rootの使用権をまとめます。通常起動と復旧処理は同じ単位で構築し、
// 複数契約を扱う復旧runtimeは契約ごとのSetを保持します。
// 使用権は同一プロセス内での二重管理を防ぎ、終了時にまとめて解放します。
// 通常起動では同じ契約・backendのwriterとrunnerが一つのLeaseを共有します。
type failureSpoolDirectoryLeaseSet struct {
	contractKind pkglogger.LoggerContractKind
	entries      []failureSpoolDirectoryLease
	once         sync.Once
}

// failureSpoolDirectoryLeaseは、一つのbackendが管理するspoolのrootと、
// そのrootを排他的に管理する使用権を対応付けます。logger契約は所属するSetが保持し、
// Set内ではbackendでLeaseを検索して対応するwriter・runnerへ結び付けます。
type failureSpoolDirectoryLease struct {
	backend pkglogger.LogOutputDestination
	root    string
	lease   *directorylease.Lease
}

func failureSpoolDirectoryScopeLabel(
	contractKind pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
) directorylease.ScopeLabel {
	return directorylease.ScopeLabel(
		contractKind.String() + "." + backend.String() + ".failure_spool",
	)
}

// canonicalSpoolRootは、spool設定providerから使用権を取得するrootの絶対パスを解決します。
func canonicalSpoolRoot(config loggerdurable.SpoolConfigProvider) (string, error) {
	root, err := fsstorespool.NewRootDir(config.SpoolPath(), config.Target(), config.Dir())
	if err != nil {
		return "", err
	}
	return filepath.Abs(root.String())
}

// acquireFailureSpoolDirectoryLeasesは、一つのlogger契約のplanから、各backendの
// spool rootの使用権を一度ずつ取得します。契約は先頭要素から取得し、別契約が混在する
// planは使用権の取得前に拒否します。復旧処理も契約ごとにこの関数を呼びます。
//
// 一契約内のrootをパス順に取得します。途中で競合した場合は、取得済みLeaseを
// 逆順で解放し、この契約の部分予約を残しません。
func acquireFailureSpoolDirectoryLeases(
	plan []failureSpoolPipelinePlan,
) (*failureSpoolDirectoryLeaseSet, error) {
	if len(plan) == 0 {
		return nil, nil
	}
	contractKind := plan[0].contractKind
	for _, pipeline := range plan {
		if pipeline.contractKind != contractKind {
			return nil, &MixedFailureSpoolContractError{
				ContractKind:         contractKind,
				PipelineContractKind: pipeline.contractKind,
			}
		}
	}

	ordered := append([]failureSpoolPipelinePlan(nil), plan...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].root < ordered[right].root
	})

	set := &failureSpoolDirectoryLeaseSet{
		contractKind: contractKind,
		entries:      make([]failureSpoolDirectoryLease, 0, len(ordered)),
	}
	for _, pipeline := range ordered {
		scopeLabel := failureSpoolDirectoryScopeLabel(set.contractKind, pipeline.backend)
		lease, err := directorylease.Acquire(scopeLabel, pipeline.root)
		if err != nil {
			set.releaseNow()
			return nil, fmt.Errorf(
				"acquire %s root %q: %w",
				scopeLabel,
				pipeline.root,
				err,
			)
		}
		set.entries = append(set.entries, failureSpoolDirectoryLease{
			backend: pipeline.backend,
			root:    pipeline.root,
			lease:   lease,
		})
	}
	return set, nil
}

// Releaseは、このsetが保持する全Leaseを構築時と逆の順序で一度だけ解放します。
func (s *failureSpoolDirectoryLeaseSet) Release() {
	if s == nil {
		return
	}
	s.once.Do(s.releaseNow)
}

func (s *failureSpoolDirectoryLeaseSet) releaseNow() {
	for index := len(s.entries) - 1; index >= 0; index-- {
		s.entries[index].lease.Release()
	}
}

func (s *failureSpoolDirectoryLeaseSet) leaseFor(
	backend pkglogger.LogOutputDestination,
) (*directorylease.Lease, bool) {
	if s == nil {
		return nil, false
	}
	for _, entry := range s.entries {
		if entry.backend == backend {
			return entry.lease, true
		}
	}
	return nil, false
}

// releaseFailureSpoolDirectoryLeaseSetsは、契約ごとのSetを取得順の逆順で解放します。
// 復旧runtimeの構築失敗時と、全runnerの停止後に呼びます。
func releaseFailureSpoolDirectoryLeaseSets(sets []*failureSpoolDirectoryLeaseSet) {
	for index := len(sets) - 1; index >= 0; index-- {
		sets[index].Release()
	}
}
