package objectstoragespool

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// terminalStatesは、保持設定に従うpurgeの対象となる状態です。
// terminalファイルとは、自動再送の対象から外れ、done（送信成功）、
// dead（恒久的な送信失敗）、quarantine（そのままでは安全に再送できないため隔離）の
// いずれかに配置された圧縮ログファイルです。
// active・processingのファイルと、自動削除しない.preservedの保全ファイルは含みません。
var terminalStates = [...]pkgspool.State{
	pkgspool.StateDone,
	pkgspool.StateDead,
	pkgspool.StateQuarantine,
}

// TerminalPurgeConfigは送信処理が終わったstateだけに適用する保持設定です。
// 未送信dataを含むactiveと、workerが所有中のprocessingは設定対象に含みません。
type TerminalPurgeConfig struct {
	Done       pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
	Dead       pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
	Quarantine pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
	// CapacityPurgeMinimumFileAgeはdurable handoff直後のterminal fileを
	// 容量purgeから除外する期間です。productionではpurge周期を設定し、
	// callerがsourceを削除する境界とcapacity purgeの競合を避けます。
	CapacityPurgeMinimumFileAge time.Duration
}

// TerminalPurgeReportはdone・dead・quarantineそれぞれのpurge結果です。
type TerminalPurgeReport struct {
	ByState map[pkgspool.State]TerminalStatePurgeReport
}

// TerminalStatePurgeReportは一つのterminal stateについて、確認・削除・保持した
// 圧縮log fileの個数とbyte数を表します。
type TerminalStatePurgeReport struct {
	Enabled bool

	ScannedFileCount   int
	ScannedBytes       uint64
	DeletedFileCount   int
	DeletedBytes       uint64
	RemainingFileCount int
	RemainingBytes     uint64

	Errors []error
}

// TerminalPurgerはdone・dead・quarantine内のファイルを保持期限・容量上限に従って削除します。
// 各stateの容量上限は、DestinationStoresが管理する全宛先の合計に適用します。
// uploadや再送、state間の移動は行いません。
type TerminalPurger struct {
	store  *DestinationStores
	config PurgeConfigProvider
	now    func() time.Time
}

func NewTerminalPurger(
	store *DestinationStores,
	config PurgeConfigProvider,
) (*TerminalPurger, error) {
	if store == nil {
		return nil, errors.New("object storage compressed log file terminal purge store is nil")
	}
	if config == nil {
		return nil, ErrNilTerminalPurgeConfig
	}
	var errs []error
	if config.CapacityPurgeMinimumFileAge() < 0 {
		errs = append(errs, ErrNegativeCapacityPurgeMinimumFileAge)
	}
	for _, state := range terminalStates {
		stateConfig := terminalPurgeConfig(config, state)
		if stateConfig == nil {
			errs = append(errs, &StatePurgeConfigError{State: state, Err: ErrNilStatePurgeConfig})
			continue
		}
		if !stateConfig.Enabled() {
			continue
		}
		if stateConfig.MaxFileAgeDays() <= 0 {
			errs = append(errs, &StatePurgeConfigError{
				State:          state,
				Err:            ErrInvalidPurgeMaxFileAgeDays,
				MaxFileAgeDays: stateConfig.MaxFileAgeDays(),
			})
		}
		if stateConfig.MaxDirSize() == 0 {
			errs = append(errs, &StatePurgeConfigError{State: state, Err: ErrInvalidPurgeMaxDirSize})
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("invalid object storage compressed log file terminal purge config: %w", err)
	}
	return &TerminalPurger{
		store:  store,
		config: config,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (p *TerminalPurger) Run(ctx context.Context) (TerminalPurgeReport, error) {
	report := TerminalPurgeReport{
		ByState: make(map[pkgspool.State]TerminalStatePurgeReport, len(terminalStates)),
	}
	if p == nil || p.store == nil {
		return report, errors.New("object storage compressed log file terminal purge is nil")
	}
	for _, state := range terminalStates {
		report.ByState[state] = TerminalStatePurgeReport{
			Enabled: terminalPurgeConfig(p.config, state).Enabled(),
		}
	}

	now := p.now().UTC()
	var errs []error
	for _, state := range terminalStates {
		stateReport, err := p.purgeByState(ctx, state, terminalPurgeConfig(p.config, state), now)
		report.ByState[state] = stateReport
		if err != nil {
			errs = append(errs, fmt.Errorf("purge object storage compressed log file %s: %w", state, err))
		}
		if contextError(ctx) != nil {
			break
		}
	}
	return report, errors.Join(errs...)
}

type terminalFileCandidate struct {
	file       CompressedLogFile
	modifiedAt time.Time
	sizeBytes  uint64
	removed    bool
}

func (p *TerminalPurger) purgeByState(
	ctx context.Context,
	state pkgspool.State,
	config StatePurgeConfigProvider,
	now time.Time,
) (TerminalStatePurgeReport, error) {
	report := TerminalStatePurgeReport{Enabled: config.Enabled()}
	if !config.Enabled() {
		return report, nil
	}
	if err := contextError(ctx); err != nil {
		report.Errors = append(report.Errors, err)
		return report, err
	}

	files, err := p.store.ListFiles(ctx, state)
	if err != nil {
		report.Errors = append(report.Errors, err)
		return report, err
	}
	report.RemainingFileCount = len(files)
	candidates := make([]terminalFileCandidate, 0, len(files))
	for _, file := range files {
		if err := contextError(ctx); err != nil {
			report.Errors = append(report.Errors, err)
			break
		}
		report.ScannedFileCount++
		candidate, inspectErr := inspectTerminalFile(file)
		if inspectErr != nil {
			report.Errors = append(report.Errors, fmt.Errorf(
				"inspect compressed log file %q: %w",
				file.FileName,
				inspectErr,
			))
			continue
		}
		if candidate.sizeBytes > math.MaxUint64-report.ScannedBytes {
			report.Errors = append(report.Errors, fmt.Errorf(
				"inspect compressed log file %q: total byte count overflows uint64",
				file.FileName,
			))
			continue
		}
		report.ScannedBytes += candidate.sizeBytes
		report.RemainingBytes += candidate.sizeBytes
		candidates = append(candidates, candidate)
	}
	if err := contextError(ctx); err != nil {
		return report, errors.Join(report.Errors...)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].modifiedAt.Equal(candidates[j].modifiedAt) {
			if candidates[i].file.FileName != candidates[j].file.FileName {
				return candidates[i].file.FileName < candidates[j].file.FileName
			}
			return candidates[i].file.Path < candidates[j].file.Path
		}
		return candidates[i].modifiedAt.Before(candidates[j].modifiedAt)
	})

	cutoff := now.AddDate(0, 0, -config.MaxFileAgeDays())
	// 候補を順に処理する部分を共通化し、更新時刻の期限判定とterminalファイルの削除を渡します。
	ageErrors := infrafsstore.PurgeExpiredFiles(
		candidates,
		func(candidate *terminalFileCandidate) (bool, error) {
			return !candidate.modifiedAt.After(cutoff), nil
		},
		func(candidate *terminalFileCandidate) error {
			return p.removeTerminalFile(ctx, candidate, &report)
		},
	)
	report.Errors = append(report.Errors, ageErrors...)
	report.RemainingBytes = report.ScannedBytes - report.DeletedBytes

	// mtimeの古い順なので、保護期間内の候補は末尾を切り詰めて除外します。
	capacityCandidates := candidates[:sort.Search(len(candidates), func(index int) bool {
		return p.capacityPurgeProtected(candidates[index].modifiedAt, now)
	})]
	remainingBytes, sizeErrors := infrafsstore.PurgeFilesToSizeLimit(
		capacityCandidates,
		report.RemainingBytes,
		config.MaxDirSize(),
		func(candidate *terminalFileCandidate) (uint64, error) {
			if candidate.removed {
				return 0, nil
			}
			if err := p.removeTerminalFile(ctx, candidate, &report); err != nil {
				return 0, err
			}
			return candidate.sizeBytes, nil
		},
	)
	report.RemainingBytes = remainingBytes
	report.Errors = append(report.Errors, sizeErrors...)
	return report, errors.Join(report.Errors...)
}

func (p *TerminalPurger) capacityPurgeProtected(modifiedAt time.Time, now time.Time) bool {
	minimumAge := p.config.CapacityPurgeMinimumFileAge()
	return minimumAge > 0 && modifiedAt.After(now.Add(-minimumAge))
}

func inspectTerminalFile(file CompressedLogFile) (terminalFileCandidate, error) {
	candidate := terminalFileCandidate{file: file}
	info, err := os.Lstat(file.Path)
	if err != nil {
		return candidate, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return candidate, errors.New("path is not a regular file")
	}
	if info.Size() < 0 {
		return candidate, errors.New("file size is negative")
	}
	candidate.modifiedAt = info.ModTime()
	candidate.sizeBytes = uint64(info.Size())
	return candidate, nil
}

func (p *TerminalPurger) removeTerminalFile(
	ctx context.Context,
	candidate *terminalFileCandidate,
	report *TerminalStatePurgeReport,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := p.store.RemoveTerminal(ctx, candidate.file); err != nil {
		return fmt.Errorf("remove compressed log file %q: %w", candidate.file.FileName, err)
	}
	candidate.removed = true
	report.DeletedFileCount++
	report.DeletedBytes += candidate.sizeBytes
	report.RemainingFileCount--
	return nil
}

func terminalPurgeConfig(
	config PurgeConfigProvider,
	state pkgspool.State,
) StatePurgeConfigProvider {
	switch state {
	case pkgspool.StateDone:
		return config.DonePurge()
	case pkgspool.StateDead:
		return config.DeadPurge()
	case pkgspool.StateQuarantine:
		return config.QuarantinePurge()
	default:
		return StatePurgeConfigAdapter{}
	}
}
