package logrecovery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
)

// recoveryLifecycleは復旧sessionが利用する、一巡処理と終了制御の契約です。
// 本番では通常ログ・JSONL・deliveryのDirectoryLifecycleを接続します。
// ForceCloseは繰り返し呼び出せ、二回目以降は既に要求した強制停止を変えません。
type recoveryLifecycle interface {
	RecoverOnce(context.Context) (logfilelifecycle.RunReport, error)
	ForceClose() error
	CloseObjectStorageCompressedLogFileUploadFailureRoutes() error
	NotificationsDone() <-chan struct{}
}

type retryRecoverySpec struct {
	name                string
	checkpointDirectory string
	build               func(logfilelifecycle.RunReporter) ([]recoveryLifecycle, error)
}

type ownedRecoveryLifecycle struct {
	lifecycle recoveryLifecycle
	closed    bool // retrySession.muで保護します。
}

// retrySessionは終了要求と公開済みownerを管理します。再試行の待機・重複防止は
// LogFileProcessingRecoveryが担当し、このsessionに別のtimerや再試行loopは持ちません。
type retrySession struct {
	mu           sync.Mutex
	forced       bool
	owners       []*ownedRecoveryLifecycle
	remaining    int
	sealed       bool
	allCompleted chan struct{}
}

func (session *retrySession) publish(lifecycles []recoveryLifecycle) []*ownedRecoveryLifecycle {
	var owners []*ownedRecoveryLifecycle
	session.mu.Lock()
	for _, lifecycle := range lifecycles {
		owner := &ownedRecoveryLifecycle{lifecycle: lifecycle}
		owners = append(owners, owner)
		session.owners = append(session.owners, owner)
	}
	forced := session.forced
	session.mu.Unlock()
	if forced {
		for _, owner := range owners {
			_ = owner.lifecycle.ForceClose()
		}
	}
	return owners
}

func (session *retrySession) force() {
	session.mu.Lock()
	session.forced = true
	owners := slices.Clone(session.owners)
	session.mu.Unlock()
	for _, owner := range owners {
		session.mu.Lock()
		closed := owner.closed
		session.mu.Unlock()
		if !closed {
			_ = owner.lifecycle.ForceClose()
		}
	}
}

func (session *retrySession) finishJob() {
	session.mu.Lock()
	session.remaining--
	session.finishLocked()
	session.mu.Unlock()
}

func (session *retrySession) finishLocked() {
	if session.sealed && session.remaining == 0 {
		select {
		case <-session.allCompleted:
		default:
			close(session.allCompleted)
		}
	}
}

func (session *retrySession) closeOwner(owner *ownedRecoveryLifecycle) error {
	session.mu.Lock()
	closed := owner.closed
	session.mu.Unlock()
	if closed {
		return nil
	}
	err := owner.lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
	<-owner.lifecycle.NotificationsDone()
	session.mu.Lock()
	owner.closed = true
	session.mu.Unlock()
	return err
}

type retryRecoveryJob struct {
	spec      retryRecoverySpec
	owners    []*ownedRecoveryLifecycle
	reports   []DirectoryReport
	completed []bool
	attempt   int
	latestErr error
	done      bool
}

func recoverWithRetry(ctx context.Context, specs []recoverySpec, spoolOwners *compressedlog.RootRuntimeBuilder, opts Options) (Report, error) {
	jobs := make([]retryRecoverySpec, 0, len(specs))
	for _, spec := range specs {
		spec := spec
		jobs = append(jobs, retryRecoverySpec{name: spec.name, checkpointDirectory: spec.checkpointDirectory,
			build: func(reporter logfilelifecycle.RunReporter) ([]recoveryLifecycle, error) {
				// LogFileProcessingRecoveryの試行は直列なので、共有Builderも直列に使います。
				lifecycles, err := spec.build(spoolOwners, reporter, logfilelifecycle.WithCheckpointSaveRetry())
				owners := make([]recoveryLifecycle, 0, len(lifecycles))
				for _, lifecycle := range lifecycles {
					owners = append(owners, lifecycle)
				}
				return owners, err
			}})
	}
	return runRetryRecovery(ctx, jobs, opts)
}

func runRetryRecovery(ctx context.Context, specs []retryRecoverySpec, opts Options) (Report, error) {
	if onProgress := opts.OnProgress; onProgress != nil {
		notifications := notification.NewDispatcher()
		opts.OnProgress = func(progress Progress) {
			notifications.Enqueue(func() { onProgress(progress) })
		}
		defer func() {
			notifications.CloseAfterDrain()
			<-notifications.Done()
		}()
	}
	interval := opts.RetryInterval
	if interval == 0 {
		interval = DefaultRetryInterval
	}
	recovery, err := logfile.NewLogFileProcessingRecovery[int](interval)
	if err != nil {
		return Report{Errors: []string{err.Error()}}, err
	}
	session := &retrySession{allCompleted: make(chan struct{})}
	collector := &notificationCollector{}
	var jobs []*retryRecoveryJob
	controlDone := make(chan struct{})
	controlStopped := make(chan struct{})
	var forceOnce sync.Once
	requestForce := func() {
		forceOnce.Do(func() {
			recovery.ForceClose()
			session.force()
			emitProgress(opts, Progress{Phase: ProgressForced})
		})
	}
	go func() {
		defer close(controlStopped)
		stop := ctx.Done()
		force := opts.ForceStop
		for {
			select {
			case <-controlDone:
				return
			case <-stop:
				emitProgress(opts, Progress{Phase: ProgressStopping})
				stop = nil
			case <-force:
				requestForce()
				force = nil
			}
		}
	}()
	defer func() { close(controlDone); <-controlStopped }()
	for index, spec := range specs {
		if ctx.Err() != nil || forceRequested(opts.ForceStop) {
			break
		}
		job := &retryRecoveryJob{spec: spec}
		jobs = append(jobs, job)
		session.mu.Lock()
		session.remaining++
		session.mu.Unlock()
		reporter := &retryProgressReporter{scope: spec.name, options: opts, collector: collector}
		accepted := recovery.Schedule(index, nil, func(workCtx context.Context) (bool, error) {
			job.attempt++
			err := job.run(workCtx, session, reporter, opts)
			if err == nil {
				job.done = true
				session.finishJob()
			}
			return err == nil, err
		}, func(err error) func() {
			// snapshotは試行と直列です。panicでrunが戻らなくても最新の失敗を残します。
			job.latestErr = err
			progress := Progress{Phase: ProgressComplete, Scope: job.spec.name, Attempt: job.attempt, Err: err}
			if err != nil {
				session.mu.Lock()
				forced := session.forced
				session.mu.Unlock()
				if forced || forceRequested(opts.ForceStop) {
					progress.Phase = ProgressForced
				} else {
					progress.Phase = ProgressRetryWaiting
					progress.NextRetryAt = time.Now().Add(interval)
				}
			}
			return func() {
				// 通知待ちの間に強制停止された場合も、次回の実行予定は表示しません。
				if progress.Phase == ProgressRetryWaiting && forceRequested(opts.ForceStop) {
					progress.Phase = ProgressForced
					progress.NextRetryAt = time.Time{}
				}
				emitProgress(opts, progress)
			}
		}, nil)
		if !accepted {
			session.finishJob()
			job.latestErr = shutdown.ErrForcedClose
		}
	}
	session.mu.Lock()
	session.sealed = true
	session.finishLocked()
	session.mu.Unlock()
	select {
	case <-session.allCompleted:
	case <-ctx.Done():
	case <-opts.ForceStop:
		requestForce()
	}
	// Closeは受理済みの再試行を継続します。通常の停止要求ではworkCtxを中止しません。
	closeErr := recovery.Close()
	<-recovery.NotificationsDone()
	var errs []error
	if closeErr != nil {
		errs = append(errs, closeErr)
	}
	for _, owner := range session.owners {
		if err := session.closeOwner(owner); err != nil {
			errs = append(errs, err)
		}
	}
	report := Report{Directories: []DirectoryReport{}}
	for _, job := range jobs {
		report.Directories = append(report.Directories, job.reports...)
		if !job.done && job.latestErr != nil {
			errs = append(errs, fmt.Errorf("recover %s: %w", job.spec.name, job.latestErr))
		}
	}
	collector.mu.Lock()
	report.Warnings = slices.Clone(collector.warnings)
	collector.mu.Unlock()
	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}
	if forceRequested(opts.ForceStop) {
		requestForce()
	}
	session.mu.Lock()
	forced := session.forced
	session.mu.Unlock()
	if forced {
		errs = append(errs, context.Canceled, shutdown.ErrForcedClose)
	}
	for _, err := range errs {
		report.Errors = appendUnique(report.Errors, err.Error())
	}
	return report, errors.Join(errs...)
}

func forceRequested(force <-chan struct{}) bool {
	select {
	case <-force:
		return true
	default:
		return false
	}
}

func (job *retryRecoveryJob) run(ctx context.Context, session *retrySession, reporter logfilelifecycle.RunReporter, opts Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.owners == nil {
		emitProgress(opts, Progress{Phase: ProgressPreparing, Scope: job.spec.name, Attempt: job.attempt})
		if job.spec.checkpointDirectory != "" {
			if err := checkpoint.PrepareDirectory(job.spec.checkpointDirectory); err != nil {
				return fmt.Errorf("prepare checkpoint: %w", err)
			}
		}
		lifecycles, err := job.spec.build(reporter)
		owners := session.publish(lifecycles)
		if err != nil {
			var closeErrs []error
			for _, owner := range owners {
				closeErrs = append(closeErrs, session.closeOwner(owner))
			}
			return errors.Join(append(closeErrs, err)...)
		}
		job.owners = owners
		job.reports = make([]DirectoryReport, len(owners))
		job.completed = make([]bool, len(owners))
	}
	var errs []error
	for index, owner := range job.owners {
		if job.completed[index] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		emitProgress(opts, Progress{Phase: ProgressRunning, Scope: job.spec.name, Attempt: job.attempt})
		result, err := owner.lifecycle.RecoverOnce(ctx)
		item := directoryReport(job.spec.name, result, err)
		mergeRecoveryActions(&item, job.reports[index])
		job.reports[index] = item
		if err == nil && len(item.FailedFiles) > 0 {
			err = fmt.Errorf("%d file operations remain incomplete", len(item.FailedFiles))
		}
		job.completed[index] = err == nil
		if err != nil {
			errs = append(errs, err)
		}
		emitProgress(opts, Progress{Phase: ProgressRunning, Scope: job.spec.name, Attempt: job.attempt, Directory: cloneDirectoryReport(item), Err: err})
	}
	return errors.Join(errs...)
}

func mergeRecoveryActions(next *DirectoryReport, previous DirectoryReport) {
	for _, pair := range []struct {
		current  *[]string
		previous []string
	}{
		{&next.CompressedFiles, previous.CompressedFiles}, {&next.AcceptedFiles, previous.AcceptedFiles},
		{&next.RemovedFiles, previous.RemovedFiles}, {&next.RecoveredDeletePendingFiles, previous.RecoveredDeletePendingFiles},
	} {
		for _, path := range pair.previous {
			*pair.current = appendUnique(*pair.current, path)
		}
	}
}

func cloneDirectoryReport(report DirectoryReport) DirectoryReport {
	report.CompressedFiles = slices.Clone(report.CompressedFiles)
	report.AcceptedFiles = slices.Clone(report.AcceptedFiles)
	report.RemovedFiles = slices.Clone(report.RemovedFiles)
	report.RecoveredDeletePendingFiles = slices.Clone(report.RecoveredDeletePendingFiles)
	report.FailedFiles = slices.Clone(report.FailedFiles)
	return report
}

func emitProgress(opts Options, progress Progress) {
	if opts.OnProgress != nil {
		opts.OnProgress(progress)
	}
}

type retryProgressReporter struct {
	scope     string
	options   Options
	collector *notificationCollector
}

func (reporter *retryProgressReporter) ReportRun(_ context.Context, report logfilelifecycle.RunReport, err error) {
	// 保存復旧中のエラーは進捗履歴です。復旧済みの失敗を最終失敗へ戻しません。
	reporter.collector.ReportRun(context.Background(), report, nil)
	if err != nil {
		emitProgress(reporter.options, Progress{Phase: ProgressRunning, Scope: reporter.scope,
			Directory: directoryReport(reporter.scope, report, err), Err: err})
	}
}
