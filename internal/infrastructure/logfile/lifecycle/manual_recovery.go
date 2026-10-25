package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
)

// ErrManualRecoveryRequiredは、通常稼働用のlifecycleへ停止中専用の走査を適用することを防ぎます。
// 通常構成のcheckpointやdurable storeは非同期処理を所有する場合があります。
var ErrManualRecoveryRequired = errors.New("offline recovery requires a manual recovery lifecycle")

// ErrOfflineRecoveryAwaitsEarlierGenerationは、停止中の復旧で、先行する未完了の世代があるため
// その後の世代を送信済みとして記録できないことを表します。先行世代の完了後に再実行します。
var ErrOfflineRecoveryAwaitsEarlierGeneration = errors.New("offline recovery upload awaits an earlier incomplete generation")

// ErrOfflineRecoveryPendingSaveStatusUnavailableは、停止中の復旧で使うcheckpoint Storeが
// 未保存の完了記録の有無を報告できず、復旧結果を確定できないことを表します。
var ErrOfflineRecoveryPendingSaveStatusUnavailable = errors.New("offline recovery checkpoint store cannot report pending saves")

// RecoveryOptionは停止中専用の復旧処理の動作を指定します。
type RecoveryOption func(*recoveryOptions)

type recoveryOptions struct {
	checkpointSaveRetry bool
}

// WithCheckpointSaveRetryはcheckpoint保存失敗後の再試行を有効にします。
// 呼び出し元は同じlifecycleでRecoverOnceを再実行し、最後にCloseまたはForceCloseします。
// 未保存の更新がある間は次のRecoverOnceを保存待ちにし、完了記録の永続化より先に
// 圧縮ファイルを削除しません。purgeやspoolの再送workerは起動しません。
func WithCheckpointSaveRetry() RecoveryOption {
	return func(options *recoveryOptions) { options.checkpointSaveRetry = true }
}

func resolveRecoveryOptions(options []RecoveryOption) recoveryOptions {
	var settings recoveryOptions
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	return settings
}

// NewLogFileRecoveryは通常loggerと同じ命名規則、checkpoint Source ID、宛先、
// durable退避先を使い、停止中のfileを一度だけ処理するlifecycleを構築します。
// 既定では自動再試行を行いません。WithCheckpointSaveRetryでcheckpoint保存だけを再試行します。
// spool再送・保持期限purgeのworkerは起動しません。構築前から全lifecycleの
// Close完了までアプリを停止してください。checkpoint保存先は呼び出し前に準備します。
func NewLogFileRecovery(opts LogFileOptions, options ...RecoveryOption) ([]*DirectoryLifecycle, error) {
	settings := resolveRecoveryOptions(options)
	return newDirectoryLifecycles(directoryLifecycleFactoryOptions{
		manualRecovery:      true,
		checkpointSaveRetry: settings.checkpointSaveRetry,
		Config:              appendlog.NewConfigAdapter(opts.Config.FsstoreConfig),
		ObjectStorageUpload: opts.Config.ObjectStorageUploadConfig,
		SpoolRuntimes:       opts.SpoolRuntimes,
		ArchiveCheckpoint:   opts.Config.ArchiveCheckpointConfig,
		Clock:               opts.Clock,
		Tracer:              opts.Tracer,
		Meter:               opts.Meter,
		Reporter:            opts.Reporter,
	})
}

// NewFailureArchiveRecoveryはinline modeを含む両failure分類のJSONL復旧を構築します。
// runner起動とpurgeは行いません。アプリの停止とCloseの責務はNewLogFileRecoveryと同じです。
func NewFailureArchiveRecovery(opts FailureArchiveOptions, options ...RecoveryOption) ([]*DirectoryLifecycle, error) {
	return newFailureArchiveLifecyclesWithMode(opts, true, options...)
}

// NewDeliveryFileRecoveryはdelivery loggerと同じ命名規則と既存checkpoint Source IDで
// 停止中のfileを処理するlifecycleを構築します。
func NewDeliveryFileRecovery(opts DeliveryFileOptions, options ...RecoveryOption) ([]*DirectoryLifecycle, error) {
	settings := resolveRecoveryOptions(options)
	return newDirectoryLifecycles(directoryLifecycleFactoryOptions{
		manualRecovery:      true,
		checkpointSaveRetry: settings.checkpointSaveRetry,
		Config:              appendlog.NewDeliveryFileLifecycleStoreConfigAdapter(opts.Config.FsstoreConfig),
		ObjectStorageUpload: opts.Config.ObjectStorageUploadConfig,
		SpoolRuntimes:       opts.SpoolRuntimes,
		ArchiveCheckpoint:   opts.Config.ArchiveCheckpointConfig,
		Clock:               opts.Clock,
		Tracer:              opts.Tracer,
		Meter:               opts.Meter,
		Reporter:            opts.Reporter,
	})
}

// RecoverOnceは最新世代を含む停止中の対象fileを一度だけ処理します。
// 圧縮と送信済みfileの削除には既存の永続化処理を使い、日数・世代数・容量purgeは行いません。
// 未完了のfileは次回実行へ残し、fileごとのerrorと実行全体の集約errorを返します。
// WithCheckpointSaveRetry指定時は、前回のcheckpoint保存が未完了なら処理を始めず、保存待ちを返します。
func (d *DirectoryLifecycle) RecoverOnce(ctx context.Context) (RunReport, error) {
	if err := d.validate(); err != nil {
		return RunReport{}, err
	}
	for _, manager := range d.fileSeriesManagers {
		if !manager.manualRecovery {
			return RunReport{}, ErrManualRecoveryRequired
		}
	}
	if d.checkpointSaveRetryEnabled {
		if err := d.pendingCheckpointSaveError(ctx); err != nil {
			directory, _ := d.DirectoryPath()
			return RunReport{DirectoryPath: directory}, err
		}
	}
	return d.RunOnce(ctx)
}

// pendingCheckpointSaveErrorは、前回受理した完了記録が保存されたかを確認します。
// Loadは未保存でも最新memoryを返すため、手動再試行では保存状況を別に確認します。
// この確認自体は保存・ファイル走査・元の更新操作の再実行を行いません。
func (d *DirectoryLifecycle) pendingCheckpointSaveError(ctx context.Context) error {
	var errs []error
	for _, manager := range d.fileSeriesManagers {
		if manager.objectStorageUpload == nil {
			continue
		}
		manager.objectStorageUpload.stateMu.Lock()
		store := manager.objectStorageUpload.checkpointStore
		manager.objectStorageUpload.stateMu.Unlock()
		if store == nil {
			continue
		}
		status, ok := store.(interface{ PendingSaveError(context.Context) error })
		if !ok {
			errs = append(errs, fmt.Errorf("%w: directory %q", ErrOfflineRecoveryPendingSaveStatusUnavailable, manager.fileName.Dir))
			continue
		}
		if err := status.PendingSaveError(ctx); err != nil {
			errs = append(errs, fmt.Errorf("offline recovery checkpoint for %q: %w", manager.fileName.Dir, err))
		}
	}
	return errors.Join(errs...)
}

func (m *FileSeriesManager) recordRecoveryError(report *RunReport, path string, err error) {
	if m.manualRecovery && err != nil {
		report.RecoveryErrors = append(report.RecoveryErrors, FileRecoveryError{Path: path, Err: err})
	}
}
