package mappedvalidation

import (
	"errors"
	"strings"

	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

// validateMappedArchiveCheckpointConfigは、圧縮した.logや.jsonlの送信進捗と、
// 処理方式を切り替えた際の宛先無効化状態を記録するcheckpointの設定を検証します。
// 保存ディレクトリと、進捗記録の識別子に使うSourcePrefixが空文字・空白だけならエラーです。
// 呼び出し元では送信無効時やlifecycle=disable時もこの設定を必要とします。
// ここでは値だけを検証し、directoryの作成・アクセス確認やcheckpointの読み書きは行いません。
func validateMappedArchiveCheckpointConfig(
	sourceLabel string,
	cfg pkgfsstore.ArchiveCheckpointConfig,
) error {
	var errs []error
	if strings.TrimSpace(cfg.Directory) == "" {
		errs = append(errs, &ArchiveCheckpointConfigError{
			Source: sourceLabel,
			Err:    ErrArchiveCheckpointDirectoryRequired,
		})
	}
	if strings.TrimSpace(cfg.SourcePrefix) == "" {
		errs = append(errs, &ArchiveCheckpointConfigError{
			Source: sourceLabel,
			Err:    ErrArchiveCheckpointSourcePrefixRequired,
		})
	}
	return errors.Join(errs...)
}

// validateMappedFailureArchiveUploadは、送信失敗レコードを保存した.jsonlを圧縮し、
// object storageへ送る設定とcheckpoint設定を検証します。.log圧縮ファイル自体の
// upload失敗時に使う保存先の検証ではありません。
//
// fileArchive未選択なら何も検証しません。inline/runnerでuploadが有効な場合に、
// 送信先の未指定・表記不備・重複・未対応名やS3設定不備を検出します。
// fileArchiveを選択していれば、送信無効時やlifecycle=disable時もcheckpointは必須です。
// 設定エラーをまとめて返すだけで、接続・送信やファイル操作は行いません。
func validateMappedFailureArchiveUpload(
	sourceLabel string,
	cfg durable.FailureStoreConfig,
) error {
	if !containsDurableType(cfg.DurableType, durable.DurableTypeFileArchive) {
		return nil
	}
	archiveCfg := cfg.DurableStoreConfig.FileArchiveConfig
	archiveSource := sourceLabel + " failure file archive"
	var errs []error
	if filelifecycle.UsesObjectStorageUpload(archiveCfg.LifecycleMode) {
		errs = append(errs, objectstoragevalidation.ValidateUploadTargetConfig(
			archiveSource,
			archiveCfg.ObjectStorageUploadConfig,
		))
	}
	errs = append(errs, validateMappedArchiveCheckpointConfig(
		archiveSource,
		archiveCfg.ArchiveCheckpointConfig,
	))
	return errors.Join(errs...)
}

// validateMappedDeliveryFailureArchiveUploadは、Delivery loggerの失敗レコードを保存した
// .jsonlの圧縮後の送信設定を検証します。fileArchiveの選択、処理方式、upload有効設定による
// 検証対象の切り替えとcheckpointの必須条件はvalidateMappedFailureArchiveUploadと同じです。
// 設定値の検証のみを行い、ファイル保存や送信は行いません。
func validateMappedDeliveryFailureArchiveUpload(
	sourceLabel string,
	cfg durable.DeliveryFailureStoreConfig,
) error {
	if !containsDurableType(cfg.DurableType, durable.DurableTypeFileArchive) {
		return nil
	}
	archiveCfg := cfg.FileArchiveConfig
	archiveSource := sourceLabel + " failure file archive"
	var errs []error
	if filelifecycle.UsesObjectStorageUpload(archiveCfg.LifecycleMode) {
		errs = append(errs, objectstoragevalidation.ValidateUploadTargetConfig(
			archiveSource,
			archiveCfg.ObjectStorageUploadConfig,
		))
	}
	errs = append(errs, validateMappedArchiveCheckpointConfig(
		archiveSource,
		archiveCfg.ArchiveCheckpointConfig,
	))
	return errors.Join(errs...)
}
