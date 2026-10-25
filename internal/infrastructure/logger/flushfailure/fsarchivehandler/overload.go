package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"

	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/pkg/disk"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func validateFileArchiveOverloadPolicyConfigProvider(cfg loggerdurable.FileArchiveOverloadPolicyConfigProvider) error {
	if cfg == nil {
		return errors.New("failure archive overload policy config is nil")
	}
	if cfg.MinAvailableDiskSpace() < pkgdurable.MinFileArchiveAvailableDiskSpace {
		return fmt.Errorf(
			"%w: must be >= %d, got %d",
			ErrFileArchiveMinAvailableDiskSpaceTooSmall,
			pkgdurable.MinFileArchiveAvailableDiskSpace,
			cfg.MinAvailableDiskSpace(),
		)
	}
	return nil
}

// 過負荷状態かどうかを判定する
func (f *FsArchiveHandler) isOverloaded(ctx context.Context) (bool, error) {

	// 判定条件が1つのみのため
	return f.isStorageOverloaded(ctx)
}

// isStorageOverloadedは構築時に固定したarchive directoryの空き容量を判定します。
// 閾値は構築時に下限以上であることを検証済みのため、常に空き容量を確認します。
// 容量を取得できなければ、容量不足とも保存可能とも判断せずエラーを返し、
// 呼び出し元が今回の保存を中断して通知します。
func (f *FsArchiveHandler) isStorageOverloaded(ctx context.Context) (bool, error) {
	minimum := f.Config.OverloadPolicyConfig().MinAvailableDiskSpace()
	directory := f.archiveRoot
	if directory == "" {
		// Pipelineを経由しない未初期化のhandlerは従来の設定値を使います。
		directory = f.Config.ArchivePath()
	}
	availableDiskSpace := f.availableDiskSpace
	if availableDiskSpace == nil {
		availableDiskSpace = freeDiskSpace
	}
	free, err := availableDiskSpace(directory)
	if err != nil {
		return false, fmt.Errorf("get available disk space for failure archive directory %q: %w", directory, err)
	}
	return free < minimum, nil
}

// freeDiskSpaceはdirectoryを含むfilesystemで利用できる空き容量を返します。
func freeDiskSpace(directory string) (uint64, error) {
	free, _, err := disk.FreeAndTotal(directory)
	return free, err
}
