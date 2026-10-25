package fsarchivehandler

import (
	"fmt"
	"io"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// FailureArchivePipelineOptionはPipelineへ構築済みの依存関係を渡します。
type FailureArchivePipelineOption func(*failureArchivePipelineOptions)

type failureArchivePipelineOptions struct {
	spoolRuntimes compressedlog.RootRuntimes
	// spillStdoutは、degrade policyのstdout退避でos.Stdoutの代わりに使うwriterです。
	// nilならos.Stdoutへ出力します。package内の試験で退避内容を確認するために使います。
	spillStdout io.Writer
}

// WithCompressedLogSpoolRuntimesは圧縮ファイルの送信失敗時に使う共有spoolを渡します。
// spoolを使用する場合は、対象backendすべてのruntimeを呼び出し元で構築してください。
// Pipelineはruntimeを構築せず、終了時は各系列の利用登録だけを解除します。
// runtime自体はPipelineの終了後に呼び出し元が閉じます。spoolを使用しなければnilを渡せます。
func WithCompressedLogSpoolRuntimes(runtimes compressedlog.RootRuntimes) FailureArchivePipelineOption {
	return func(options *failureArchivePipelineOptions) { options.spoolRuntimes = runtimes }
}

// manageInlineArchiveUploadersは宛先とcheckpointの準備後に、各.jsonl系列へ共通の
// 圧縮ファイル退避処理を接続します。元のbackend失敗分類別prefixを持つuploaderを
// 委譲先にするため、retryableとpermanentの送信先・退避先を混同しません。
func (f *FailureArchivePipeline) manageInlineArchiveUploaders(runtimes compressedlog.RootRuntimes) error {
	handler := f.handler
	reporter := newFailureArchiveLifecycleLogReporter(handler, nil)
	for _, target := range []struct {
		category failurearchive.FailureCategory
		fileName logpattern.FileNamePattern
		uploader *uploader.Uploader
	}{
		{failurearchive.FailureCategoryRetryable, handler.Fsstore.FileNameRetryable, &handler.retryableArchiveUploader},
		{failurearchive.FailureCategoryPermanent, handler.Fsstore.FileNamePermanent, &handler.permanentArchiveUploader},
	} {
		managed, owner, err := logfilelifecycle.WrapObjectStorageUploader(
			handler.Config.ObjectStorageUpload(), runtimes, *target.uploader, target.fileName,
			handler.Clock, reporter, handler.compressedArchiveUploadCheckpointStore(target.category), false,
		)
		if err != nil {
			return fmt.Errorf("create %s failure archive compressed log file failure uploader: %w", target.category, err)
		}
		*target.uploader = managed
		if owner != nil {
			f.uploadFailureRoutes = append(f.uploadFailureRoutes, owner)
		}
	}
	return nil
}
