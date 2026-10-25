package file

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

var errInvalidUserConfiguredCompressedFile = errors.New("invalid user-configured compressed file")

const userConfiguredCompressionHeader = "user-configured-codec-v1\n"

// userConfiguredCompressionCodecは、利用者が.envではなくtyped configに
// 独自圧縮形式を追加するケースを再現します。
type userConfiguredCompressionCodec struct{}

func (userConfiguredCompressionCodec) Format() pkgfsstore.CompressFormat {
	return "br"
}

func (userConfiguredCompressionCodec) Compress(in, out *os.File) error {
	if _, err := io.WriteString(out, userConfiguredCompressionHeader); err != nil {
		return err
	}
	_, err := io.Copy(out, in)
	return err
}

func (userConfiguredCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	content, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(content, []byte(userConfiguredCompressionHeader)) {
		return fmt.Errorf("%w: header is missing", errInvalidUserConfiguredCompressedFile)
	}
	return nil
}

func TestFileLoggerUsesUserConfiguredCompressionCodecForInlineArchive(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	cfg := newTestFileLoggerConfig(logPath, "user_codec_logs", "user-codec")
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	// 独自codecのFormatが、組み込み形式の設定値より優先されることも検証する。
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	cfg.FsstoreConfig.CompressionCodec = userConfiguredCompressionCodec{}

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger, err := loggerSet.NewFileLogger(ctx)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	sourcePath := filepath.Join(
		logPath,
		cfg.FsstoreConfig.Layout.Target,
		cfg.FsstoreConfig.Layout.LogDir,
		"rotated.log",
	)
	require.NoError(t, os.WriteFile(sourcePath, []byte("user codec payload\n"), 0o600))

	var uploadedPath string
	remote := archiveUploaderFunc(func(
		_ context.Context,
		input uploader.UploadRequest,
	) error {
		uploadedPath = input.FilePath()
		// local削除前のupload境界では、設定codecで検証できる完成済みfileです。
		return logger.Fsstore.VerifyCompressedFileIntegrity(input.FilePath())
	})
	logger.ArchiveUploader = newSingleArchiveUploader(t, remote)
	logger.tryArchiveRotatedFile(ctx, sourcePath, logger.Fsstore.FileName)

	compressedPath := sourcePath + ".br"
	require.Equal(t, compressedPath, uploadedPath)
	require.NoFileExists(t, sourcePath)
	require.NoFileExists(t, compressedPath)
	require.NoFileExists(t, sourcePath+pkgfsstore.ExtensionCompressGzip)
}

type archiveUploaderFunc func(context.Context, uploader.UploadRequest) error

func (f archiveUploaderFunc) Upload(ctx context.Context, input uploader.UploadRequest) error {
	return f(ctx, input)
}

var _ pkgfsstore.CompressionCodec = userConfiguredCompressionCodec{}
