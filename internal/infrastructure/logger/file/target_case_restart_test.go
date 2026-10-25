package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// 大小文字を区別しないfilesystemで、HETEROLOG_TARGETの大小文字だけを変えて再起動する場合を確認します。
// OS名ではなく、作成したdirectoryの別表記が同じ実体かで実行を判断します。
func TestFileLoggerCaseInsensitiveTargetCaseRestartPreservesGenerationsAndRetriesArchive(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	sameName := caseAliasDirectoriesShareIdentity(t, filepath.Join(root, "CaseProbe"), filepath.Join(root, "CASEPROBE"))
	skipUnsupportedCaseConfiguration(t, root, sameName)
	if !sameName {
		t.Skip("the test filesystem distinguishes case-only file names")
	}
	cfg := newTestFileLoggerConfig(root, "logs", "App")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	first, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "first Close", func() closerForTest { return first }, true)
	require.NoError(t, first.HandleLogEntry(ctx, &logging.LogEntry{
		Level: pkglogger.LogLevelInfo, Message: "original uppercase target record",
	}))
	rawPath := first.Fsstore.ActiveFilePath
	period := first.Fsstore.FileName.Date
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "first Close", first))
	originalRaw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	compressedPath := filepath.Join(filepath.Dir(rawPath), fmt.Sprintf("App_%s_2.log.gz", period))
	writeGzipFile(t, compressedPath, "uppercase target pending archive")
	originalCompressed, err := os.ReadFile(compressedPath)
	require.NoError(t, err)

	// HETEROLOG_TARGETだけ変更して再構築します。filesystem上のdirectoryと既存basenameは旧表記のままです。
	cfg.FsstoreConfig.Layout.Target = "app"
	cfg.RuntimeConfig.Target = "app"
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	restarted, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "restarted Close", func() closerForTest { return restarted }, true)
	require.Equal(t, 3, restarted.Fsstore.FileName.Index,
		"the compressed second generation must prevent reopening the first raw generation")
	require.NoError(t, restarted.HandleLogEntry(ctx, &logging.LogEntry{
		Level: pkglogger.LogLevelInfo, Message: "new lowercase target record",
	}))
	waitBackgroundForTest(t, restarted)
	gotRaw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	require.Equal(t, originalRaw, gotRaw)
	gotCompressed, err := os.ReadFile(compressedPath)
	require.NoError(t, err)
	require.Equal(t, originalCompressed, gotCompressed)
	assertLogFileContains(t, restarted.Fsstore.ActiveFilePath, "new lowercase target record")
	require.True(t, logpattern.CompressedFileNameMatcher(restarted.Fsstore.FileName)(compressedPath),
		"the flat-spool matcher must also recognize the original uppercase basename")

	remote := &recordingArchiveUploader{}
	restarted.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	restarted.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
	require.NoError(t, purgeFileLoggerForTest(ctx, restarted))
	inputs := remote.inputsSnapshot()
	require.Len(t, inputs, 1)
	require.Equal(t, filepath.Base(compressedPath), filepath.Base(inputs[0].FilePath()),
		"upload must retain the original basename rather than lowercase the object key")
	require.NoFileExists(t, compressedPath)
	gotRaw, err = os.ReadFile(rawPath)
	require.NoError(t, err)
	require.Equal(t, originalRaw, gotRaw)
}
