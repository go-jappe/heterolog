package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestFileSeriesManagerPurgeUsesLatestFileName(t *testing.T) {
	for _, entryKind := range []string{"regular", "symlink", "dangling-symlink", "directory"} {
		for _, period := range []string{"current", "newer"} {
			for _, purge := range []string{"age", "directory-size"} {
				t.Run(entryKind+"/"+period+"/"+purge, func(t *testing.T) {
					root := resolvedTempDirForTest(t)
					const target = "latest-filename"
					logDir := filepath.Join(root, target, "logs")
					require.NoError(t, os.MkdirAll(logDir, 0o750))
					now := time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)
					// 週次の現在期間は13日からです。同じ期間の_1も保持期限を
					// 過ぎた状態にし、_99だけが最新名として保護されることを確認します。
					latestDate := now.AddDate(0, 0, -3)
					olderDate := latestDate
					if period == "newer" {
						olderDate = latestDate.AddDate(0, 0, -1)
					}
					olderPath := filepath.Join(logDir, target+"_"+olderDate.Format(pkgfsstore.FileDateFormat)+"_1.log")
					require.NoError(t, os.WriteFile(olderPath, []byte("older log is eligible for purge"), 0o600))
					old := now.Add(-48 * time.Hour)
					require.NoError(t, os.Chtimes(olderPath, old, old))

					entryPath := filepath.Join(logDir, target+"_"+latestDate.Format(pkgfsstore.FileDateFormat)+"_99.log")
					externalPath := filepath.Join(resolvedTempDirForTest(t), "external.log")
					const externalContent = "external log must remain"
					switch entryKind {
					case "regular":
						require.NoError(t, os.WriteFile(entryPath, []byte("latest log must remain"), 0o600))
						require.NoError(t, os.Chtimes(entryPath, old, old))
					case "symlink":
						require.NoError(t, os.WriteFile(externalPath, []byte(externalContent), 0o600))
						testutil.Symlink(t, externalPath, entryPath)
					case "dangling-symlink":
						testutil.Symlink(t, externalPath, entryPath)
					case "directory":
						require.NoError(t, os.Mkdir(entryPath, 0o750))
					}

					maxAge := 0
					var maxDirSize uint64
					if purge == "age" {
						maxAge = 1
					} else {
						maxDirSize = 1
					}
					manager := NewFileSeriesManager(FileSeriesManagerOptions{
						FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
						Config:   testFileSeriesManagerConfigWithRotation(root, target, pkgfsstore.FileRotationWeekly, 0, maxAge, maxDirSize, false),
						Clock:    pkgclock.NewFixedClock(now),
					})

					// 最新判定の正本はファイル名の日付・世代番号です。_99.logが
					// リンク等でも、_1.logを代わりの最新として追加保護しません。
					fileName, err := manager.fileNamePattern()
					require.NoError(t, err)
					latest, found, err := manager.latestExistingFileName(fileName)
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, filepath.Base(entryPath), filepath.Base(latest))
					protected, err := manager.latestFileNames(fileName)
					require.NoError(t, err)
					require.Len(t, protected, 1)
					require.Contains(t, protected, latest)

					report, err := manager.RunOnce(context.Background())
					require.NoError(t, err)
					if purge == "age" {
						require.True(t, report.Purge.ByAge.Ran)
						require.Len(t, report.Purge.ByAge.RemovalResults, 1)
					} else {
						require.True(t, report.Purge.ByDirSize.Ran)
						require.Len(t, report.Purge.ByDirSize.RemovalResults, 1)
					}
					_, err = os.Lstat(olderPath)
					require.ErrorIs(t, err, os.ErrNotExist)
					// 最新判定とは別に、purgeはリンク自身もその参照先も削除しません。
					_, err = os.Lstat(entryPath)
					require.NoError(t, err)
					if entryKind == "symlink" {
						content, err := os.ReadFile(externalPath)
						require.NoError(t, err)
						require.Equal(t, externalContent, string(content))
					}
				})
			}
		}
	}
}

// currentPeriodLatestFileNameは、最新名を解決できない原因のfilesystem errorを
// ErrInvalidLatestLogと共にerrors.Is・errors.Asで辿れる形で返します。
func TestCurrentPeriodLatestFileNameKeepsDirectoryListingErrorAsCause(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "latest-listing-error"
	notDirectory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(filepath.Dir(notDirectory), 0o750))
	require.NoError(t, os.WriteFile(notDirectory, nil, 0o600))
	fileName := logpattern.FileNamePattern{Dir: notDirectory, Target: target, Extension: "log"}
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: fileName,
		Config: testFileSeriesManagerConfigWithRotation(
			root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, false,
		),
		Clock: pkgclock.NewFixedClock(time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)),
	})

	_, err := manager.currentPeriodLatestFileName(fileName)
	require.ErrorIs(t, err, ErrInvalidLatestLog)
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, notDirectory, pathErr.Path)
}
