package logpattern

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestResolveFilePathAtAdvancesPastLatestSymlink(t *testing.T) {
	for _, kind := range []string{"file", "dangling", "directory", "loop"} {
		t.Run(kind, func(t *testing.T) {
			const date = "20260913"
			pattern := FileNamePattern{
				Dir: t.TempDir(), Target: "app", Date: date, Extension: "log",
			}
			cfg := rotationNameTestConfig{
				rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat,
			}
			linkPath := filepath.Join(pattern.Dir, "app_"+date+"_99.log")
			targetPath := filepath.Join(t.TempDir(), "target")
			switch kind {
			case "file":
				// サイズだけなら再利用できる空ファイルでも、リンク経由では開きません。
				require.NoError(t, os.WriteFile(targetPath, nil, 0o600))
			case "directory":
				require.NoError(t, os.Mkdir(targetPath, 0o700))
			case "loop":
				targetPath = linkPath
			}
			testutil.Symlink(t, targetPath, linkPath)

			// 世代番号の正本は引き続きファイル名です。リンクの99も使用済みとします。
			index, err := FindMaxIndex(pattern)
			require.NoError(t, err)
			require.Equal(t, 99, index)
			at := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
			path, err := ResolveFilePathAt(&pattern, cfg, at)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(pattern.Dir, "app_"+date+"_100.log"), path)
			require.Equal(t, 100, pattern.Index)
			require.Equal(t, date, pattern.Date)
			info, err := os.Lstat(linkPath)
			require.NoError(t, err)
			require.NotZero(t, info.Mode()&os.ModeSymlink)
			if kind == "dangling" {
				_, err := os.Lstat(targetPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestSelectWritableFilePathSkipsEachSymlinkCandidate(t *testing.T) {
	for _, startWithRegularFile := range []bool{false, true} {
		name := "consecutive links"
		if startWithRegularFile {
			name = "small regular file before dangling link"
		}
		t.Run(name, func(t *testing.T) {
			const date = "20260913"
			pattern := FileNamePattern{
				Dir: t.TempDir(), Target: "app", Date: date, Extension: "log", Index: 99,
			}
			cfg := rotationNameTestConfig{
				rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat,
			}
			firstPath := BuildFileFullPath(date, pattern, cfg)
			missingTarget := filepath.Join(t.TempDir(), "missing-target")
			if startWithRegularFile {
				require.NoError(t, os.WriteFile(firstPath, nil, 0o600))
			} else {
				testutil.Symlink(t, missingTarget, firstPath)
			}
			secondPath := filepath.Join(pattern.Dir, "app_"+date+"_100.log")
			testutil.Symlink(t, missingTarget, secondPath)
			thirdPath := filepath.Join(pattern.Dir, "app_"+date+"_101.log")
			testutil.Symlink(t, thirdPath, thirdPath)

			path, err := selectWritableFilePathForDate(firstPath, &pattern, cfg, date)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(pattern.Dir, "app_"+date+"_102.log"), path)
			require.Equal(t, 102, pattern.Index)
			// 走査中に見つかった次世代のリンクも、リンク先を辿らず使用済みと判定します。
			for _, linkPath := range []string{secondPath, thirdPath} {
				info, err := os.Lstat(linkPath)
				require.NoError(t, err)
				require.NotZero(t, info.Mode()&os.ModeSymlink)
			}
			_, err = os.Lstat(missingTarget)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
