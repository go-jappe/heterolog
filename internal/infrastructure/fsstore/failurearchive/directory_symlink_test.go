package failurearchive

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestFailureArchiveRejectsManagedClassificationSymlinks(t *testing.T) {
	for _, failureCategory := range []string{"retryable", "permanent"} {
		for _, nested := range []bool{false, true} {
			for _, outsideRoot := range []bool{false, true} {
				name := failureCategory + "/direct/inside"
				if nested {
					name = failureCategory + "/ancestor/inside"
				}
				if outsideRoot {
					name += "-outside"
				}
				t.Run(name, func(t *testing.T) {
					cfg := newValidTestFileArchiveConfig(t)
					root := ResolveDirectoryPath(cfg.archivePath, cfg.target, cfg.dir, "")
					require.NoError(t, os.MkdirAll(root, 0o750))
					terminal, link := failureCategory, filepath.Join(root, failureCategory)
					if nested {
						terminal, link = filepath.Join("classified", failureCategory), filepath.Join(root, "classified")
					}
					if failureCategory == "retryable" {
						cfg.dirCaseRetryable = terminal
					} else {
						cfg.dirCasePermanent = terminal
					}
					physical := filepath.Join(root, "physical")
					if outsideRoot {
						physical = t.TempDir()
					}
					require.NoError(t, os.MkdirAll(physical, 0o750))
					marker := filepath.Join(physical, "preserve")
					require.NoError(t, os.WriteFile(marker, []byte("unchanged"), 0o600))
					testutil.Symlink(t, physical, link)
					require.Error(t, ValidateLayoutConfigProvider(cfg))
					require.Error(t, ValidateFileArchiveDirectorySeparationConfigProvider(cfg))
					got, err := GetFileArchiveDirectoryPath(cfg, terminal)
					require.Error(t, err)
					require.Empty(t, got)
					entries, err := os.ReadDir(physical)
					require.NoError(t, err)
					require.Len(t, entries, 1, "rejected classification links must not create descendants")
					body, err := os.ReadFile(marker)
					require.NoError(t, err)
					require.Equal(t, "unchanged", string(body))
				})
			}
		}
	}
}

func TestFailureArchiveAllowsConfiguredRootSymlinkAndPinsGenerationDirectory(t *testing.T) {
	cfg := newValidTestFileArchiveConfig(t)
	configuredRoot := ResolveDirectoryPath(cfg.archivePath, cfg.target, cfg.dir, "")
	require.NoError(t, os.MkdirAll(filepath.Dir(configuredRoot), 0o750))
	physical, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	testutil.Symlink(t, physical, configuredRoot)
	require.NoError(t, ValidateLayoutConfigProvider(cfg))
	resolvedRoot, err := GetFileArchiveDirectoryPath(cfg, "")
	require.NoError(t, err)
	require.Equal(t, physical, resolvedRoot)
	entries, err := os.ReadDir(physical)
	require.NoError(t, err)
	require.Empty(t, entries, "validation must not create classification directories")
	retryable := newTestFileNamePattern(cfg)
	permanent := newTestFileNamePattern(cfg)
	now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
	firstRetryable, err := GetNewRetryableFilePath(&retryable, cfg, now)
	require.NoError(t, err)
	firstPermanent, err := GetNewPermanentFilePath(&permanent, cfg, now)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(physical, "retryable"), filepath.Dir(firstRetryable))
	require.Equal(t, filepath.Join(physical, "permanent"), filepath.Dir(firstPermanent))
	replacement := t.TempDir()
	require.NoError(t, os.Remove(configuredRoot))
	testutil.Symlink(t, replacement, configuredRoot)
	nextRetryable, err := GetNewRetryableFilePath(&retryable, cfg, now.Add(24*time.Hour))
	require.NoError(t, err)
	nextPermanent, err := GetNewPermanentFilePath(&permanent, cfg, now.Add(24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, retryable.Dir, filepath.Dir(nextRetryable))
	require.Equal(t, permanent.Dir, filepath.Dir(nextPermanent))
	require.Equal(t, filepath.Dir(firstRetryable), filepath.Dir(nextRetryable))
	require.Equal(t, filepath.Dir(firstPermanent), filepath.Dir(nextPermanent))
	entries, err = os.ReadDir(replacement)
	require.NoError(t, err)
	require.Empty(t, entries, "rotation must not follow a replaced root alias")
}
