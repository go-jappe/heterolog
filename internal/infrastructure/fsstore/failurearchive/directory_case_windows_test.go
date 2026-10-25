//go:build windows

package failurearchive_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/stretchr/testify/require"
)

func TestWindowsFailureArchiveDirectoryCaseChangeFindsExistingFiles(t *testing.T) {
	for _, test := range []struct {
		name   string
		suffix string
	}{
		{name: "target_is_base_directory"},
		{name: "target_is_ancestor_directory", suffix: "State"},
	} {
		t.Run(test.name, func(t *testing.T) {
			// 保存先の解決は既存部分を実体pathへ揃えるため、TEMPが8.3短縮名でも比較できるよう
			// 入力側の一時directoryを先に実体の表記へ解決します。
			parent, err := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, err)
			originalBase := filepath.Join(parent, "service", test.suffix)
			aliasedBase := filepath.Join(parent, "SERVICE", test.suffix)
			archiveDir := filepath.Join("FailureArchive", "OpenSearch")
			originalConfig := durable.FileArchiveConfig{
				Target:                 "service",
				FailureFileArchivePath: originalBase,
				Dir:                    archiveDir,
				DirCaseRetryable:       "Retryable",
				DirCasePermanent:       "Permanent",
				FileNameFormat:         pkgfsstore.FailureArchiveFileNameFormat,
				FileDateFormat:         pkgfsstore.FileDateFormat,
			}
			aliasedConfig := originalConfig
			aliasedConfig.FailureFileArchivePath = aliasedBase
			original := loggerdurable.NewFileArchiveConfigAdapter(originalConfig)
			aliased := loggerdurable.NewFileArchiveConfigAdapter(aliasedConfig)
			require.NoError(t, failurearchive.ValidateLayoutConfigProvider(original))
			require.NoError(t, failurearchive.ValidateLayoutConfigProvider(aliased))

			for _, terminal := range []string{"Retryable", "Permanent"} {
				t.Run(terminal, func(t *testing.T) {
					originalDirectory, err := failurearchive.GetFileArchiveDirectoryPath(original, terminal)
					require.NoError(t, err)
					require.Equal(t, filepath.Join(originalBase, archiveDir, terminal), originalDirectory)
					fileName := "service_20260911_7.jsonl"
					originalFilePath := filepath.Join(originalDirectory, fileName)
					body := []byte("{\"message\":\"existing failure archive\"}\n")
					file, err := failurearchive.OpenFile(originalFilePath)
					require.NoError(t, err)
					_, writeErr := file.Write(body)
					closeErr := file.Close()
					require.NoError(t, writeErr)
					require.NoError(t, closeErr)
					originalInfo, err := os.Stat(originalFilePath)
					require.NoError(t, err)

					// HETEROLOG_APP_STATE_DIRの大小文字だけを変更してもtargetを再付与しません。設定値から求める
					// directoryは呼び出し元のpath表記を保持します。
					wantDirectory := filepath.Join(aliasedBase, archiveDir, terminal)
					resolved := failurearchive.ResolveDirectoryPath(
						aliased.ArchivePath(), aliased.Target(), aliased.Dir(), terminal,
					)
					require.Equal(t, wantDirectory, resolved)
					// 保存先の解決はリンクと同じく大小文字も既存archiveの実体pathへ揃えるため、
					// 元の設定と同じdirectoryへ到達します。
					aliasedDirectory, err := failurearchive.GetFileArchiveDirectoryPath(aliased, terminal)
					require.NoError(t, err)
					require.Equal(t, originalDirectory, aliasedDirectory)

					paths, err := logpattern.GlobFileNames(logpattern.FileNamePattern{
						Dir: aliasedDirectory, Target: aliased.Target(), Extension: "jsonl",
					}, logpattern.GlobByTarget)
					require.NoError(t, err)
					require.Equal(t, []string{filepath.Join(aliasedDirectory, fileName)}, paths)
					// 呼び出し元の表記のpathからも同じ既存fileへ到達します。
					aliasedFilePath := filepath.Join(wantDirectory, fileName)
					aliasedInfo, err := os.Stat(aliasedFilePath)
					require.NoError(t, err)
					require.True(t, os.SameFile(originalInfo, aliasedInfo))
					got, err := os.ReadFile(aliasedFilePath)
					require.NoError(t, err)
					require.Equal(t, body, got)
				})
			}
		})
	}
}
