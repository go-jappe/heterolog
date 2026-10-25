package logpattern

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// recordedGenerationTestPatternは、size上限に達していない空のrawを置いたpatternを返します。
// 記録済みでない最新rawへは、従来どおり追記を続けられることも確認します。
func recordedGenerationTestPattern(t *testing.T, date string, existing ...string) FileNamePattern {
	t.Helper()
	directory := t.TempDir()
	for _, name := range existing {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), nil, 0o600))
	}
	pattern := FileNamePattern{Dir: directory, Target: "app", Date: date, Extension: "log"}
	require.NoError(t, ResolveMaxIndex(&pattern))
	return pattern
}

func TestAdvancePastRecordedGenerationDoesNotReuseRecordedIndexes(t *testing.T) {
	cfg := rotationNameTestConfig{rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat}
	at := time.Date(2026, time.October, 22, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		existing  []string
		period    string
		index     uint64
		wantDate  string
		wantIndex int
	}{
		// 手動復旧で最新fileまで送信・削除され、directoryが空になった状態です。
		{name: "empty directory", period: "20261022", index: 2, wantDate: "20261022", wantIndex: 3},
		// 送信済みの世代と同じ番号のrawが残っていても、その末尾へ追記しません。
		{name: "recorded raw remains", existing: []string{"app_20261022_2.log"}, period: "20261022", index: 2, wantDate: "20261022", wantIndex: 3},
		{name: "later file exists", existing: []string{"app_20261022_6.log"}, period: "20261022", index: 2, wantDate: "20261022", wantIndex: 6},
		{name: "older recorded period", period: "20261021", index: 9, wantDate: "20261022", wantIndex: DefaultFileIndex},
		// system clockが記録済みの期間より前へ戻っても、その期間を続けます。
		{name: "recorded period ahead of clock", existing: []string{"app_20261023_2.log"}, period: "20261023", index: 1, wantDate: "20261023", wantIndex: 2},
		{name: "recorded period ahead without files", period: "20261023", index: 4, wantDate: "20261023", wantIndex: 5},
		{name: "no record", wantDate: "20261022", wantIndex: DefaultFileIndex},
	} {
		t.Run(test.name, func(t *testing.T) {
			pattern := recordedGenerationTestPattern(t, "20261022", test.existing...)
			require.NoError(t, AdvancePastRecordedGeneration(&pattern, test.period, test.index))
			require.Equal(t, test.wantDate, pattern.Date)
			require.Equal(t, test.wantIndex, pattern.Index)

			// 以後のpath選択も、進めた世代より前へ戻りません。
			path, err := ResolveFilePathAt(&pattern, cfg, at)
			require.NoError(t, err)
			wantPattern := pattern
			wantPattern.Date, wantPattern.Index = test.wantDate, test.wantIndex
			require.Equal(t, BuildFileFullPath(test.wantDate, wantPattern, cfg), path)
			require.Equal(t, test.wantIndex, pattern.Index)
		})
	}
}

func TestAdvancePastRecordedGenerationRejectsIndexWithoutSuccessor(t *testing.T) {
	pattern := recordedGenerationTestPattern(t, "20261022")
	before := pattern
	require.Error(t, AdvancePastRecordedGeneration(&pattern, "20261022", uint64(math.MaxInt)))
	require.Equal(t, before, pattern)
	require.Error(t, AdvancePastRecordedGeneration(&pattern, "2026-10-22", 1))
	require.Equal(t, before, pattern)
}

func TestResolveFilePathAtDoesNotReturnBelowActiveIndexInSamePeriod(t *testing.T) {
	cfg := rotationNameTestConfig{rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat}
	pattern := FileNamePattern{Dir: t.TempDir(), Target: "app", Date: "20261022", Extension: "log", Index: 5}
	// 古い世代のfileが残っていない場合も、現在のIndexより前の番号を選びません。
	path, err := ResolveFilePathAt(&pattern, cfg, time.Date(2026, time.October, 22, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(pattern.Dir, "app_20261022_5.log"), path)
	require.Equal(t, 5, pattern.Index)
	// 新しい期間へ進む場合は、従来どおり最初の番号から選びます。
	path, err = ResolveFilePathAt(&pattern, cfg, time.Date(2026, time.October, 23, 0, 30, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(pattern.Dir, "app_20261023_1.log"), path)
	require.Equal(t, DefaultFileIndex, pattern.Index)
}
