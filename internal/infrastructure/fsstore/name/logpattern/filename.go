package logpattern

import (
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// findDirectoryFileNamesはdirectoryを文字どおり開き、実際のbasenameを照合します。
// 設定値や既存file名に含まれるglob記号を解釈せず、返却pathの表記と名前順を保ちます。
func findDirectoryFileNames(directory string, matchesName func(string) bool) ([]string, error) {
	readDirectory := directory
	if readDirectory == "" {
		readDirectory = "."
	}
	entries, err := fsstore.ReadDirectoryEntries(readDirectory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, entry := range entries {
		if matchesName(entry.Name()) {
			matches = append(matches, filepath.Join(directory, entry.Name()))
		}
	}
	return matches, nil
}
