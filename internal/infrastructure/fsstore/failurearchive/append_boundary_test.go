package failurearchive

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenArchiveRequiresCompleteAppendBoundaryWithoutDiscardingTail(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
		valid   bool
	}{
		{"empty", "", true},
		{"complete records", "{\"id\":1}\n{\"id\":2}\n", true},
		{"partial first record", "{\"id", false},
		{"partial following record", "{\"id\":1}\n{\"id", false},
		{"missing record separator", "{\"id\":1}", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "archive.jsonl")
			require.NoError(t, os.WriteFile(path, []byte(test.content), 0o640))
			file, err := OpenFile(path)
			if !test.valid {
				require.Nil(t, file)
				require.ErrorIs(t, err, ErrIncompleteJSONLTail)
			} else {
				require.NoError(t, err)
				require.NoError(t, file.Close())
			}
			unchanged, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, test.content, string(unchanged))
		})
	}
}
