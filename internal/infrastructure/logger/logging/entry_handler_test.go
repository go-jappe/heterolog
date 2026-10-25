package logging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneLogEntryCopiesMutableContainers(t *testing.T) {
	original := &LogEntry{
		File:     []string{"source.go"},
		Line:     []int{10},
		FuncName: []string{"source"},
		Fields:   map[string]any{"key": "original"},
	}

	cloned := CloneLogEntry(original)
	cloned.File[0] = "clone.go"
	cloned.Line[0] = 20
	cloned.FuncName[0] = "clone"
	cloned.Fields["key"] = "cloned"

	require.Equal(t, []string{"source.go"}, original.File)
	require.Equal(t, []int{10}, original.Line)
	require.Equal(t, []string{"source"}, original.FuncName)
	require.Equal(t, map[string]any{"key": "original"}, original.Fields)
}

func TestCloneLogEntryPreservesNilAndEmptySlices(t *testing.T) {
	nilSlices := CloneLogEntry(&LogEntry{})
	require.Nil(t, nilSlices.File)
	require.Nil(t, nilSlices.Line)
	require.Nil(t, nilSlices.FuncName)

	emptySlices := CloneLogEntry(&LogEntry{
		File:     []string{},
		Line:     []int{},
		FuncName: []string{},
	})
	require.NotNil(t, emptySlices.File)
	require.NotNil(t, emptySlices.Line)
	require.NotNil(t, emptySlices.FuncName)
	require.Empty(t, emptySlices.File)
	require.Empty(t, emptySlices.Line)
	require.Empty(t, emptySlices.FuncName)
}
