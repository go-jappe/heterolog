package logging

import (
	"reflect"
	"strings"
	"testing"
)

func captureOriginForTest(entry *LogEntry, remaining int) {
	if remaining > 0 {
		captureOriginForTest(entry, remaining-1)
		return
	}
	entry.CaptureStack(0)
}

func TestCapturedStackPreservesOriginAcrossWorkersAndBackendDepths(t *testing.T) {
	original := &LogEntry{StackTraceRequested: true}
	// A deep application stack also checks that capture is not silently
	// limited to the initial buffer when a backend requests more frames.
	captureOriginForTest(original, 40)
	for _, depth := range []int{0, 2, 40} {
		entry := CloneLogEntry(original)
		done := make(chan struct{})
		go func() {
			defer close(done)
			EnsureEntryStackTrace(entry, depth)
		}()
		<-done
		if len(entry.FuncName) != depth+1 || len(entry.File) != depth+1 || len(entry.Line) != depth+1 {
			t.Fatalf("depth %d: inconsistent stack: %+v", depth, entry)
		}
		for i, function := range entry.FuncName {
			if !strings.HasSuffix(function, ".captureOriginForTest") || !strings.HasSuffix(entry.File[i], "stacktrace_test.go") || entry.Line[i] <= 0 {
				t.Fatalf("depth %d: lost application frame %d: %+v", depth, i, entry)
			}
		}
	}
	if len(original.FuncName) != 0 || len(original.File) != 0 || len(original.Line) != 0 {
		t.Fatal("backend formatting changed the shared application entry")
	}
}

func TestEnsureEntryStackTracePreservesCompletedDetails(t *testing.T) {
	entry := &LogEntry{FuncName: []string{"original"}, File: []string{"original.go"}, Line: []int{42}}
	before := CloneLogEntry(entry)
	EnsureEntryStackTrace(entry, 0)
	if !reflect.DeepEqual(before, entry) {
		t.Fatalf("completed details changed: %+v", entry)
	}
}
