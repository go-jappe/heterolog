package logging

import (
	"runtime"
)

const startStackTraceIndex = 2 // GetCaller, CollectCallerDetailsをログから除外

// CaptureStackは現在のgoroutineのstackを保存します。skipはこのmethodの呼び出し元から
// 除外するframe数です。symbolの展開と表示深度の適用は各backendで行います。
func (entry *LogEntry) CaptureStack(skip int) {
	for size := 32; ; size *= 2 {
		pcs := make([]uintptr, size)
		n := runtime.Callers(skip+2, pcs)
		if n < len(pcs) {
			entry.stackPCs = pcs[:n]
			return
		}
	}
}

// EnsureEntryStackTraceは保存済みの呼び出し元stackをbackendの深度で展開します。
// depthは従来同様、呼び出し位置に加えて表示する親frame数です（0なら呼び出し位置のみ）。
// 構築済みの表示情報は保持します。physical handlerへ直接渡されたentryだけは、
// そのhandlerの呼び出し位置でstackを取得します。
func EnsureEntryStackTrace(entry *LogEntry, depth int) {
	if len(entry.FuncName) != 0 || len(entry.File) != 0 || len(entry.Line) != 0 || depth < 0 {
		return
	}
	if entry.stackPCs == nil {
		entry.CaptureStack(1)
	}
	if len(entry.stackPCs) == 0 {
		return
	}
	frames := runtime.CallersFrames(entry.stackPCs)
	for i := 0; i <= depth; i++ {
		frame, more := frames.Next()
		entry.FuncName = append(entry.FuncName, frame.Function)
		entry.File = append(entry.File, frame.File)
		entry.Line = append(entry.Line, frame.Line)
		if !more {
			break
		}
	}
}

// 呼び出し元を取得する関数
func GetCaller(skip int) (string, string, int, bool) {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown", "unknown", 0, false
	}
	funcName := runtime.FuncForPC(pc).Name()
	return funcName, file, line, ok
}

// Stacktraceを取得
func CollectCallerDetails(depth int) ([]string, []string, []int) {
	funcNames, sourceFiles, lines := []string{}, []string{}, []int{}
	// depthはgetcacller,collectcallderdetailsを除いた深さ
	for i := startStackTraceIndex; i <= depth+startStackTraceIndex; i++ {
		funcName, sourceFile, line, ok := GetCaller(i)
		if !ok {
			break
		}
		funcNames = append(funcNames, funcName)
		sourceFiles = append(sourceFiles, sourceFile)
		lines = append(lines, line)
	}
	return funcNames, sourceFiles, lines
}

// スタックトレースの階層の深度を返す
func GetCallerDepth(cfg StackTraceConfigProvider) int {
	if cfg.IsDebug() {
		return cfg.ErrorTraceDepthDebugMode()
	}
	return cfg.ErrorTraceDepth()
}

type StackTraceConfigProvider interface {
	IsDebug() bool
	ErrorTraceDepthDebugMode() int
	ErrorTraceDepth() int
}

// buildStackTraceArray はスタックトレース情報を配列として構築します
func BuildStackTraceArray(entry LogEntry) []map[string]interface{} {
	traces := make([]map[string]interface{}, len(entry.FuncName))
	for j, funcName := range entry.FuncName {
		trace := map[string]interface{}{
			"function": funcName,
		}
		if j < len(entry.File) {
			trace["file"] = entry.File[j]
		}
		if j < len(entry.Line) {
			trace["line"] = entry.Line[j]
		}
		traces[j] = trace
	}
	return traces
}
