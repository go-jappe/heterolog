package formatter

import (
	"bytes"
	"encoding/json"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"strconv"
	"strings"
	"sync"
)

// BuildStacktraceString はスタックトレース情報を文字列として構築します
func BuildStacktraceString(entry *logging.LogEntry) string {
	if len(entry.File) == 0 {
		return ""
	}

	var stacktraceBuilder strings.Builder
	stacktraceBuilder.WriteString(" (stacktrace):")
	for i := range entry.File {
		stacktraceBuilder.WriteString(" (")
		stacktraceBuilder.WriteString(strconv.Itoa(i + 1))
		stacktraceBuilder.WriteString(") at: (File: ")
		stacktraceBuilder.WriteString(entry.File[i])
		stacktraceBuilder.WriteString(" | Line: ")
		stacktraceBuilder.WriteString(strconv.Itoa(entry.Line[i]))
		stacktraceBuilder.WriteString(" | FuncName: ")
		stacktraceBuilder.WriteString(entry.FuncName[i])
		stacktraceBuilder.WriteString(")")
	}
	return stacktraceBuilder.String()
}

// buildFieldsString はフィールド情報をJSON文字列として構築します
func BuildFieldsString(entry *logging.LogEntry) string {
	if entry.Fields == nil {
		return ""
	}

	var fieldsBuilder strings.Builder
	var jsonBufPool = sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}

	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	// json.Marshal(buf) 相当
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(entry.Fields); err != nil {
		fieldsBuilder.WriteString("[marshal error:")
		fieldsBuilder.WriteString(err.Error())
		fieldsBuilder.WriteByte(']')
	} else {
		// Encode() は末尾に `\n` がつくのでトリム
		data := buf.Bytes()
		if len(data) > 0 && data[len(data)-1] == '\n' {
			data = data[:len(data)-1]
		}
		fieldsBuilder.Write(data)
	}
	jsonBufPool.Put(buf)

	return fieldsBuilder.String()
}
