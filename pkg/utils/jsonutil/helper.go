package jsonutil

import (
	"encoding/json"
	"fmt"
	"strings"
)

func SerializeDocs(docs []map[string]any, max int) string {
	var b strings.Builder
	for i := 0; i < len(docs) && i < max; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		data, err := json.Marshal(docs[i])
		if err != nil {
			b.WriteString(fmt.Sprintf("{\"marshal_error\":\"%v\"}", err))
		} else {
			b.Write(data)
		}
	}
	if len(docs) > max {
		b.WriteString(fmt.Sprintf(", ...(and %d more)", len(docs)-max))
	}
	return b.String()
}

func MapAnytoJsonRawMessage(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}
