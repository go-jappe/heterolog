package logger

import (
	"encoding/json"
	"fmt"
)

// NewSlogField snapshots the value of a log/slog attribute as a Field. Unlike the
// field constructors, it accepts any key, including the empty key, as log/slog
// does; use it when adapting slog records. Like Any, it returns
// ErrFieldValueNotJSON for a value that cannot be encoded or that nests deeper
// than MaxFieldNestingDepth.
func NewSlogField(key string, value any) (Field, error) {
	encoded, err := json.Marshal(value)
	if err == nil {
		err = validateNestingDepth(encoded)
	}
	if err != nil {
		// JSON化のerrorは記録しようとした値やMarshalJSONが返したerrorを保持し得るため、
		// errors.Asで取り出せないよう原因としては包みません。種類はErrFieldValueNotJSONで判定します。
		return Field{}, fmt.Errorf("%w: slog attribute %q: %v", ErrFieldValueNotJSON, key, err)
	}
	return Field{
		key:             key,
		value:           jsonSnapshot(encoded),
		emptyKeyDefined: key == "",
	}, nil
}
