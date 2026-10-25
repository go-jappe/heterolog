package jsonutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// UnmarshalPreservingNumbers decodes one complete JSON value, preserving numbers
// in interface values as json.Number. Like json.Unmarshal, it rejects trailing
// values or malformed data instead of accepting a valid prefix.
func UnmarshalPreservingNumbers(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected JSON value after payload")
	}
	return nil
}
