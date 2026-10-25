package otel

import (
	"reflect"
	"strings"
)

// OtelLogEntryをmapへ変換
func ConvertOtelLogEntryToMap(entry OtelLogEntry) map[string]interface{} {
	return structToMap(entry)
}

func structToMap(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return result
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return result
	}

	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// JSONタグ解析（omitempty対応）
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		parts := strings.Split(jsonTag, ",")
		key := parts[0]
		omitempty := false
		for _, opt := range parts[1:] {
			if opt == "omitempty" {
				omitempty = true
				break
			}
		}

		// 値が空でomitemptyならスキップ
		if omitempty && isEmptyValue(value) {
			continue
		}

		// 再帰処理または直接格納
		switch value.Kind() {
		case reflect.Struct:
			result[key] = structToMap(value.Interface())
		case reflect.Ptr:
			if !value.IsNil() {
				elem := value.Elem()
				if elem.Kind() == reflect.Struct {
					result[key] = structToMap(elem.Interface())
				} else {
					result[key] = elem.Interface()
				}
			}
		case reflect.Map:
			if value.Len() > 0 {
				// 必要であれば map[string]any 内も再帰する処理を追加可能
				result[key] = value.Interface()
			}
		default:
			result[key] = value.Interface()
		}
	}

	return result
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
