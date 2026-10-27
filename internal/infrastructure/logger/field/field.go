package field

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/requestctx"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type IDs struct {
	RequestID string
	TraceID   string
	SpanID    string
}

// ExtractIDsAndExtraFieldsは明示的なFieldを、event metadataになるIDと通常の構造化fieldへ
// 分けます。同じkeyが複数回ある場合は後の値だけを残し、その値で分類します。
// request_id・trace_id・span_idは残った値が文字列ならmetadata、それ以外の型なら型を保った
// 構造化fieldになります。slog属性の重複を後の値へ統一してから分類する規則と同じです。
func ExtractIDsAndExtraFields(fields []pkglogger.Field) (IDs, map[string]any) {
	var ids IDs
	extra := make(map[string]any)

	for _, f := range fields {
		if !f.IsDefined() {
			continue
		}
		key := f.Key()
		val := f.Value()

		// 予約keyの後の値は、前の値がmetadataと構造化fieldのどちらに分類されていても
		// 置き換えます。文字列だけをmetadataとし、他の型はkeyを理由に捨てません。
		if id := reservedID(&ids, key); id != nil {
			*id = ""
			delete(extra, key)
			if v, ok := val.(string); ok {
				*id = v
				continue
			}
		}
		extra[key] = val
	}

	return ids, extra
}

// reservedIDはkeyがevent metadataのIDであれば、その保存先を返します。
func reservedID(ids *IDs, key string) *string {
	switch key {
	case "request_id":
		return &ids.RequestID
	case "trace_id":
		return &ids.TraceID
	case "span_id":
		return &ids.SpanID
	default:
		return nil
	}
}

// ResolveIDsAndExtraFieldsはcontextに保持されたIDと明示的なFieldをLogEntry用に統合します。
// Fieldの重複は後の値へ統一してから分類します。同じIDが両方にある場合は、そのログ呼び出しで
// 指定された空でない文字列Fieldを優先し、最後の値が文字列以外ならcontextのIDを使います。
func ResolveIDsAndExtraFields(
	ctx context.Context,
	fields []pkglogger.Field,
) (IDs, map[string]any) {
	contextIDs := requestctx.IDsFromContext(ctx)
	ids := IDs{
		RequestID: contextIDs.RequestID,
		TraceID:   contextIDs.TraceID,
		SpanID:    contextIDs.SpanID,
	}
	fieldIDs, extra := ExtractIDsAndExtraFields(fields)
	if fieldIDs.RequestID != "" {
		ids.RequestID = fieldIDs.RequestID
	}
	if fieldIDs.TraceID != "" {
		ids.TraceID = fieldIDs.TraceID
	}
	if fieldIDs.SpanID != "" {
		ids.SpanID = fieldIDs.SpanID
	}
	if len(extra) == 0 {
		extra = nil
	}
	return ids, extra
}
