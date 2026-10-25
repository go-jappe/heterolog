package formatter

import (
	"encoding/json"
	"fmt"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/otel"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
)

// Formatterは、一つのLogEntryを出力先へ渡す文字列へ変換します。
// 時刻表示を必要とする実装は、その表示規則を構築時に保持します。
type Formatter interface {
	Format(entry *logging.LogEntry) string
}

var _ Formatter = (*OtelFormatter)(nil)

// OtelFormatterはLogEntryをOpenTelemetry log record形式のJSONへ変換します。
// Resourceは省略可能です。zero値とnil receiverはresourceを持たないlog recordを出力し、
// level、本文、error、fieldsを省略しません。
type OtelFormatter struct {
	Resource resource.OtelLogResourceProvider
}

func (o *OtelFormatter) Format(entry *logging.LogEntry) string {
	var otelResource resource.OtelLogResourceProvider
	if o != nil {
		otelResource = o.Resource
	}
	jsonBytes, err := json.Marshal(otel.ConvertOtelLogEntryToMap(otel.ToOtelLogEntry(entry, otelResource)))
	if err != nil {
		return fmt.Sprintf("[FORMATTER][ERROR] JSON変換エラー:%v", err)
	}
	return string(jsonBytes)
}
