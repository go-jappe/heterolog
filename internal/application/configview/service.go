package configview

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

// 設定の持ち主の種類です。文書のkindの値になります。
const (
	KindLogger         = "logger"
	KindAsyncLogger    = "async_logger"
	KindDeliveryLogger = "delivery_logger"
	KindTelemetry      = "telemetry"
)

// ErrUnknownKindは、SourceがConfigurationで上のどれでもない種類を返したことを表します。
var ErrUnknownKind = errors.New("unknown configuration kind")

// Configurationは、持ち主の種類と、持ち主を作った設定の構造体（pkg/loggerのLoggerConfig
// など）です。
type Configuration struct {
	Kind  string
	Value any
}

// Sourceは、自分を作った設定を示せるloggerやtelemetryです。heterologが作るruntimeが
// 実装します。
type Source interface {
	Configuration() Configuration
}

// Serviceは、名前を付けた複数の持ち主の設定を、一つの文書として返します。設定は作った
// 後に変わらないため、文書はNewServiceで一度だけ組み立てます。
type Service struct {
	document map[string]any
}

// NewServiceは、sourcesの設定の文書を組み立てたServiceを返します。文書は次の形で、
// loggersにはlogger・async_logger・delivery_loggerの持ち主、telemetriesにはtelemetryの
// 持ち主が入ります。
//
//	{"loggers": {"<name>": {"kind": "logger", "config": {...}}}, "telemetries": {...}}
//
// 種類が上のどれでもない持ち主があれば、ErrUnknownKindを返します。
func NewService(sources map[string]Source) (*Service, error) {
	loggers := map[string]any{}
	telemetries := map[string]any{}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		cfg := sources[name].Configuration()
		entry := map[string]any{"kind": cfg.Kind, "config": Render(cfg.Value)}
		switch cfg.Kind {
		case KindLogger, KindAsyncLogger, KindDeliveryLogger:
			loggers[name] = entry
		case KindTelemetry:
			telemetries[name] = entry
		default:
			return nil, fmt.Errorf("%w: %q of %q", ErrUnknownKind, cfg.Kind, name)
		}
	}
	return &Service{document: map[string]any{"loggers": loggers, "telemetries": telemetries}}, nil
}

// Documentは、設定の文書を返します。呼び出し元が変えないよう、最上位は複製します。
func (s *Service) Document() map[string]any {
	return maps.Clone(s.document)
}
