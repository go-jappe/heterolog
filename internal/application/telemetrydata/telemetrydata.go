// Package telemetrydataは、spanとmetricsに共通する記録の部品です。どのprocessが送ったかを
// 表すResourceと、どの計装が作ったかを表すScopeを定義します。
package telemetrydata

import pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

// Scopeは、記録を作った計装（OpenTelemetryのinstrumentation scope）です。
type Scope struct {
	Name    string
	Version string
}

// Resourceは、記録を送ったprocessを表す属性です。記録のResourceがnilの場合は、配送の
// 仕組みに設定したResourceで送ります。
type Resource struct {
	Attrs        []pkgobserver.Attr
	DroppedAttrs int
	SchemaURL    string
}
