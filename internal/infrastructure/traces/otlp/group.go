package otlp

import pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

// SpanKindは、pkg/observerのSpanKindを、OTLPのSpan.SpanKind（UNSPECIFIEDが0）に
// します。
func SpanKind(kind pkgobserver.SpanKind) int {
	switch kind {
	case pkgobserver.SpanKindServer:
		return 2
	case pkgobserver.SpanKindClient:
		return 3
	case pkgobserver.SpanKindProducer:
		return 4
	case pkgobserver.SpanKindConsumer:
		return 5
	default:
		return 1
	}
}

// StatusCodeは、pkg/observerのStatusCodeを、OTLPのStatus.StatusCodeにします。
func StatusCode(code pkgobserver.StatusCode) int {
	switch code {
	case pkgobserver.StatusOK:
		return 1
	case pkgobserver.StatusError:
		return 2
	default:
		return 0
	}
}
