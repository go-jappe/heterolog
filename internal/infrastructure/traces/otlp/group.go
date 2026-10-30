package otlp

import (
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// resourceGroupとscopeGroupは、OTLPのResourceSpansとScopeSpansに当たるまとまりです。
type resourceGroup struct {
	resource *tracing.Resource
	scopes   []scopeGroup
}

type scopeGroup struct {
	scope tracing.Scope
	spans []*tracing.SpanData
}

// groupSpansは、spansをResourceとScopeごとにまとめます。Resourceがnilのspanは
// defaultResourceで送ります。まとまりとspanの順は、最初に現れた順です。
func groupSpans(spans []tracing.SpanData, defaultResource *tracing.Resource) []resourceGroup {
	var groups []resourceGroup
	resourceIndex := make(map[*tracing.Resource]int)
	type scopeKey struct {
		resource int
		scope    tracing.Scope
	}
	scopeIndex := make(map[scopeKey]int)
	for i := range spans {
		span := &spans[i]
		resource := span.Resource
		if resource == nil {
			resource = defaultResource
		}
		ri, ok := resourceIndex[resource]
		if !ok {
			ri = len(groups)
			resourceIndex[resource] = ri
			groups = append(groups, resourceGroup{resource: resource})
		}
		key := scopeKey{resource: ri, scope: span.Scope}
		si, ok := scopeIndex[key]
		if !ok {
			si = len(groups[ri].scopes)
			scopeIndex[key] = si
			groups[ri].scopes = append(groups[ri].scopes, scopeGroup{scope: span.Scope})
		}
		groups[ri].scopes[si].spans = append(groups[ri].scopes[si].spans, span)
	}
	return groups
}

// NewResourceは、ログと同じResourceの設定を、OpenTelemetryのsemantic conventionsの属性に
// します。名前は、ログのOTel形式（internal/infrastructure/logger/otel）と同じです。
// 空の値は送りません。ただし、service.nameは必須のため、空ならOpenTelemetry SDKと同じく
// unknown_service:<実行fileの名前>にします。
func NewResource(r pkglogger.Resource) *tracing.Resource {
	var attrs []pkgobserver.Attr
	add := func(key, value string) {
		if value != "" {
			attrs = append(attrs, pkgobserver.Attr{Key: key, Value: value})
		}
	}
	serviceName := r.ServiceName
	if serviceName == "" {
		serviceName = "unknown_service:" + filepath.Base(os.Args[0])
	}
	add("service.name", serviceName)
	add("service.version", r.ServiceVersion)
	add("host.name", r.HostName)
	add("os.type", r.OSType)
	add("deployment.environment", r.Environment)
	add("heterolog.runtime.type", r.RuntimeType)
	if r.Cloud != nil {
		add("service.instance.id", r.Cloud.RuntimeInstanceID)
		add("cloud.provider", r.Cloud.Provider)
		add("cloud.region", r.Cloud.Region)
	}
	return &tracing.Resource{Attrs: attrs}
}

// otlpSpanKindは、pkg/observerのSpanKindを、OTLPのSpan.SpanKind（UNSPECIFIEDが0）に
// します。
func otlpSpanKind(kind pkgobserver.SpanKind) int {
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

// otlpStatusCodeは、pkg/observerのStatusCodeを、OTLPのStatus.StatusCodeにします。
func otlpStatusCode(code pkgobserver.StatusCode) int {
	switch code {
	case pkgobserver.StatusOK:
		return 1
	case pkgobserver.StatusError:
		return 2
	default:
		return 0
	}
}
