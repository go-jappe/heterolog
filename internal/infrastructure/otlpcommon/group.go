package otlpcommon

import (
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ResourceGroupとScopeGroupは、OTLPのResourceSpansとScopeSpans（metricsでは
// ResourceMetricsとScopeMetrics）に当たるまとまりです。
type ResourceGroup[T any] struct {
	Resource *telemetrydata.Resource
	Scopes   []ScopeGroup[T]
}

type ScopeGroup[T any] struct {
	Scope telemetrydata.Scope
	Items []*T
}

// Groupは、itemsをResourceとScopeごとにまとめます。Resourceがnilのものは
// defaultResourceで送ります。まとまりと並びは、最初に現れた順です。
func Group[T any](
	items []T,
	defaultResource *telemetrydata.Resource,
	resourceOf func(*T) *telemetrydata.Resource,
	scopeOf func(*T) telemetrydata.Scope,
) []ResourceGroup[T] {
	var groups []ResourceGroup[T]
	resourceIndex := make(map[*telemetrydata.Resource]int)
	type scopeKey struct {
		resource int
		scope    telemetrydata.Scope
	}
	scopeIndex := make(map[scopeKey]int)
	for i := range items {
		item := &items[i]
		resource := resourceOf(item)
		if resource == nil {
			resource = defaultResource
		}
		ri, ok := resourceIndex[resource]
		if !ok {
			ri = len(groups)
			resourceIndex[resource] = ri
			groups = append(groups, ResourceGroup[T]{Resource: resource})
		}
		scope := scopeOf(item)
		key := scopeKey{resource: ri, scope: scope}
		si, ok := scopeIndex[key]
		if !ok {
			si = len(groups[ri].Scopes)
			scopeIndex[key] = si
			groups[ri].Scopes = append(groups[ri].Scopes, ScopeGroup[T]{Scope: scope})
		}
		groups[ri].Scopes[si].Items = append(groups[ri].Scopes[si].Items, item)
	}
	return groups
}

// NewResourceは、ログと同じResourceの設定を、OpenTelemetryのsemantic conventionsの属性に
// します。名前は、ログのOTel形式（internal/infrastructure/logger/otel）と同じです。
// 空の値は送りません。ただし、service.nameは必須のため、空ならOpenTelemetry SDKと同じく
// unknown_service:<実行fileの名前>にします。
func NewResource(r pkglogger.Resource) *telemetrydata.Resource {
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
	return &telemetrydata.Resource{Attrs: attrs}
}
