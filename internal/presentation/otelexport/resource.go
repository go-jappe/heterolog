package otelexport

import (
	"sync"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// maxCachedResourcesは、変換したResourceを覚えておく数の上限です。providerは1つの
// Resourceを使い続けるため、通常はproviderの数だけ覚えます。
const maxCachedResources = 64

// resourceCacheは、SDKのResourceを変換した結果を覚えます。同じResourceのspanやmetricに
// 同じpointerを付け、送るときに1つのResourceSpansやResourceMetricsへまとめるためです。
// zero値から使えます。
type resourceCache struct {
	mu        sync.Mutex
	resources map[*resource.Resource]*telemetrydata.Resource
}

// convertは、SDKのResourceを変換します。nilと空のResourceは、NewTelemetryに設定した
// Resourceで送るため、nilにします。
func (c *resourceCache) convert(r *resource.Resource) *telemetrydata.Resource {
	if r == nil || r.Len() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if converted, ok := c.resources[r]; ok {
		return converted
	}
	converted := &telemetrydata.Resource{Attrs: toAttrs(r.Attributes()), SchemaURL: r.SchemaURL()}
	if c.resources == nil {
		c.resources = make(map[*resource.Resource]*telemetrydata.Resource)
	}
	if len(c.resources) < maxCachedResources {
		c.resources[r] = converted
	}
	return converted
}
