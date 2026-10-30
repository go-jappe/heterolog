package otlpcommon

import (
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestNewResourceNamesAnUnknownService(t *testing.T) {
	r := NewResource(pkglogger.Resource{})
	if len(r.Attrs) != 1 || !strings.HasPrefix(r.Attrs[0].Value.(string), "unknown_service:") {
		t.Errorf("resource = %+v, want only an unknown_service name", r.Attrs)
	}
}

func TestGroupKeepsTheOrderOfResourcesAndScopes(t *testing.T) {
	type item struct {
		name     string
		resource *telemetrydata.Resource
		scope    string
	}
	defaultResource, other := &telemetrydata.Resource{}, &telemetrydata.Resource{SchemaURL: "other"}
	items := []item{{"a", nil, "x"}, {"b", other, "x"}, {"c", nil, "y"}, {"d", defaultResource, "x"}}
	groups := Group(items, defaultResource,
		func(i *item) *telemetrydata.Resource { return i.resource },
		func(i *item) telemetrydata.Scope { return telemetrydata.Scope{Name: i.scope} },
	)
	// Resourceがnilのものは、defaultResourceのまとまりに入ります。
	if len(groups) != 2 || groups[0].Resource != defaultResource || groups[1].Resource != other ||
		len(groups[0].Scopes) != 2 || len(groups[0].Scopes[0].Items) != 2 || groups[0].Scopes[0].Items[1].name != "d" ||
		groups[0].Scopes[1].Scope.Name != "y" {
		t.Errorf("groups = %+v", groups)
	}
}
