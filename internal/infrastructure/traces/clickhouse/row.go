// Package clickhouseは、spanをClickHouseの表へ書く送り先です。表は、OpenTelemetry Collectorの
// ClickHouse exporterが書くotel_tracesと同じ列を持ちます。GrafanaのClickHouseのdata sourceなど、
// その表を読む道具で、spanを見られるようにします。spanは、1件を1行にして、HTTPの
// INSERT（JSONEachRow）で書きます。
package clickhouse

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// spanKindNamesとstatusCodeNamesは、OTLPのSpan.SpanKindとStatus.StatusCodeの値の名前です。
// Collectorと同じく、OpenTelemetry Collectorのpdataの書き方（Serverなど）にします。
var (
	spanKindNames   = [...]string{"Unspecified", "Internal", "Server", "Client", "Producer", "Consumer"}
	statusCodeNames = [...]string{"Unset", "Ok", "Error"}
)

// rowは、otel_tracesの1行です。Nestedの列（EventsとLinks）は、列ごとの配列として書きます。
type row struct {
	Timestamp          string              `json:"Timestamp"`
	TraceID            string              `json:"TraceId"`
	SpanID             string              `json:"SpanId"`
	ParentSpanID       string              `json:"ParentSpanId"`
	TraceState         string              `json:"TraceState"`
	SpanName           string              `json:"SpanName"`
	SpanKind           string              `json:"SpanKind"`
	ServiceName        string              `json:"ServiceName"`
	ResourceAttributes map[string]string   `json:"ResourceAttributes"`
	ScopeName          string              `json:"ScopeName"`
	ScopeVersion       string              `json:"ScopeVersion"`
	SpanAttributes     map[string]string   `json:"SpanAttributes"`
	Duration           int64               `json:"Duration"`
	StatusCode         string              `json:"StatusCode"`
	StatusMessage      string              `json:"StatusMessage"`
	EventsTimestamp    []string            `json:"Events.Timestamp"`
	EventsName         []string            `json:"Events.Name"`
	EventsAttributes   []map[string]string `json:"Events.Attributes"`
	LinksTraceID       []string            `json:"Links.TraceId"`
	LinksSpanID        []string            `json:"Links.SpanId"`
	LinksTraceState    []string            `json:"Links.TraceState"`
	LinksAttributes    []map[string]string `json:"Links.Attributes"`
}

// marshalRowsは、spanを、JSONEachRowの本文（1行に1件のJSONのobject）にします。
// defaultResourceは、Resourceを持たないspanに付けます。
func marshalRows(spans []tracing.SpanData, defaultResource *tracing.Resource) []byte {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	for _, span := range spans {
		// rowは文字列・数・文字列のmapだけを持つため、Encodeは失敗しません。
		_ = encoder.Encode(newRow(span, defaultResource))
	}
	return buf.Bytes()
}

func newRow(span tracing.SpanData, defaultResource *tracing.Resource) row {
	resource := span.Resource
	if resource == nil {
		resource = defaultResource
	}
	r := row{
		Timestamp:        clickHouseTime(span.StartTime),
		TraceID:          span.TraceID.String(),
		SpanID:           span.SpanID.String(),
		SpanName:         span.Name,
		SpanKind:         spanKindNames[otlp.SpanKind(span.Kind)],
		SpanAttributes:   attributes(span.Attrs),
		ScopeName:        span.Scope.Name,
		ScopeVersion:     span.Scope.Version,
		Duration:         max(span.EndTime.Sub(span.StartTime).Nanoseconds(), 0),
		StatusCode:       statusCodeNames[otlp.StatusCode(span.Status)],
		StatusMessage:    span.StatusMessage,
		EventsTimestamp:  make([]string, len(span.Events)),
		EventsName:       make([]string, len(span.Events)),
		EventsAttributes: make([]map[string]string, len(span.Events)),
		LinksTraceID:     make([]string, len(span.Links)),
		LinksSpanID:      make([]string, len(span.Links)),
		LinksTraceState:  make([]string, len(span.Links)),
		LinksAttributes:  make([]map[string]string, len(span.Links)),
	}
	if !span.ParentSpanID.IsZero() {
		r.ParentSpanID = span.ParentSpanID.String()
	}
	r.ResourceAttributes = map[string]string{}
	if resource != nil {
		r.ResourceAttributes = attributes(resource.Attrs)
		r.ServiceName = r.ResourceAttributes["service.name"]
	}
	for i, event := range span.Events {
		r.EventsTimestamp[i] = clickHouseTime(event.Time)
		r.EventsName[i] = event.Name
		r.EventsAttributes[i] = attributes(event.Attrs)
	}
	for i, link := range span.Links {
		r.LinksTraceID[i] = link.TraceID.String()
		r.LinksSpanID[i] = link.SpanID.String()
		r.LinksAttributes[i] = attributes(link.Attrs)
	}
	return r
}

// attributesは、属性を、Map(String, String)の列に入れる文字列のmapにします。
func attributes(attrs []pkgobserver.Attr) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		out[attr.Key] = otlpcommon.StringValue(attr.Value)
	}
	return out
}

// clickHouseTimeは、時刻を、Unix epochからの秒と9桁の小数の文字列にします。ClickHouseの
// DateTime64は、この形を、serverのtime zoneによらず、列の精度で読みます。
func clickHouseTime(t time.Time) string {
	t = t.UTC()
	nanos := strconv.Itoa(t.Nanosecond())
	return strconv.FormatInt(t.Unix(), 10) + "." + "000000000"[len(nanos):] + nanos
}
