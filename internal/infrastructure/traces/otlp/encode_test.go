package otlp

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var (
	testStart = time.Date(2026, 10, 30, 1, 2, 3, 4, time.UTC)
	testEnd   = testStart.Add(1500 * time.Millisecond)
)

func testSpans() ([]tracing.SpanData, *tracing.Resource) {
	defaultResource := otlpcommon.NewResource(pkglogger.Resource{ServiceName: "api", Environment: "prod"})
	sdkResource := &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "sdk"}}, SchemaURL: "https://example.com/schema"}
	root := tracing.SpanData{
		TraceID:   tracing.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:    tracing.SpanID{1, 1, 1, 1, 1, 1, 1, 1},
		Name:      "GET /items",
		Kind:      pkgobserver.SpanKindServer,
		StartTime: testStart,
		EndTime:   testEnd,
		Attrs: []pkgobserver.Attr{
			{Key: "s", Value: ""},
			{Key: "b", Value: false},
			{Key: "i", Value: 0},
			{Key: "u", Value: uint64(math.MaxUint64)},
			{Key: "f", Value: 1.5},
			{Key: "list", Value: []string{"a", "b"}},
			{Key: "d", Value: 2 * time.Second},
		},
		DroppedAttrs: 3,
		Events: []tracing.Event{{
			Name: "exception", Time: testStart.Add(time.Millisecond),
			Attrs: []pkgobserver.Attr{{Key: "exception.message", Value: "boom"}},
		}},
		DroppedEvents: 1,
		Links: []tracing.Link{{
			TraceID: tracing.TraceID{9}, SpanID: tracing.SpanID{8},
			Attrs: []pkgobserver.Attr{{Key: "message", Value: int64(7)}},
		}},
		Status:        pkgobserver.StatusError,
		StatusMessage: "boom",
		Scope:         tracing.Scope{Name: tracing.InstrumentationScope},
	}
	child := root
	child.SpanID = tracing.SpanID{2, 2, 2, 2, 2, 2, 2, 2}
	child.ParentSpanID = root.SpanID
	child.Name = "query"
	child.Kind = pkgobserver.SpanKindInternal
	child.Attrs, child.Events, child.Links = nil, nil, nil
	child.DroppedAttrs, child.DroppedEvents = 0, 0
	child.Status, child.StatusMessage = pkgobserver.StatusUnset, ""
	sdk := child
	sdk.SpanID = tracing.SpanID{3, 3, 3, 3, 3, 3, 3, 3}
	sdk.Name = "sdk span"
	sdk.Scope = tracing.Scope{Name: "sdk-lib", Version: "1.0"}
	sdk.Resource = sdkResource
	return []tracing.SpanData{root, sdk, child}, defaultResource
}

func TestMarshalProtoDecodesAsExportTraceServiceRequest(t *testing.T) {
	spans, defaultResource := testSpans()
	var req collectortrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(MarshalProto(spans, defaultResource), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(req.ResourceSpans) != 2 {
		t.Fatalf("resource spans = %d, want the default resource and the SDK resource", len(req.ResourceSpans))
	}
	defaultGroup, sdkGroup := req.ResourceSpans[0], req.ResourceSpans[1]
	if got := attrMap(defaultGroup.Resource.Attributes); got["service.name"] != "api" || got["deployment.environment"] != "prod" || len(got) != 2 {
		t.Errorf("default resource = %v, want service.name and deployment.environment", got)
	}
	if got := attrMap(sdkGroup.Resource.Attributes); got["service.name"] != "sdk" || sdkGroup.SchemaUrl != "https://example.com/schema" {
		t.Errorf("SDK resource = %v schema %q, want the SDK resource", got, sdkGroup.SchemaUrl)
	}
	if len(defaultGroup.ScopeSpans) != 1 || len(defaultGroup.ScopeSpans[0].Spans) != 2 ||
		defaultGroup.ScopeSpans[0].Scope.Name != tracing.InstrumentationScope {
		t.Fatalf("default scope spans = %+v, want the heterolog scope with the root and the child", defaultGroup.ScopeSpans)
	}
	if scope := sdkGroup.ScopeSpans[0].Scope; scope.Name != "sdk-lib" || scope.Version != "1.0" {
		t.Errorf("SDK scope = %+v, want sdk-lib 1.0", scope)
	}

	root, child := defaultGroup.ScopeSpans[0].Spans[0], defaultGroup.ScopeSpans[0].Spans[1]
	if root.Name != "GET /items" || root.Kind != tracepb.Span_SPAN_KIND_SERVER || len(root.ParentSpanId) != 0 ||
		root.StartTimeUnixNano != uint64(testStart.UnixNano()) || root.EndTimeUnixNano != uint64(testEnd.UnixNano()) {
		t.Errorf("root = %v", root)
	}
	if string(root.TraceId) != string(spans[0].TraceID[:]) || string(root.SpanId) != string(spans[0].SpanID[:]) {
		t.Errorf("root IDs = %x %x", root.TraceId, root.SpanId)
	}
	attrs := map[string]*commonpb.AnyValue{}
	for _, kv := range root.Attributes {
		attrs[kv.Key] = kv.Value
	}
	// 既定値でも、AnyValueのどの値かは残ります。
	if _, ok := attrs["s"].Value.(*commonpb.AnyValue_StringValue); !ok {
		t.Errorf("empty string attribute = %v, want a string value", attrs["s"])
	}
	if v, ok := attrs["b"].Value.(*commonpb.AnyValue_BoolValue); !ok || v.BoolValue {
		t.Errorf("false attribute = %v, want a bool value", attrs["b"])
	}
	if v, ok := attrs["i"].Value.(*commonpb.AnyValue_IntValue); !ok || v.IntValue != 0 {
		t.Errorf("zero attribute = %v, want an int value", attrs["i"])
	}
	if attrs["u"].GetStringValue() != "18446744073709551615" || attrs["f"].GetDoubleValue() != 1.5 || attrs["d"].GetStringValue() != "2s" {
		t.Errorf("uint64/float/duration attributes = %v %v %v", attrs["u"], attrs["f"], attrs["d"])
	}
	if values := attrs["list"].GetArrayValue().GetValues(); len(values) != 2 || values[1].GetStringValue() != "b" {
		t.Errorf("list attribute = %v, want an array of strings", attrs["list"])
	}
	if root.DroppedAttributesCount != 3 || root.DroppedEventsCount != 1 {
		t.Errorf("dropped counts = %d attrs, %d events", root.DroppedAttributesCount, root.DroppedEventsCount)
	}
	if len(root.Events) != 1 || root.Events[0].Name != "exception" || root.Events[0].TimeUnixNano != uint64(testStart.Add(time.Millisecond).UnixNano()) ||
		attrMap(root.Events[0].Attributes)["exception.message"] != "boom" {
		t.Errorf("events = %v", root.Events)
	}
	if len(root.Links) != 1 || root.Links[0].TraceId[0] != 9 || root.Links[0].SpanId[0] != 8 ||
		root.Links[0].Attributes[0].Value.GetIntValue() != 7 {
		t.Errorf("links = %v", root.Links)
	}
	if root.Status.GetCode() != tracepb.Status_STATUS_CODE_ERROR || root.Status.GetMessage() != "boom" {
		t.Errorf("status = %v", root.Status)
	}
	if string(child.ParentSpanId) != string(spans[0].SpanID[:]) || child.Kind != tracepb.Span_SPAN_KIND_INTERNAL || child.Status != nil {
		t.Errorf("child = %v, want an internal child of the root without status", child)
	}
}

func attrMap(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[kv.Key] = kv.Value.GetStringValue()
	}
	return out
}

func TestMarshalJSONFollowsTheOTLPJSONMapping(t *testing.T) {
	spans, defaultResource := testSpans()
	spans[0].Attrs = append(spans[0].Attrs, pkgobserver.Attr{Key: "nan", Value: math.NaN()})
	b, err := MarshalJSON(spans, defaultResource)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "\n") {
		t.Error("the JSON contains a line break")
	}
	var data struct {
		ResourceSpans []struct {
			Resource struct {
				Attributes []map[string]any `json:"attributes"`
			} `json:"resource"`
			ScopeSpans []struct {
				Scope map[string]string `json:"scope"`
				Spans []map[string]any  `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	if len(data.ResourceSpans) != 2 || len(data.ResourceSpans[0].ScopeSpans[0].Spans) != 2 {
		t.Fatalf("JSON = %s, want two resources and two spans in the first", b)
	}
	root := data.ResourceSpans[0].ScopeSpans[0].Spans[0]
	child := data.ResourceSpans[0].ScopeSpans[0].Spans[1]
	for key, want := range map[string]any{
		"traceId":           "0102030405060708090a0b0c0d0e0f10",
		"spanId":            "0101010101010101",
		"name":              "GET /items",
		"kind":              float64(2),
		"startTimeUnixNano": "1793322123000000004",
		"endTimeUnixNano":   "1793322124500000004",
	} {
		if root[key] != want {
			t.Errorf("root %s = %#v, want %#v", key, root[key], want)
		}
	}
	if _, ok := root["parentSpanId"]; ok {
		t.Error("the root span has a parentSpanId")
	}
	if child["parentSpanId"] != "0101010101010101" {
		t.Errorf("child parentSpanId = %v", child["parentSpanId"])
	}
	values := map[string]any{}
	for _, attr := range root["attributes"].([]any) {
		kv := attr.(map[string]any)
		values[kv["key"].(string)] = kv["value"]
	}
	for key, want := range map[string]string{
		"s":    `{"stringValue":""}`,
		"b":    `{"boolValue":false}`,
		"i":    `{"intValue":"0"}`,
		"f":    `{"doubleValue":1.5}`,
		"nan":  `{"doubleValue":"NaN"}`,
		"list": `{"arrayValue":{"values":[{"stringValue":"a"},{"stringValue":"b"}]}}`,
	} {
		got, _ := json.Marshal(values[key])
		if string(got) != want {
			t.Errorf("attribute %s = %s, want %s", key, got, want)
		}
	}
	if status, _ := json.Marshal(root["status"]); string(status) != `{"code":2,"message":"boom"}` {
		t.Errorf("status = %s", status)
	}
}
