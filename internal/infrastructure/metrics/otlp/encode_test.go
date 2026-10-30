package otlp

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	testStart = time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	testNow   = testStart.Add(time.Minute)
)

func testMetrics() ([]metrics.Metric, *telemetrydata.Resource) {
	defaultResource := &telemetrydata.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "api"}}}
	sdkResource := &telemetrydata.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "sdk"}}, SchemaURL: "https://example.com/schema"}
	scope := telemetrydata.Scope{Name: metrics.InstrumentationScope}
	return []metrics.Metric{
		{
			Name: "requests", Kind: metrics.KindSum, Monotonic: true, Temporality: metrics.TemporalityCumulative, Scope: scope,
			Points: []metrics.Point{
				{Attrs: []pkgobserver.Attr{{Key: "code", Value: 200}}, StartTime: testStart, Time: testNow, IsInt: true, Int: 1 << 60},
				{StartTime: testStart, Time: testNow, IsInt: true, Int: 0},
			},
		},
		{
			Name: "memory", Description: "used memory", Unit: "1", Kind: metrics.KindGauge, Scope: scope,
			Points: []metrics.Point{{Time: testNow, Float: math.Inf(1)}, {Time: testNow, Float: 0}},
		},
		{
			Name: "duration", Unit: "s", Kind: metrics.KindHistogram, Temporality: metrics.TemporalityDelta,
			Scope: telemetrydata.Scope{Name: "sdk-lib", Version: "1.0"}, Resource: sdkResource,
			Points: []metrics.Point{{
				StartTime: testStart, Time: testNow, Count: 4, Sum: 2.65, HasMinMax: true, Min: 0.05, Max: 2,
				Bounds: []float64{0.1, 1}, BucketCounts: []uint64{2, 1, 1},
			}},
		},
	}, defaultResource
}

func TestMarshalProtoDecodesAsExportMetricsServiceRequest(t *testing.T) {
	items, defaultResource := testMetrics()
	var req collectormetrics.ExportMetricsServiceRequest
	if err := proto.Unmarshal(MarshalProto(items, defaultResource), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(req.ResourceMetrics) != 2 {
		t.Fatalf("resource metrics = %d, want the default resource and the SDK resource", len(req.ResourceMetrics))
	}
	own := req.ResourceMetrics[0].ScopeMetrics[0]
	if own.Scope.Name != metrics.InstrumentationScope || len(own.Metrics) != 2 {
		t.Fatalf("own scope metrics = %v", own)
	}
	sum := own.Metrics[0].GetSum()
	if sum == nil || !sum.IsMonotonic || sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE ||
		sum.DataPoints[0].GetAsInt() != 1<<60 || sum.DataPoints[0].StartTimeUnixNano != uint64(testStart.UnixNano()) ||
		sum.DataPoints[0].Attributes[0].Value.GetIntValue() != 200 {
		t.Errorf("sum = %v", sum)
	}
	// 0の値も、どちらの型かを表すため書きます。
	if _, ok := sum.DataPoints[1].Value.(*metricspb.NumberDataPoint_AsInt); !ok {
		t.Errorf("zero point value = %T, want asInt", sum.DataPoints[1].Value)
	}
	gauge := own.Metrics[1]
	if gauge.Description != "used memory" || gauge.Unit != "1" || !math.IsInf(gauge.GetGauge().DataPoints[0].GetAsDouble(), 1) {
		t.Errorf("gauge = %v", gauge)
	}
	if _, ok := gauge.GetGauge().DataPoints[1].Value.(*metricspb.NumberDataPoint_AsDouble); !ok {
		t.Errorf("zero gauge value = %T, want asDouble", gauge.GetGauge().DataPoints[1].Value)
	}
	sdk := req.ResourceMetrics[1]
	histogram := sdk.ScopeMetrics[0].Metrics[0].GetHistogram()
	point := histogram.DataPoints[0]
	if sdk.SchemaUrl != "https://example.com/schema" || histogram.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA ||
		point.Count != 4 || point.GetSum() != 2.65 || point.GetMin() != 0.05 || point.GetMax() != 2 ||
		len(point.BucketCounts) != 3 || point.BucketCounts[0] != 2 || len(point.ExplicitBounds) != 2 || point.ExplicitBounds[1] != 1 {
		t.Errorf("histogram = %v", histogram)
	}
}

func TestMarshalJSONFollowsTheOTLPJSONMappingAndRoundTrips(t *testing.T) {
	items, defaultResource := testMetrics()
	data, err := MarshalJSON(items, defaultResource)
	if err != nil {
		t.Fatal(err)
	}
	// OpenTelemetryのprotobufのJSONとして読めることで、OTLP/JSONの規則に従っていることを
	// 確かめます。
	var req collectormetrics.ExportMetricsServiceRequest
	if err := protojson.Unmarshal(data, &req); err != nil {
		t.Fatalf("protojson: %v\n%s", err, data)
	}
	if got := req.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetSum().DataPoints[0].GetAsInt(); got != 1<<60 {
		t.Errorf("asInt = %d, want the exact value", got)
	}
	decoded, err := UnmarshalJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalJSON = %v", err)
	}
	if got, want := MarshalProto(decoded, nil), MarshalProto(items, defaultResource); !bytes.Equal(got, want) {
		t.Error("the decoded metrics do not encode to the same protobuf")
	}
	if decoded[0].Resource != decoded[1].Resource || decoded[0].Resource == decoded[2].Resource {
		t.Error("the metrics do not share the Resource of their ResourceMetrics")
	}
}

func TestUnmarshalJSONRejectsWhatItCannotRestore(t *testing.T) {
	metric := func(fields string) string {
		return `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{},"metrics":[{"name":"m",` + fields + `}]}]}]}`
	}
	for name, data := range map[string]string{
		"not JSON":              `{`,
		"no data":               metric(`"unit":"1"`),
		"two kinds":             metric(`"gauge":{"dataPoints":[]},"sum":{"dataPoints":[],"aggregationTemporality":2}`),
		"exponential histogram": metric(`"exponentialHistogram":{"dataPoints":[]}`),
		"bad temporality":       metric(`"sum":{"dataPoints":[],"aggregationTemporality":0}`),
		"no value":              metric(`"gauge":{"dataPoints":[{"timeUnixNano":"1"}]}`),
		"bad asInt":             metric(`"gauge":{"dataPoints":[{"timeUnixNano":"1","asInt":"1.5"}]}`),
		"bucket mismatch":       metric(`"histogram":{"aggregationTemporality":2,"dataPoints":[{"timeUnixNano":"1","count":"1","sum":1,"bucketCounts":["1"],"explicitBounds":[1]}]}`),
	} {
		if _, err := UnmarshalJSON([]byte(data)); !errors.Is(err, ErrInvalidJSON) {
			t.Errorf("%s: UnmarshalJSON = %v, want ErrInvalidJSON", name, err)
		}
	}
}
