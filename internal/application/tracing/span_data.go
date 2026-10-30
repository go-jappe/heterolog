// Package tracingは、heterologのTracerを実装します。spanのID、親子関係、samplingを
// 決め、終わったspanをSpanSinkへ渡します。spanをまとめて送る処理、送り先、再試行、
// 既定値はinfrastructure層が受け持ちます。OpenTelemetry SDKから届いたspanも、
// presentation層がSpanDataに変えて同じSpanSinkへ渡します。
package tracing

import (
	"encoding/hex"
	"time"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// TraceIDは、traceのIDの16 byteです。すべて0は、IDがないことを表します。
type TraceID [16]byte

// SpanIDは、spanのIDの8 byteです。すべて0は、IDがないことを表します。
type SpanID [8]byte

// IsZeroは、IDがないかを返します。
func (id TraceID) IsZero() bool { return id == TraceID{} }

// IsZeroは、IDがないかを返します。
func (id SpanID) IsZero() bool { return id == SpanID{} }

// Stringは、IDを小文字の16進数で返します。
func (id TraceID) String() string { return hex.EncodeToString(id[:]) }

// Stringは、IDを小文字の16進数で返します。
func (id SpanID) String() string { return hex.EncodeToString(id[:]) }

// ParseTraceIDとParseSpanIDは、pkg/observerの16進数のIDを読みます。不正な値は、
// IDがないもの（すべて0）にします。
func ParseTraceID(s pkgobserver.TraceID) TraceID {
	var id TraceID
	if len(s) != 2*len(id) {
		return TraceID{}
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return TraceID{}
	}
	return id
}

func ParseSpanID(s pkgobserver.SpanID) SpanID {
	var id SpanID
	if len(s) != 2*len(id) {
		return SpanID{}
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return SpanID{}
	}
	return id
}

// Scopeは、spanを作った計装（OpenTelemetryのinstrumentation scope）です。
type Scope = telemetrydata.Scope

// Resourceは、spanを送ったprocessを表す属性です。SpanDataのResourceがnilのspanは、
// 配送の仕組みに設定したResourceで送ります。
type Resource = telemetrydata.Resource

// Eventは、spanの途中で起きたことです。
type Event struct {
	Name         string
	Time         time.Time
	Attrs        []pkgobserver.Attr
	DroppedAttrs int
}

// Linkは、親以外のspanとの関係です。
type Link struct {
	TraceID      TraceID
	SpanID       SpanID
	Attrs        []pkgobserver.Attr
	DroppedAttrs int
}

// SpanDataは、終わったspanの記録です。SpanSinkへ渡した後は、どこからも変更しません。
type SpanData struct {
	TraceID TraceID
	SpanID  SpanID
	// ParentSpanIDは、親のspanのIDです。親のないspanでは、すべて0です。
	ParentSpanID SpanID
	Name         string
	Kind         pkgobserver.SpanKind
	StartTime    time.Time
	EndTime      time.Time

	Attrs         []pkgobserver.Attr
	DroppedAttrs  int
	Events        []Event
	DroppedEvents int
	Links         []Link
	DroppedLinks  int

	Status        pkgobserver.StatusCode
	StatusMessage string

	Scope Scope
	// Resourceがnilのspanは、配送の仕組みに設定したResourceで送ります。同じprocessの
	// spanは同じpointerを使い、送るときにResourceごとにまとめます。
	Resource *Resource
}

// SpanSinkは、終わったspanを受け取って送り先へ届ける配送の仕組みです。infrastructure層が
// 実装します。Enqueueは、Span.Endを呼んだ処理を待たせないよう、送り終わるのを待たずに
// 返します。受け取れないspan（待ち行列が一杯の場合や、終了した後）は、送らずに捨てて
// metricsで数えます。
type SpanSink interface {
	Enqueue(span SpanData)
}
