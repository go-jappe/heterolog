// sdk使う予定のため自作は廃止予定
package span

import "time"

type SpanInput struct {
	Name      string   // 処理名（例: "GET /api/users"）
	Kind      SpanKind // SERVER / CLIENT など
	StartTime time.Time
	EndTime   time.Time

	Attributes []Attribute // 業務関連のメタ情報（http.status など）
	Events     []SpanEvent // DBクエリ等のイベント
	Status     SpanStatus  // 成否
	Links      []SpanLink  // 関連Span（バッチや非同期）

	Sampled bool // ★ サンプリングされているか明示

	DroppedAttributesCount int
	DroppedEventsCount     int
	DroppedLinksCount      int
}

type Attribute struct {
	Key   string
	Value interface{}
}

type SpanEvent struct {
	Time  time.Time
	Name  string
	Attrs []Attribute
}

type SpanLink struct {
	TraceID string
	SpanID  string
}

type SpanStatus struct {
	Code    StatusCode
	Message string
}

type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

type SpanKind int

const (
	KindUnspecified SpanKind = iota // 未定義のデフォルト値
	KindInternal                    // 内部処理
	KindServer                      // 外部リクエスト受信
	KindClient                      // 外部リクエスト送信
	KindProducer                    // 非同期メッセージ送信
	KindConsumer                    // 非同期メッセージ受信
)
