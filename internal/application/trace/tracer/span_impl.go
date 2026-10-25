package tracer

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
)

// spanImpl はSpanインターフェースの実装
type spanImpl struct {
	mu         sync.RWMutex
	tracer     *tracerImpl
	name       string
	kind       span.SpanKind
	startTime  time.Time
	endTime    time.Time
	attributes []span.Attribute
	events     []span.SpanEvent
	status     span.SpanStatus
	links      []span.SpanLink

	spanContext SpanContext
	sampled     bool
	recording   bool

	droppedAttributesCount int
	droppedEventsCount     int
	droppedLinksCount      int
}

// End はSpanを終了する
func (s *spanImpl) End() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.recording {
		return
	}

	s.endTime = time.Now()
	s.recording = false

	// Tracerにエクスポートを依頼
	s.tracer.export(s)
}

// SetAttributes は属性を設定する
func (s *spanImpl) SetAttributes(attrs ...span.Attribute) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.recording {
		return
	}

	// 属性の上限チェック（例: 128個まで）
	const maxAttributes = 128
	currentCount := len(s.attributes)

	for _, attr := range attrs {
		if currentCount >= maxAttributes {
			s.droppedAttributesCount++
			continue
		}
		s.attributes = append(s.attributes, attr)
		currentCount++
	}
}

// AddEvent はイベントを追加する
func (s *spanImpl) AddEvent(name string, attrs ...span.Attribute) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.recording {
		return
	}

	// イベントの上限チェック（例: 128個まで）
	const maxEvents = 128
	if len(s.events) >= maxEvents {
		s.droppedEventsCount++
		return
	}

	event := span.SpanEvent{
		Time:  time.Now(),
		Name:  name,
		Attrs: attrs,
	}
	s.events = append(s.events, event)
}

// SetStatus はステータスを設定する
func (s *spanImpl) SetStatus(code span.StatusCode, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.recording {
		return
	}

	s.status = span.SpanStatus{
		Code:    code,
		Message: message,
	}
}

// RecordError はエラーを記録する
func (s *spanImpl) RecordError(err error) {
	if err == nil {
		return
	}

	// エラーをイベントとして記録
	s.AddEvent("error",
		span.Attribute{Key: "error.type", Value: fmt.Sprintf("%T", err)},
		span.Attribute{Key: "error.message", Value: err.Error()},
	)

	// ステータスをエラーに設定
	s.SetStatus(span.StatusError, err.Error())
}

// IsRecording はSpanが記録中かどうかを返す
func (s *spanImpl) IsRecording() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recording
}

// SpanContext はSpanのコンテキスト情報を返す
func (s *spanImpl) SpanContext() SpanContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.spanContext
}
