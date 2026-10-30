package tracing

import (
	"fmt"
	"sync"
	"time"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// recordingSpanは、記録するspanです。どのメソッドも並行に呼べ、Endの後の呼び出しは
// 何もしません。
type recordingSpan struct {
	sink SpanSink
	now  func() time.Time

	mu    sync.Mutex
	data  SpanData
	ended bool
}

func (s *recordingSpan) TraceID() pkgobserver.TraceID {
	return pkgobserver.TraceID(s.data.TraceID.String())
}

// Endは、spanを終え、SpanSinkへ渡します。2回目からは何もしません。
func (s *recordingSpan) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.data.EndTime = s.now()
	data := s.data
	s.mu.Unlock()
	s.sink.Enqueue(data)
}

func (s *recordingSpan) AddEvent(name string, attrs ...pkgobserver.Attr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addEventLocked(name, attrs)
}

func (s *recordingSpan) addEventLocked(name string, attrs []pkgobserver.Attr) {
	if s.ended {
		return
	}
	if len(s.data.Events) >= maxEventsPerSpan {
		s.data.DroppedEvents++
		return
	}
	eventAttrs, dropped := limitAttrs(attrs)
	s.data.Events = append(s.data.Events, Event{Name: name, Time: s.now(), Attrs: eventAttrs, DroppedAttrs: dropped})
}

func (s *recordingSpan) SetAttributes(attrs ...pkgobserver.Attr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.setAttributes(attrs)
}

// setAttributesは、spanの属性を足します。呼び出し元はmuを持つか、spanをまだ誰にも
// 渡していない必要があります。
func (s *recordingSpan) setAttributes(attrs []pkgobserver.Attr) {
	for _, attr := range attrs {
		s.data.Attrs, s.data.DroppedAttrs = putAttr(s.data.Attrs, attr, maxAttrsPerSpan, s.data.DroppedAttrs)
	}
}

// RecordErrorは、OpenTelemetryの規約に従い、errをexceptionのeventとして記録し、
// statusをStatusErrorにします。statusがStatusOKの場合は、statusを変えません。
func (s *recordingSpan) RecordError(err error, attrs ...pkgobserver.Attr) {
	if err == nil {
		return
	}
	eventAttrs := make([]pkgobserver.Attr, 0, len(attrs)+2)
	eventAttrs = append(eventAttrs,
		pkgobserver.Attr{Key: "exception.type", Value: fmt.Sprintf("%T", err)},
		pkgobserver.Attr{Key: "exception.message", Value: err.Error()},
	)
	eventAttrs = append(eventAttrs, attrs...)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.addEventLocked("exception", eventAttrs)
	s.setStatusLocked(pkgobserver.StatusError, err.Error())
}

func (s *recordingSpan) SetStatus(code pkgobserver.StatusCode, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStatusLocked(code, description)
}

// setStatusLockedは、pkg/observerのSpan.SetStatusの規則でstatusを変えます。
// StatusOKは確定した結果として、後から変えません。
func (s *recordingSpan) setStatusLocked(code pkgobserver.StatusCode, description string) {
	if s.ended || s.data.Status == pkgobserver.StatusOK {
		return
	}
	switch code {
	case pkgobserver.StatusOK:
		s.data.Status, s.data.StatusMessage = code, ""
	case pkgobserver.StatusError:
		s.data.Status, s.data.StatusMessage = code, description
	}
}

// nonRecordingSpanは、samplingで記録しないと決めたspanです。trace IDだけを返し、
// ほかのメソッドは何もしません。
type nonRecordingSpan struct {
	traceID pkgobserver.TraceID
}

func (s nonRecordingSpan) TraceID() pkgobserver.TraceID           { return s.traceID }
func (nonRecordingSpan) End()                                     {}
func (nonRecordingSpan) AddEvent(string, ...pkgobserver.Attr)     {}
func (nonRecordingSpan) SetAttributes(...pkgobserver.Attr)        {}
func (nonRecordingSpan) RecordError(error, ...pkgobserver.Attr)   {}
func (nonRecordingSpan) SetStatus(pkgobserver.StatusCode, string) {}
