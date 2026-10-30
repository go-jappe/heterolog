package otlphttp

import (
	"testing"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestUnmarshalPartialSuccessReadsTheRejectedCount(t *testing.T) {
	b, err := proto.Marshal(&collectortrace.ExportTraceServiceResponse{
		PartialSuccess: &collectortrace.ExportTracePartialSuccess{RejectedSpans: 3, ErrorMessage: "too old"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 知らないfieldは読み飛ばします。
	b = protowire.AppendTag(b, 9, protowire.BytesType)
	b = protowire.AppendString(b, "future")
	got, err := unmarshalPartialSuccess(b)
	if err != nil || got != (partialSuccess{Rejected: 3, ErrorMessage: "too old"}) {
		t.Errorf("unmarshalPartialSuccess = %+v, %v", got, err)
	}
	if _, err := unmarshalPartialSuccess([]byte{0x0a, 0x05}); err == nil {
		t.Error("a truncated response was accepted")
	}
}
