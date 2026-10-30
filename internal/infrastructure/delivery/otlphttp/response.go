package otlphttp

import (
	"errors"

	"google.golang.org/protobuf/encoding/protowire"
)

// errMalformedResponseは、OTLPのexportの応答として読めない応答です。
var errMalformedResponse = errors.New("malformed OTLP export response")

// partialSuccessは、受信側が一部を受け付けなかったことを表す、exportの応答の
// partial_successです。
type partialSuccess struct {
	// Rejectedは、受け付けなかった数です。traceではspan、metricsではdata pointの数です。
	Rejected     int64
	ErrorMessage string
}

// unmarshalPartialSuccessは、protobufのexportの応答（ExportTraceServiceResponseや
// ExportMetricsServiceResponse）から、partial_success（field 1。中身はrejected_spansまたは
// rejected_data_points = 1とerror_message = 2）を読みます。どの種類の応答も同じ形です。
// 知らないfieldは読み飛ばします。
func unmarshalPartialSuccess(b []byte) (partialSuccess, error) {
	var result partialSuccess
	err := consumeFields(b, func(num protowire.Number, typ protowire.Type, field []byte) error {
		if num != 1 || typ != protowire.BytesType {
			return nil
		}
		message, n := protowire.ConsumeBytes(field)
		if n < 0 {
			return errMalformedResponse
		}
		return consumeFields(message, func(num protowire.Number, typ protowire.Type, field []byte) error {
			switch {
			case num == 1 && typ == protowire.VarintType:
				v, n := protowire.ConsumeVarint(field)
				if n < 0 {
					return errMalformedResponse
				}
				result.Rejected = int64(v)
			case num == 2 && typ == protowire.BytesType:
				v, n := protowire.ConsumeString(field)
				if n < 0 {
					return errMalformedResponse
				}
				result.ErrorMessage = v
			}
			return nil
		})
	})
	return result, err
}

// consumeFieldsは、bのfieldを順にvisitへ渡します。fieldは、tagの後の値の部分です。
func consumeFields(b []byte, visit func(num protowire.Number, typ protowire.Type, field []byte) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return errMalformedResponse
		}
		b = b[n:]
		size := protowire.ConsumeFieldValue(num, typ, b)
		if size < 0 {
			return errMalformedResponse
		}
		if err := visit(num, typ, b[:size]); err != nil {
			return err
		}
		b = b[size:]
	}
	return nil
}
