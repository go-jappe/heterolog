package opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// BulkRequestSizeは、IndexDocumentsが送信するgzip圧縮前のNDJSON bodyのbyte数を返します。
// action metadata、document本文、および各行末の改行をすべて含みます。
//
// この関数とIndexDocumentsは同じbulkRequestBodyを使用するため、呼び出し側は送信時と
// 同じ形式を推測や近似なしで計測できます。
func BulkRequestSize(docs []DocumentWriteRequest) (int, error) {
	body, err := bulkRequestBody(docs)
	if err != nil {
		return 0, err
	}
	return len(body), nil
}

// bulkRequestBodyは、IndexDocumentsが送信するcanonicalなBulk NDJSON bodyを生成します。
// 一件につきaction metadata行とdocument本文行を書き、最後の行にも改行を付けます。
func bulkRequestBody(docs []DocumentWriteRequest) ([]byte, error) {
	var buf bytes.Buffer

	for _, doc := range docs {
		if err := ValidateIndex(doc.Index); err != nil {
			return nil, err
		}
		if doc.ID == "" {
			return nil, fmt.Errorf("document id is required")
		}

		actionKey, err := doc.BulkActionKey()
		if err != nil {
			return nil, err
		}
		metadata := map[string]any{
			actionKey: map[string]any{
				"_index": doc.Index,
				"_id":    doc.ID,
			},
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal bulk meta: %w", err)
		}
		buf.Write(metadataJSON)
		buf.WriteByte('\n')

		documentJSON, err := json.Marshal(doc.Doc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal bulk doc: %w", err)
		}
		buf.Write(documentJSON)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}
