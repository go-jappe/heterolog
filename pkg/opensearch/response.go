package opensearch

import "fmt"

// Validate checks that a successful response identifies the document processed
// by request. An ingest pipeline can successfully drop a document with a noop
// result. The returned index may be a concrete/backing index resolved from
// an alias or data stream, so it must be present but need not equal request.Index.
// Status is not checked because the single-document API does not require it
// in its JSON response body.
func (r *IndexResponse) Validate(request DocumentWriteRequest) error {
	if r == nil {
		return fmt.Errorf("IndexDocument returned nil response without error")
	}
	if request.ID == "" {
		return fmt.Errorf("document id is required to validate index response")
	}
	if r.ID == "" {
		return fmt.Errorf("index response is missing _id")
	}
	if r.ID != request.ID {
		return fmt.Errorf("index response _id mismatch: got=%q want=%q", r.ID, request.ID)
	}
	if r.Index == "" {
		return fmt.Errorf("index response is missing _index")
	}
	switch request.Op {
	case WriteOpIndex:
		if r.Result == "created" || r.Result == "updated" || r.Result == "noop" {
			return nil
		}
	case WriteOpCreate:
		if r.Result == "created" || r.Result == "noop" {
			return nil
		}
	default:
		return fmt.Errorf("unknown op: %s", request.Op)
	}
	return fmt.Errorf("unexpected result for %s: %q", request.Op, r.Result)
}
