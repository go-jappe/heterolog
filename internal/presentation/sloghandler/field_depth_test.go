package sloghandler_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/presentation/sloghandler"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// nestedValueは、leafをdepth重の配列で包みます。
func nestedValue(depth int, leaf string) any {
	var value any = leaf
	for range depth {
		value = []any{value}
	}
	return value
}

// requireNestedLeafは、要素が一つの配列をdepth重だけ外し、leafを確かめます。
func requireNestedLeaf(t *testing.T, value any, depth int, leaf string) {
	t.Helper()
	for level := range depth {
		array, ok := value.([]any)
		if !ok || len(array) != 1 {
			t.Fatalf("nesting lost at level %d: value type %T", level, value)
		}
		value = array[0]
	}
	if value != leaf {
		t.Fatalf("nested leaf = %#v, want %q", value, leaf)
	}
}

// slogのattributeは、Anyと同じ入れ子の深さの上限に従います。上限を超えると、logを
// 記録する前にErrFieldValueNotJSONを返します。
func TestHandlerSharesTheFieldNestingLimit(t *testing.T) {
	for _, depth := range []int{pkglogger.MaxFieldNestingDepth, pkglogger.MaxFieldNestingDepth + 1} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			capture := &slogCaptureLogger{}
			handler := sloghandler.New(capture, nil)
			record := slog.NewRecord(time.Now(), slog.LevelInfo, "nested attribute", 0)
			record.AddAttrs(slog.Any("deep", nestedValue(depth, "slog leaf")))
			err := handler.Handle(context.Background(), record)
			records := capture.snapshot()
			if depth > pkglogger.MaxFieldNestingDepth {
				if !errors.Is(err, pkglogger.ErrFieldValueNotJSON) || len(records) != 0 {
					t.Fatalf("Handle() = %v with %d records, want ErrFieldValueNotJSON before logging", err, len(records))
				}
				return
			}
			if err != nil || len(records) != 1 {
				t.Fatalf("Handle() = %v with %d records, want one logged record", err, len(records))
			}
			requireNestedLeaf(t, records[0].fields["deep"], depth, "slog leaf")
		})
	}
}
