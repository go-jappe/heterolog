package fsstore

import (
	"errors"
	"reflect"
	"testing"
)

func TestPurgeExpiredFilesContinuesAfterFailuresAndStopsAtFirstRetainedFile(t *testing.T) {
	type candidate struct {
		id      int
		removed bool
	}
	candidates := []candidate{{id: 0}, {id: 1}, {id: 2}, {id: 3}, {id: 4}}
	inspectionErr := errors.New("cannot determine expiration")
	removalErr := errors.New("cannot remove oldest file")
	var inspected, attempted []int
	errs := PurgeExpiredFiles(candidates, func(file *candidate) (bool, error) {
		inspected = append(inspected, file.id)
		if file.id == 0 {
			return false, inspectionErr
		}
		return file.id < 3, nil
	}, func(file *candidate) error {
		attempted = append(attempted, file.id)
		if file.id == 1 {
			return removalErr
		}
		file.removed = true
		return nil
	})
	if !reflect.DeepEqual(inspected, []int{0, 1, 2, 3}) || !reflect.DeepEqual(attempted, []int{1, 2}) {
		t.Fatalf("inspected = %v, attempted = %v", inspected, attempted)
	}
	if len(errs) != 2 || !errors.Is(errs[0], inspectionErr) || !errors.Is(errs[1], removalErr) {
		t.Fatalf("errors = %v, want inspection then removal failure", errs)
	}
	for index, file := range candidates {
		if file.removed != (index == 2) {
			t.Fatalf("candidate %d removed = %t", index, file.removed)
		}
	}
}

func TestPurgeFilesToSizeLimitUsesOnlySuccessfulRemovalsAndIncludesProtectedBytes(t *testing.T) {
	tests := []struct {
		name          string
		sizes         []uint64
		total, limit  uint64
		firstFails    bool
		wantRemaining uint64
		wantAttempts  int
	}{
		{"failed removal frees no bytes", []uint64{8, 4, 4}, 16, 12, true, 12, 2},
		{"protected files still occupy capacity", []uint64{4, 4}, 12, 3, false, 4, 2},
		{"already at limit", []uint64{4}, 4, 4, false, 4, 0},
		{"subtraction stops at zero", []uint64{11}, 6, 0, false, 0, 1},
		{"completed candidate consumes no capacity", []uint64{0, 4, 4}, 8, 4, false, 4, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			type candidate struct {
				size    uint64
				removed bool
			}
			candidates := make([]candidate, len(test.sizes))
			for index, size := range test.sizes {
				candidates[index].size = size
			}
			removalErr := errors.New("removal failed")
			attempts := 0
			remaining, errs := PurgeFilesToSizeLimit(candidates, test.total, test.limit, func(file *candidate) (uint64, error) {
				attempts++
				if test.firstFails && attempts == 1 {
					// エラーとサイズが同時に返っても、容量は減らしてはいけません。
					return file.size, removalErr
				}
				file.removed = true
				return file.size, nil
			})
			if remaining != test.wantRemaining || attempts != test.wantAttempts {
				t.Fatalf("remaining = %d, attempts = %d; want %d, %d", remaining, attempts, test.wantRemaining, test.wantAttempts)
			}
			if test.firstFails {
				if len(errs) != 1 || !errors.Is(errs[0], removalErr) {
					t.Fatalf("errors = %v, want removal failure", errs)
				}
			} else if len(errs) != 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}
			for index, file := range candidates {
				wantRemoved := index < attempts && !(test.firstFails && index == 0)
				if file.removed != wantRemoved {
					t.Fatalf("candidate %d removed = %t, want %t", index, file.removed, wantRemoved)
				}
			}
		})
	}
}
