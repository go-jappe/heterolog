package flushfailure

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/degradehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestBuildRejectsNilConfigBeforeReadingProvider(t *testing.T) {
	handler, err := Build(
		context.Background(), clock.NewUTCClock(), nil, nil,
		observer.NopTracer{}, observer.NopMeter{}, FactoryOptions{},
	)
	if handler != nil || !errors.Is(err, ErrNilFailureStoreConfig) {
		t.Fatalf("Build(nil) = (%v, %v), want nil handler and missing config error", handler, err)
	}
}

func TestBuildDeliveryRejectsNilConfigBeforeReadingProvider(t *testing.T) {
	handler, err := BuildDelivery(
		context.Background(), clock.NewUTCClock(), nil,
		observer.NopTracer{}, observer.NopMeter{}, FactoryOptions{},
	)
	if !errors.Is(err, ErrNilFailureStoreConfig) {
		t.Fatalf("BuildDelivery(nil) error = %v, want missing config error", err)
	}
	if handler == nil || len(handler.Handlers) != 0 {
		t.Fatalf("BuildDelivery(nil) handler = %v, want empty cleanup composite", handler)
	}
}

func TestBuildPropagatesUnsupportedSpoolSpillDestination(t *testing.T) {
	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: t.TempDir(),
				Target:    "test-app",
				Dir:       "test-backend",
				OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
					OverloadPolicy:   pkgdurable.OverloadPolicyDegrade,
					SpillDestination: pkgdurable.SpillDestinationS3,
				},
			},
		},
	})

	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if !errors.Is(err, evacuation.ErrUnsupportedSpillDestination) {
		t.Fatalf("Build error = %v, want ErrUnsupportedSpillDestination", err)
	}
	if handler == nil {
		t.Fatal("Build handler = nil, want empty composite for accumulated factory error")
	}
	if len(handler.Handlers) != 0 {
		t.Fatalf("Build handlers = %d, want 0", len(handler.Handlers))
	}
}

func TestBuildRejectsNonPositiveConcurrencyWhenFailureHandlingIsActive(t *testing.T) {
	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: -1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
	})

	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if handler != nil {
		t.Fatalf("Build handler = %#v, want nil", handler)
	}
	var concurrencyErr *FailureHandlerConcurrencyError
	if !errors.Is(err, pkgdurable.ErrInvalidFailureHandlerMaxConcurrent) ||
		!errors.As(err, &concurrencyErr) || concurrencyErr.MaxConcurrent != -1 {
		t.Fatalf("Build error = %v, want invalid max concurrency -1", err)
	}
}

func TestBuildReturnsConstructionErrorForEverySelectedDurableStore(t *testing.T) {
	for _, durableType := range []pkgdurable.DurableType{
		pkgdurable.DurableTypeSpool,
		pkgdurable.DurableTypeFileArchive,
	} {
		t.Run(string(durableType), func(t *testing.T) {
			cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
				MaxConcurrent: 1,
				DurableType:   []pkgdurable.DurableType{durableType},
			})

			handler, err := Build(
				context.Background(),
				clock.NewUTCClock(),
				cfg,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				FactoryOptions{},
			)
			if err == nil {
				t.Fatalf("Build(%q) error = nil, want selected store construction error", durableType)
			}
			if handler == nil {
				t.Fatalf("Build(%q) handler = nil, want cleanup composite", durableType)
			}
			if len(handler.Handlers) != 0 {
				t.Fatalf("Build(%q) handlers = %d, want no successfully constructed child", durableType, len(handler.Handlers))
			}
		})
	}
}

func TestBuildReturnsNilWhenNoDurableOrDegradeDestinationIsSelected(t *testing.T) {
	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: -1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
		SpillType:     []pkgdurable.SpillType{pkgdurable.SpillType("unsupported")},
	})

	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err != nil {
		t.Fatalf("Build error = %v, want nil without a failure destination", err)
	}
	if handler != nil {
		t.Fatalf("Build handler = %#v, want nil without a failure destination", handler)
	}
}

func TestBuildPreconstructionErrorsDoNotUseInvalidUnusedConcurrency(t *testing.T) {
	for _, tt := range []struct {
		name          string
		durableTypes  []pkgdurable.DurableType
		spillTypes    []pkgdurable.SpillType
		enableDegrade bool
		maxConcurrent int
		requireErr    func(*testing.T, error)
	}{
		{
			name: "invalid durable selection",
			durableTypes: []pkgdurable.DurableType{
				pkgdurable.DurableTypeNone,
				pkgdurable.DurableTypeSpool,
			},
			maxConcurrent: -1,
			requireErr: func(t *testing.T, err error) {
				requireDurableTypeErrorForTest(t, err, "failure store", pkgdurable.DurableTypeNone, pkgdurable.ErrDurableTypeNoneNotAlone)
			},
		},
		{
			name:         "invalid spill selection",
			durableTypes: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			spillTypes: []pkgdurable.SpillType{
				pkgdurable.SpillTypeNone,
				pkgdurable.SpillType("stdout"),
			},
			enableDegrade: true,
			maxConcurrent: 1,
			requireErr: func(t *testing.T, err error) {
				requireSpillTypeErrorForTest(t, err, pkgdurable.SpillTypeNone, pkgdurable.ErrSpillTypeNoneNotAlone)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
				MaxConcurrent: tt.maxConcurrent,
				EnableDegrade: tt.enableDegrade,
				DurableType:   tt.durableTypes,
				SpillType:     tt.spillTypes,
			})

			handler, err := Build(
				context.Background(),
				clock.NewUTCClock(),
				cfg,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				FactoryOptions{},
			)
			tt.requireErr(t, err)
			// 使わない同時実行数の不正は、構築前の選択の誤りとして報告しません。
			if errors.Is(err, pkgdurable.ErrInvalidFailureHandlerMaxConcurrent) {
				t.Fatalf("Build error = %v, want no unused concurrency error", err)
			}
			if handler == nil {
				t.Fatal("Build handler = nil, want empty cleanup composite")
			}
			if len(handler.Handlers) != 0 {
				t.Fatalf("factory error composite handlers = %d, want 0", len(handler.Handlers))
			}
			// 設定errorと一緒に返すcompositeも、呼び出し側が通常どおり終了できます。
			if err := drainWithinHangTimeoutForTest(handler); err != nil {
				t.Fatalf("factory error composite Drain() error = %v", err)
			}
		})
	}
}

func TestBuildAcceptsExplicitNoneAsNoFailureDestination(t *testing.T) {
	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
		SpillType:     []pkgdurable.SpillType{pkgdurable.SpillTypeNone},
	})

	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err != nil {
		t.Fatalf("Build error = %v, want nil for explicit none", err)
	}
	if handler != nil {
		t.Fatalf("Build handler = %#v, want nil for explicit none", handler)
	}
}

func TestBuildCreatesDirectDegradeHandlerWithoutBuildingDurableStores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
			MaxConcurrent: 3,
			EnableDegrade: true,
			DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			SpillType:     []pkgdurable.SpillType{pkgdurable.SpillType("stdout")},
		})

		handler, err := Build(
			context.Background(),
			clock.NewUTCClock(),
			cfg,
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
			FactoryOptions{},
		)
		if err != nil {
			t.Fatalf("Build error = %v, want nil", err)
		}
		if len(handler.Handlers) != 1 {
			t.Fatalf("Build handlers = %d, want one direct degrade handler", len(handler.Handlers))
		}
		if _, ok := handler.Handlers[0].(*degradehandler.Handler); !ok {
			t.Fatalf("Build handler type = %T, want *degradehandler.Handler", handler.Handlers[0])
		}
		// 設定した同時実行上限は、child処理の同時実行数として効きます。
		requireCompositeChildConcurrencyForTest(t, handler, cfg.MaxConcurrent())
	})
}

// requireCompositeChildConcurrencyForTestは、構築済みcompositeのchildを止める試験用handlerへ
// 置き換えてlimit+1件のfailureを並行に渡し、同時に実行されるchildがlimit件で止まることを
// 確認します。synctestのbubble内で構築したcompositeへ使います。
func requireCompositeChildConcurrencyForTest(t *testing.T, composite *CompositeFailureHandler, limit int) {
	t.Helper()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseChildren := func() { releaseOnce.Do(func() { close(release) }) }
	// 試験が途中で失敗しても、止めたchildを再開してから終了します。
	t.Cleanup(releaseChildren)
	var running atomic.Int32
	composite.Handlers = []FailureHandler{failureHandlerFunc(func(ctx context.Context, _ loggerfailure.SendResult) error {
		running.Add(1)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})}
	done := make(chan error, limit+1)
	for range limit + 1 {
		go func() { done <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{}) }()
	}
	synctest.Wait()
	if got := running.Load(); got != int32(limit) {
		t.Fatalf("concurrent child handlers = %d, want %d", got, limit)
	}
	releaseChildren()
	for range limit + 1 {
		if err := <-done; err != nil {
			t.Fatalf("HandleFailure() error = %v", err)
		}
	}
	if got := running.Load(); got != int32(limit+1) {
		t.Fatalf("child handlers after release = %d, want %d", got, limit+1)
	}
}

func TestBuildRejectsInvalidDirectDegradeSpillTypesBeforeConstructingHandlers(t *testing.T) {
	tests := []struct {
		name     string
		types    []pkgdurable.SpillType
		wantKind error
		wantType pkgdurable.SpillType
	}{
		{
			name:     "empty",
			wantKind: pkgdurable.ErrSpillTypesRequired,
		},
		{
			name:     "none",
			types:    []pkgdurable.SpillType{pkgdurable.SpillTypeNone},
			wantKind: pkgdurable.ErrSpillTypeNoneWithDegrade,
			wantType: pkgdurable.SpillTypeNone,
		},
		{
			name:     "unsupported",
			types:    []pkgdurable.SpillType{"s3", "stdout"},
			wantKind: pkgdurable.ErrUnsupportedSpillType,
			wantType: "s3",
		},
		{
			name:     "empty value",
			types:    []pkgdurable.SpillType{""},
			wantKind: pkgdurable.ErrUnsupportedSpillType,
			wantType: "",
		},
		{
			name:     "duplicate",
			types:    []pkgdurable.SpillType{"stdout", "stdout"},
			wantKind: pkgdurable.ErrDuplicateSpillType,
			wantType: "stdout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
				MaxConcurrent: 1,
				EnableDegrade: true,
				DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
				SpillType:     tt.types,
			})

			handler, err := Build(
				context.Background(),
				clock.NewUTCClock(),
				cfg,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				FactoryOptions{},
			)
			requireSpillTypeErrorForTest(t, err, tt.wantType, tt.wantKind)
			if handler == nil {
				t.Fatal("Build handler = nil, want empty composite")
			}
			if len(handler.Handlers) != 0 {
				t.Fatalf("Build handlers = %d, want 0 after fail-fast", len(handler.Handlers))
			}
		})
	}
}

func TestBuildIgnoresDirectDegradeSettingsWhenDurableStoreIsSelected(t *testing.T) {
	cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		EnableDegrade: true,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		SpillType:     []pkgdurable.SpillType{pkgdurable.SpillType("s3")},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: t.TempDir(),
				Target:    "test-app",
				Dir:       "test-backend",
				OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
					OverloadPolicy: pkgdurable.OverloadPolicyDrop,
				},
			},
		},
	})

	handler, err := Build(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err != nil {
		t.Fatalf("Build error = %v, want direct degrade settings ignored while a durable store is selected", err)
	}
	if len(handler.Handlers) != 1 {
		t.Fatalf("Build handlers = %d, want one durable store handler", len(handler.Handlers))
	}
	if _, ok := handler.Handlers[0].(*degradehandler.Handler); ok {
		t.Fatal("Build created a direct degrade handler while a durable store is selected")
	}
}

func TestBuildRejectsInvalidDurableTypesBeforeConstructingStores(t *testing.T) {
	tests := []struct {
		name     string
		types    []pkgdurable.DurableType
		wantKind error
		wantType pkgdurable.DurableType
	}{
		{
			name:     "empty",
			wantKind: pkgdurable.ErrDurableTypesRequired,
		},
		{
			name:     "unsupported",
			types:    []pkgdurable.DurableType{"unknown"},
			wantKind: pkgdurable.ErrUnsupportedDurableType,
			wantType: "unknown",
		},
		{
			name: "duplicate",
			types: []pkgdurable.DurableType{
				pkgdurable.DurableTypeSpool,
				pkgdurable.DurableTypeSpool,
			},
			wantKind: pkgdurable.ErrDuplicateDurableType,
			wantType: pkgdurable.DurableTypeSpool,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loggerdurable.NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
				MaxConcurrent: 1,
				DurableType:   tt.types,
			})

			handler, err := Build(
				context.Background(),
				clock.NewUTCClock(),
				cfg,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				FactoryOptions{},
			)
			requireDurableTypeErrorForTest(t, err, "failure store", tt.wantType, tt.wantKind)
			if handler == nil {
				t.Fatal("Build handler = nil, want empty composite")
			}
			if len(handler.Handlers) != 0 {
				t.Fatalf("Build handlers = %d, want 0 after fail-fast", len(handler.Handlers))
			}
		})
	}
}

func TestBuildDeliveryReturnsNilWhenNoDurableOrDegradeDestinationIsSelected(t *testing.T) {
	cfg := loggerdurable.NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
		SpillType:   []pkgdurable.SpillType{pkgdurable.SpillType("unsupported")},
	})

	handler, err := BuildDelivery(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err != nil {
		t.Fatalf("BuildDelivery error = %v, want nil without a failure destination", err)
	}
	if handler != nil {
		t.Fatalf("BuildDelivery handler = %#v, want nil without a failure destination", handler)
	}
}

func TestBuildDeliveryReturnsConstructionErrorForSelectedFileArchive(t *testing.T) {
	cfg := loggerdurable.NewDeliveryFailureStoreConfigAdapter(
		pkgdurable.DeliveryFailureStoreConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
		},
	)

	handler, err := BuildDelivery(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	if err == nil {
		t.Fatal("BuildDelivery() error = nil, want selected file archive construction error")
	}
	if handler == nil {
		t.Fatal("BuildDelivery() handler = nil, want cleanup composite")
	}
	if len(handler.Handlers) != 0 {
		t.Fatalf("BuildDelivery() handlers = %d, want no successfully constructed child", len(handler.Handlers))
	}
}

func TestBuildDeliveryCreatesDirectDegradeHandlerWithoutDurableStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := loggerdurable.NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
			EnableDegrade: true,
			DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			SpillType:     []pkgdurable.SpillType{pkgdurable.SpillType("stdout")},
		})

		handler, err := BuildDelivery(
			context.Background(),
			clock.NewUTCClock(),
			cfg,
			observer.NopTracer{},
			observer.NopMeter{},
			FactoryOptions{},
		)
		if err != nil {
			t.Fatalf("BuildDelivery error = %v, want nil", err)
		}
		if len(handler.Handlers) != 1 {
			t.Fatalf("BuildDelivery handlers = %d, want one direct degrade handler", len(handler.Handlers))
		}
		if _, ok := handler.Handlers[0].(*degradehandler.Handler); !ok {
			t.Fatalf("BuildDelivery handler type = %T, want *degradehandler.Handler", handler.Handlers[0])
		}
		// Deliveryのfailure処理は、一度に一件ずつ実行します。
		requireCompositeChildConcurrencyForTest(t, handler.CompositeFailureHandler, 1)
	})
}

func TestBuildDeliveryRejectsInvalidDirectDegradeSpillTypes(t *testing.T) {
	cfg := loggerdurable.NewDeliveryFailureStoreConfigAdapter(
		pkgdurable.DeliveryFailureStoreConfig{
			EnableDegrade: true,
			DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			SpillType:     []pkgdurable.SpillType{"stdout", "stdout"},
		},
	)

	handler, err := BuildDelivery(
		context.Background(),
		clock.NewUTCClock(),
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		FactoryOptions{},
	)
	requireSpillTypeErrorForTest(t, err, "stdout", pkgdurable.ErrDuplicateSpillType)
	if handler == nil || len(handler.Handlers) != 0 {
		t.Fatalf("BuildDelivery handler = %+v, want empty composite", handler)
	}
	// 設定errorと一緒に返すcompositeも、呼び出し側が通常どおり終了できます。
	if err := drainWithinHangTimeoutForTest(handler); err != nil {
		t.Fatalf("factory error reporting composite Drain() error = %v", err)
	}
}

func TestBuildDeliveryRejectsInvalidDurableTypesBeforeConstructingStore(t *testing.T) {
	tests := []struct {
		name     string
		types    []pkgdurable.DurableType
		wantKind error
		wantType pkgdurable.DurableType
	}{
		{
			name:     "empty",
			wantKind: pkgdurable.ErrDurableTypesRequired,
		},
		{
			name:     "spool",
			types:    []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
			wantKind: pkgdurable.ErrUnsupportedDurableType,
			wantType: pkgdurable.DurableTypeSpool,
		},
		{
			name: "duplicate",
			types: []pkgdurable.DurableType{
				pkgdurable.DurableTypeFileArchive,
				pkgdurable.DurableTypeFileArchive,
			},
			wantKind: pkgdurable.ErrDuplicateDurableType,
			wantType: pkgdurable.DurableTypeFileArchive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loggerdurable.NewDeliveryFailureStoreConfigAdapter(
				pkgdurable.DeliveryFailureStoreConfig{
					DurableType: tt.types,
				},
			)

			handler, err := BuildDelivery(
				context.Background(),
				clock.NewUTCClock(),
				cfg,
				observer.NopTracer{},
				observer.NopMeter{},
				FactoryOptions{},
			)
			requireDurableTypeErrorForTest(t, err, "delivery failure store", tt.wantType, tt.wantKind)
			if handler == nil {
				t.Fatal("BuildDelivery handler = nil, want empty composite")
			}
			if len(handler.Handlers) != 0 {
				t.Fatalf(
					"BuildDelivery handlers = %d, want 0 after fail-fast",
					len(handler.Handlers),
				)
			}
		})
	}
}

// requireDurableTypeErrorForTestは、errがscopeのdurable store選択でdurableTypeをkindの理由で
// 拒否したことを表すか確認します。
func requireDurableTypeErrorForTest(
	t *testing.T,
	err error,
	scope string,
	durableType pkgdurable.DurableType,
	kind error,
) {
	t.Helper()
	var typeErr *pkgdurable.DurableTypeError
	if !errors.Is(err, kind) || !errors.As(err, &typeErr) || typeErr.Scope != scope || typeErr.Type != durableType {
		t.Fatalf("error = %v, want %s durable type %q rejected by %v", err, scope, durableType, kind)
	}
}

// requireSpillTypeErrorForTestは、errが直接退避先の選択でspillTypeをkindの理由で拒否したことを
// 表すか確認します。
func requireSpillTypeErrorForTest(t *testing.T, err error, spillType pkgdurable.SpillType, kind error) {
	t.Helper()
	var typeErr *pkgdurable.SpillTypeError
	if !errors.Is(err, kind) || !errors.As(err, &typeErr) || typeErr.Type != spillType {
		t.Fatalf("error = %v, want direct degrade spill type %q rejected by %v", err, spillType, kind)
	}
}
