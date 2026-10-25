package durable

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	spoolconfig "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type durableAdapterTestCompressionCodec struct{}

func (*durableAdapterTestCompressionCodec) Format() pkgfsstore.CompressFormat { return "br" }
func (*durableAdapterTestCompressionCodec) Compress(_, _ *os.File) error      { return nil }
func (*durableAdapterTestCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return nil
}

// Delivery loggerはspoolを使用しないため、Delivery用のfailure store設定はspool設定を
// 公開しません。特定のmethod signatureとの一致ではなく、全methodの名前と戻り値を
// 検査し、戻り値型や名前を変えたspool設定の追加も検出します。
func TestDeliveryFailureStoreConfigAdapterDoesNotExposeSpoolConfig(t *testing.T) {
	adapter := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
	})

	var _ DeliveryFailureStoreConfigProvider = adapter
	durableStore := adapter.DeliveryDurableStoreConfig()
	var _ DeliveryDurableStoreConfigProvider = durableStore
	spoolStoreConfig := reflect.TypeOf((*spoolstore.SpoolStoreConfigProvider)(nil)).Elem()
	for _, typ := range []reflect.Type{
		reflect.TypeOf(adapter),
		reflect.PointerTo(reflect.TypeOf(adapter)),
		reflect.TypeOf(durableStore),
		reflect.PointerTo(reflect.TypeOf(durableStore)),
		reflect.TypeOf((*DeliveryFailureStoreConfigProvider)(nil)).Elem(),
		reflect.TypeOf((*DeliveryDurableStoreConfigProvider)(nil)).Elem(),
	} {
		for index := range typ.NumMethod() {
			method := typ.Method(index)
			if strings.Contains(strings.ToLower(method.Name), "spool") {
				t.Fatalf("%s must not expose spool config through %s", typ, method.Name)
			}
			for out := range method.Type.NumOut() {
				if result := method.Type.Out(out); result.Implements(spoolStoreConfig) {
					t.Fatalf("%s.%s returns spool store config %s", typ, method.Name, result)
				}
			}
		}
	}
}

func TestFailureStoreConfigAdaptersDoNotExposeIndependentEnableFlag(t *testing.T) {
	normal := NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
	})
	if _, ok := any(normal).(interface{ EnableFailureStore() bool }); ok {
		t.Fatal("normal failure store adapter must derive activation from DurableType")
	}

	delivery := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
	})
	if _, ok := any(delivery).(interface{ EnableFailureStore() bool }); ok {
		t.Fatal("delivery failure store adapter must derive activation from DurableType")
	}
}

func TestFailureStoreConfigAdaptersExposeObjectStorageUpload(t *testing.T) {
	want := objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Bucket: "failure-archive",
			Prefix: "backend/failures",
		},
	}

	normal := NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			FileArchiveConfig: pkgdurable.FileArchiveConfig{
				ObjectStorageUploadConfig: want,
			},
		},
	})
	assertObjectStorageUploadConfig(t, normal.DurableStoreConfig().FileArchiveConfig(), want)

	delivery := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
			ObjectStorageUploadConfig: want,
		},
	})
	assertObjectStorageUploadConfig(t, delivery.DeliveryDurableStoreConfig().FileArchiveConfig(), want)
}

func TestFailureStoreConfigAdaptersPreserveLifecycleRunnerConfig(t *testing.T) {
	want := pkgfsstore.AppendLogRunnerConfig{
		Interval:    2 * time.Minute,
		StopOnError: true,
		Supervisor: pkgfsstore.AppendLogRunnerSupervisorConfig{
			Enabled:      true,
			RestartDelay: 3 * time.Second,
			MaxRestarts:  4,
		},
	}

	normal := NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			FileArchiveConfig: pkgdurable.FileArchiveConfig{
				LifecycleRunnerConfig: want,
			},
		},
	})
	assertLifecycleRunnerConfig(t, normal.DurableStoreConfig().FileArchiveConfig(), want)

	delivery := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
			LifecycleRunnerConfig: want,
		},
	})
	assertLifecycleRunnerConfig(t, delivery.DeliveryDurableStoreConfig().FileArchiveConfig(), want)
}

func TestFailureStoreConfigAdaptersPreserveUsageCacheTTL(t *testing.T) {
	const wantUsageCacheTTL = 23 * time.Minute

	normal := NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			FileArchiveConfig: pkgdurable.FileArchiveConfig{
				UsageCacheTTL: wantUsageCacheTTL,
			},
		},
	})
	if got := normal.DurableStoreConfig().FileArchiveConfig().UsageCacheTTL(); got != wantUsageCacheTTL {
		t.Fatalf("normal UsageCacheTTL() = %s, want %s", got, wantUsageCacheTTL)
	}

	delivery := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
			UsageCacheTTL: wantUsageCacheTTL,
		},
	})
	if got := delivery.DeliveryDurableStoreConfig().FileArchiveConfig().UsageCacheTTL(); got != wantUsageCacheTTL {
		t.Fatalf("delivery UsageCacheTTL() = %s, want %s", got, wantUsageCacheTTL)
	}
}

func TestFailureStoreConfigAdaptersPreserveCompressionCodec(t *testing.T) {
	codec := &durableAdapterTestCompressionCodec{}

	normal := NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			FileArchiveConfig: pkgdurable.FileArchiveConfig{CompressionCodec: codec},
		},
	})
	assertCompressionCodec(t, normal.DurableStoreConfig().FileArchiveConfig(), codec)

	delivery := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{CompressionCodec: codec},
	})
	assertCompressionCodec(t, delivery.DeliveryDurableStoreConfig().FileArchiveConfig(), codec)
}

func TestFailureStoreConfigAdaptersExposeTopLevelDegradeGate(t *testing.T) {
	wantSpillTypes := []pkgdurable.SpillType{"stdout", "s3"}

	normal := NewFailureStoreConfigAdapter(pkgdurable.FailureStoreConfig{
		EnableDegrade: true,
		SpillType:     wantSpillTypes,
	})
	var _ FailureStoreConfigProvider = normal
	if !normal.EnableDegrade() {
		t.Fatal("normal EnableDegrade() = false, want true")
	}
	assertSpillTypes(t, normal.SpillType(), wantSpillTypes)

	delivery := NewDeliveryFailureStoreConfigAdapter(pkgdurable.DeliveryFailureStoreConfig{
		EnableDegrade: true,
		SpillType:     wantSpillTypes,
	})
	var _ DeliveryFailureStoreConfigProvider = delivery
	if !delivery.EnableDegrade() {
		t.Fatal("delivery EnableDegrade() = false, want true")
	}
	assertSpillTypes(t, delivery.SpillType(), wantSpillTypes)
}

func TestSpoolConfigAdapterExposesInternalHandlerConfig(t *testing.T) {
	want := pkgloggerhandler.HandlerConfig{
		OnErrorConfig: []string{"stdout", "slack"},
		OnWarnConfig:  []string{"stdout"},
		OnInfoConfig:  []string{"file"},
		OnDebugConfig: []string{"file", "stdout"},
	}
	adapter := NewSpoolConfigAdapter(pkgdurable.SpoolConfig{HandlerConfig: want})

	var _ SpoolConfigProvider = adapter
	assertStrings(t, "OnError", adapter.Handler().OnError(), want.OnErrorConfig)
	assertStrings(t, "OnWarn", adapter.Handler().OnWarn(), want.OnWarnConfig)
	assertStrings(t, "OnInfo", adapter.Handler().OnInfo(), want.OnInfoConfig)
	assertStrings(t, "OnDebug", adapter.Handler().OnDebug(), want.OnDebugConfig)
}

func TestSpoolOverloadPolicyConfigAdapterExposesActiveBacklogWarnAfterDays(t *testing.T) {
	const want = 17
	adapter := NewSpoolOverloadPolicyConfigAdapter(pkgdurable.SpoolOverloadPolicyConfig{
		ActiveBacklogWarnAfterDays: want,
	})

	var _ spoolconfig.SpoolStoreOverloadPolicyConfigProvider = adapter
	if got := adapter.ActiveBacklogWarnAfterDays(); got != want {
		t.Fatalf("ActiveBacklogWarnAfterDays() = %d, want %d", got, want)
	}
}

func assertStrings(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %q, want %q", name, i, got[i], want[i])
		}
	}
}

func assertSpillTypes(t *testing.T, got, want []pkgdurable.SpillType) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("SpillType() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SpillType()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func assertObjectStorageUploadConfig(
	t *testing.T,
	archiveConfig FileArchiveConfigProvider,
	want objectstorage.UploadTargetConfig,
) {
	t.Helper()

	got := archiveConfig.ObjectStorageUpload()
	if got.S3.Bucket != want.S3.Bucket || got.S3.Prefix != want.S3.Prefix {
		t.Fatalf("archive upload config = %+v, want %+v", got, want)
	}
}

func assertLifecycleRunnerConfig(
	t *testing.T,
	archiveConfig FileArchiveConfigProvider,
	want pkgfsstore.AppendLogRunnerConfig,
) {
	t.Helper()

	if got := archiveConfig.LifecycleRunnerConfig(); got != want {
		t.Fatalf("lifecycle runner config = %+v, want %+v", got, want)
	}
}

func assertCompressionCodec(
	t *testing.T,
	archiveConfig failurearchive.FileArchiveConfigProvider,
	want pkgfsstore.CompressionCodec,
) {
	t.Helper()

	provider, ok := archiveConfig.(failurearchive.CompressionCodecProvider)
	if !ok {
		t.Fatalf("archive config type %T does not provide compression codec", archiveConfig)
	}
	if got := provider.CompressionCodec(); got != want {
		t.Fatalf("compression codec = %T, want %T", got, want)
	}
}
