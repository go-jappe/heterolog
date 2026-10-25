package logger

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestLoggerConstructorsRejectRolePurgeBeforeIO(t *testing.T) {
	for _, contract := range []string{"logger", "async logger", "delivery logger"} {
		for _, internal := range []bool{false, true} {
			label := contract + " file"
			if internal {
				label = contract + " internal log file"
			}
			for _, role := range []string{"error", "debug"} {
				for _, backups := range []int{0, -1} {
					t.Run(label+"/"+role+"/"+map[int]string{0: "zero", -1: "negative"}[backups], func(t *testing.T) {
						root := filepath.Join(t.TempDir(), "uncreated")
						file := rolePurgeConstructionFileConfig(root)
						purge := &pkgfsstore.FilePurgeConfig{MaxFileBackups: backups, MaxFileAge: 3, MaxDirSize: 30 << 20}
						if role == "error" {
							file.FsstoreConfig.Lifecycle.ErrorPurge = purge
						} else {
							file.FsstoreConfig.Lifecycle.DebugPurge = purge
						}
						cfg := pkglogger.LoggerConfig{
							Timeout: time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, File: file,
						}
						if internal {
							cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: file}
							cfg.File = rolePurgeConstructionFileConfig(root)
							cfg.File.FsstoreConfig.Layout.LogDir = "base"
						}
						err := constructRolePurgeTestLogger(t, contract, cfg)
						fields := filePurgeConfigFields
						if contract == "delivery logger" {
							fields = deliveryFilePurgeConfigFields
						}
						purgeRole := appendlog.PurgeRole(role)
						requireSingleFilePurgeConfigError(t, err, label, fields[purgeRole], purgeRole, fsstore.ErrInvalidMaxFileBackups)
						if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("constructor created filesystem resources: stat error = %v", err)
						}
					})
				}
			}
		}
	}
}

func TestFilePurgePreflightSkipsInactiveConfiguration(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, mode := range []pkgfsstore.FileLifecycleMode{"", pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable} {
			if delivery && mode == pkgfsstore.FileLifecycleModeInline {
				continue
			}
			name := "sync/" + string(mode)
			if delivery {
				name = "delivery/" + string(mode)
			}
			t.Run(name, func(t *testing.T) {
				file := rolePurgeConstructionFileConfig("/unused")
				file.LifecycleMode = mode
				file.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{}
				cfg := pkglogger.LoggerConfig{File: file}
				validate := func() error {
					if delivery {
						return validateDeliveryLoggerFilePurgeConfigs("delivery logger", rolePurgeConstructionDeliveryConfig(cfg))
					}
					return validateLoggerFilePurgeConfigs("logger", cfg)
				}
				active := mode != pkgfsstore.FileLifecycleModeDisable
				if delivery {
					active = mode == pkgfsstore.FileLifecycleModeRunner
				}
				if err := validate(); (err != nil) != active {
					t.Fatalf("preflight error = %v, active = %v", err, active)
				}
				file.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
				if err := validate(); err != nil {
					t.Fatalf("disabled role error = %v", err)
				}
				file.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.Stdout = &pkglogger.StdoutLoggerConfig{}
				if err := validate(); err != nil {
					t.Fatalf("unused base error = %v", err)
				}
				cfg.Stdout.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}}
				cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: rolePurgeConstructionFileConfig("/internal")}
				if err := validate(); err != nil {
					t.Fatalf("unused base replaced by dedicated destination error = %v", err)
				}
			})
		}
	}
}

func TestFilePurgePreflightResolvesInternalDirectoryCapacity(t *testing.T) {
	base := rolePurgeConstructionFileConfig("/unused")
	base.FsstoreConfig.Layout.ErrorLogDir = base.FsstoreConfig.Layout.LogDir
	base.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{MaxFileBackups: 3, MaxFileAge: 3}
	internal := *base
	internal.FsstoreConfig.Layout.LogDir = "internal"
	internal.FsstoreConfig.Layout.ErrorLogDir = "internal-error"
	cfg := pkglogger.LoggerConfig{
		File:                    base,
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{File: &internal},
	}
	for _, delivery := range []bool{false, true} {
		var err error
		if delivery {
			err = validateDeliveryLoggerFilePurgeConfigs("delivery logger", rolePurgeConstructionDeliveryConfig(cfg))
		} else {
			err = validateLoggerFilePurgeConfigs("logger", cfg)
		}
		// 専用保存先のerror系列だけを拒否し、通常logとdirectoryを共有する系列の容量は
		// 通常logの値へ揃えて受理します。
		source, fields := "logger internal log file", filePurgeConfigFields
		if delivery {
			source, fields = "delivery logger internal log file", deliveryFilePurgeConfigFields
		}
		requireSingleFilePurgeConfigError(t, err, source, fields[appendlog.PurgeRoleError], appendlog.PurgeRoleError, fsstore.ErrInvalidMaxDirSize)
	}
	if base.FsstoreConfig.Lifecycle.ErrorPurge.MaxDirSize != 0 {
		t.Fatal("preflight mutated inherited role capacity")
	}
}

// requireSingleFilePurgeConfigErrorは、errに含まれる保持設定の不正が一件だけであり、
// その設定の出所・項目・系列と項目別sentinelが期待どおりであることをerrors.Is/Asで確認します。
func requireSingleFilePurgeConfigError(
	t *testing.T,
	err error,
	source string,
	field pkglogger.ConfigField,
	role appendlog.PurgeRole,
	sentinel error,
) {
	t.Helper()
	fieldErrs := pkglogger.ConfigFieldErrors(err)
	if len(fieldErrs) != 1 {
		t.Fatalf("config field errors = %v, want exactly one from %s; error = %v", fieldErrs, source, err)
	}
	fieldErr := fieldErrs[0]
	if fieldErr.Source != source || fieldErr.Backend != pkglogger.LogOutputFile || fieldErr.Field != field {
		t.Fatalf("config field error = %+v, want source=%q backend=%q field=%q", fieldErr, source, pkglogger.LogOutputFile, field)
	}
	var purgeErr *appendlog.PurgeConfigError
	if !errors.As(fieldErr, &purgeErr) || purgeErr.Role != role {
		t.Fatalf("purge config error = %v, want %s series", fieldErr, role)
	}
	if !errors.Is(fieldErr, sentinel) {
		t.Fatalf("purge config error = %v, want %v", fieldErr, sentinel)
	}
}

func rolePurgeConstructionFileConfig(root string) *pkglogger.FileLoggerConfig {
	return &pkglogger.FileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				LogPath: root, Target: "app", LogDir: "default", ErrorLogDir: "error", DebugLogDir: "debug",
				IsSeparateDebugLogDirEnabled: true,
				IsSeparateErrorLogsEnabled:   true, IsSeparateDebugLogsEnabled: true,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				MaxFileBackups: 5, MaxFileAge: 5, MaxFileSize: 1 << 20, MaxDirSize: 50 << 20,
			},
		},
		ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{
			Directory: filepath.Join(root, "checkpoints"), SourcePrefix: "test",
		},
	}
}

func rolePurgeConstructionDeliveryConfig(cfg pkglogger.LoggerConfig) pkglogger.DeliveryLoggerConfig {
	convertFile := func(file *pkglogger.FileLoggerConfig) *pkglogger.DeliveryFileLoggerConfig {
		if file == nil {
			return nil
		}
		lifecycle := file.FsstoreConfig.Lifecycle
		return &pkglogger.DeliveryFileLoggerConfig{
			LifecycleMode: file.LifecycleMode,
			FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
				Layout:   file.FsstoreConfig.Layout,
				Rotation: pkgfsstore.FileRotationConfig{MaxFileSize: lifecycle.MaxFileSize},
			},
			LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
				MaxFileBackups: lifecycle.MaxFileBackups, MaxFileAge: lifecycle.MaxFileAge, MaxDirSize: lifecycle.MaxDirSize,
				ErrorPurge: lifecycle.ErrorPurge, DebugPurge: lifecycle.DebugPurge,
				ArchiveCheckpointConfig: file.ArchiveCheckpointConfig,
			},
		}
	}
	delivery := pkglogger.DeliveryLoggerConfig{
		File: convertFile(cfg.File), LogOutputDestinations: cfg.LogOutputDestinations,
		Timeout: cfg.Timeout, MaxLogEntryBytes: cfg.MaxLogEntryBytes,
	}
	if cfg.InternalLogDestinations != nil {
		delivery.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{
			File: convertFile(cfg.InternalLogDestinations.File),
		}
	}
	if cfg.Stdout != nil {
		delivery.Stdout = &pkglogger.DeliveryStdoutLoggerConfig{HandlerConfig: cfg.Stdout.HandlerConfig}
	}
	return delivery
}

func constructRolePurgeTestLogger(t *testing.T, contract string, cfg pkglogger.LoggerConfig) error {
	t.Helper()
	var created io.Closer
	var err error
	switch contract {
	case "logger":
		created, err = NewLogger(context.Background(), cfg, streamrouting.None, nil, nil, nil, nil)
	case "async logger":
		created, err = NewAsyncLogger(context.Background(), pkglogger.AsyncBestEffortMultiLoggerConfig{
			File: cfg.File, InternalLogDestinations: cfg.InternalLogDestinations,
			Timeout: cfg.Timeout, MaxParallel: 1, MaxLogEntryBytes: cfg.MaxLogEntryBytes,
		}, streamrouting.None, nil, nil, nil, nil)
	case "delivery logger":
		created, err = NewDeliveryLogger(context.Background(), rolePurgeConstructionDeliveryConfig(cfg), streamrouting.None, nil, nil)
	default:
		t.Fatalf("unexpected contract %q", contract)
	}
	if created != nil {
		t.Cleanup(func() { _ = created.Close() })
	}
	return err
}

// Timeout、MaxParallel、MaxLogEntryBytesはfan-out構築時にも検証しますが、constructorは
// log rootなどfilesystem上のresourceを作る前に拒否します。file設定自体は構築できる値なので、
// 検証が後段に残るとrootを作成してから失敗します。
func TestLoggerConstructorsRejectInvalidScalarConfigBeforeIO(t *testing.T) {
	fileConfig := func(t *testing.T, root string) *pkglogger.FileLoggerConfig {
		file := newActivationFileConfig(t, "scalar-validation")
		file.FsstoreConfig.Layout.LogPath = root
		return file
	}
	syncConfig := func(t *testing.T, root string, timeout time.Duration, maxLogEntryBytes int) pkglogger.LoggerConfig {
		return pkglogger.LoggerConfig{Timeout: timeout, MaxLogEntryBytes: maxLogEntryBytes, File: fileConfig(t, root)}
	}
	asyncConfig := func(t *testing.T, root string, timeout time.Duration, maxParallel, maxLogEntryBytes int) pkglogger.AsyncBestEffortMultiLoggerConfig {
		return pkglogger.AsyncBestEffortMultiLoggerConfig{
			Timeout: timeout, MaxParallel: maxParallel, MaxLogEntryBytes: maxLogEntryBytes,
			File: fileConfig(t, root),
		}
	}
	deliveryConfig := func(root string, timeout time.Duration, maxLogEntryBytes int) pkglogger.DeliveryLoggerConfig {
		return pkglogger.DeliveryLoggerConfig{
			Timeout: timeout, MaxLogEntryBytes: maxLogEntryBytes,
			File: newCloseOwnershipDeliveryFileConfig(root, "scalar-validation"),
		}
	}
	const validBytes = pkglogger.DefaultMaxLogEntryBytes
	tests := []struct {
		name      string
		want      error
		construct func(t *testing.T, root string) (io.Closer, error)
	}{
		{"logger/timeout", multi.ErrInvalidTimeout, func(t *testing.T, root string) (io.Closer, error) {
			return NewLogger(context.Background(), syncConfig(t, root, 0, validBytes), streamrouting.None, nil, nil, nil, nil)
		}},
		{"logger/max log entry bytes", pkglogger.ErrInvalidMaxLogEntryBytes, func(t *testing.T, root string) (io.Closer, error) {
			return NewLogger(context.Background(), syncConfig(t, root, time.Second, 0), streamrouting.None, nil, nil, nil, nil)
		}},
		{"async logger/timeout", multi.ErrInvalidTimeout, func(t *testing.T, root string) (io.Closer, error) {
			return NewAsyncLogger(context.Background(), asyncConfig(t, root, 0, 1, validBytes), streamrouting.None, nil, nil, nil, nil)
		}},
		{"async logger/max parallel", multi.ErrInvalidMaxParallel, func(t *testing.T, root string) (io.Closer, error) {
			return NewAsyncLogger(context.Background(), asyncConfig(t, root, time.Second, 0, validBytes), streamrouting.None, nil, nil, nil, nil)
		}},
		{"async logger/max log entry bytes", pkglogger.ErrInvalidMaxLogEntryBytes, func(t *testing.T, root string) (io.Closer, error) {
			return NewAsyncLogger(context.Background(), asyncConfig(t, root, time.Second, 1, 0), streamrouting.None, nil, nil, nil, nil)
		}},
		{"delivery logger/timeout", multi.ErrInvalidTimeout, func(t *testing.T, root string) (io.Closer, error) {
			return NewDeliveryLogger(context.Background(), deliveryConfig(root, 0, validBytes), streamrouting.None, nil, nil)
		}},
		{"delivery logger/max log entry bytes", pkglogger.ErrInvalidMaxLogEntryBytes, func(t *testing.T, root string) (io.Closer, error) {
			return NewDeliveryLogger(context.Background(), deliveryConfig(root, time.Second, 0), streamrouting.None, nil, nil)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "uncreated")
			created, err := tt.construct(t, root)
			if created != nil {
				t.Cleanup(func() { _ = created.Close() })
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("constructor error = %v, want %v", err, tt.want)
			}
			if created != nil {
				t.Fatalf("constructor returned %T with an error", created)
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("constructor created filesystem resources: stat error = %v", err)
			}
		})
	}
}
