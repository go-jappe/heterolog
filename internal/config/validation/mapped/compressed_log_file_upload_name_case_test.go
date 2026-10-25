package mappedvalidation

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// compressedLogUploadNameCaseRulesは、保存先directoryが大小文字を区別する場合・しない場合を、
// OS名に依存せず両方試験するための判定です。
var compressedLogUploadNameCaseRules = []struct {
	name     string
	nameCase pathidentity.NameCase
}{
	{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
	{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
}

func fixedCompressedLogUploadNameCase(nameCase pathidentity.NameCase) mappedDirectoryNameCase {
	return func(string) pathidentity.NameCase { return nameCase }
}

func TestMappedSyncCompressedLogFileUploadNameCaseFollowsStorageDirectories(t *testing.T) {
	for _, label := range []string{"logger", "async logger"} {
		for _, mode := range []pkgfsstore.FileLifecycleMode{
			pkgfsstore.FileLifecycleModeInline,
			pkgfsstore.FileLifecycleModeRunner,
		} {
			for _, crossRole := range []bool{false, true} {
				role := "default"
				internalTarget := "APP"
				if crossRole {
					role = "error"
					internalTarget = "ERROR_APP"
				}
				t.Run(label+"/"+string(mode)+"/"+role, func(t *testing.T) {
					base := compressedLogUploadCollisionTestSyncFile("app")
					base.LifecycleMode = mode
					base.LifecycleRunnerSettings.Interval = time.Minute
					base.FsstoreConfig.Layout.LogDir = "application"
					if crossRole {
						base.FsstoreConfig.Lifecycle.MaxFileSize = 1 << 20
						base.FsstoreConfig.Lifecycle.MaxFileBackups = 5
						base.FsstoreConfig.Lifecycle.MaxFileAge = 5
						base.FsstoreConfig.Lifecycle.MaxDirSize = 50 << 20
						base.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
						base.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
						base.ObjectStorageUploadConfig.Error = compressedLogUploadCollisionTestTarget()
						base.ObjectStorageUploadConfig.Error.S3.Bucket = "error-archive-bucket"
					}
					internal := compressedLogUploadCollisionTestSyncFile(internalTarget)
					internal.LifecycleMode = mode
					internal.LifecycleRunnerSettings.Interval = time.Minute
					internal.FsstoreConfig.Layout.LogDir = "internal"
					cfg := pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
						File:                  base,
						InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
							File: internal,
						},
					}
					contract := strings.ReplaceAll(label, " ", "_")
					for _, rule := range compressedLogUploadNameCaseRules {
						err := validateMappedSyncCompressedLogFileUploadNameCollisionsForNameCase(
							label, cfg, fixedCompressedLogUploadNameCase(rule.nameCase))
						if rule.nameCase == pathidentity.NameCaseSensitive {
							if err != nil {
								t.Fatalf("%s: case-distinct series must remain valid: %v", rule.name, err)
							}
							continue
						}
						assertCompressedLogUploadNameCollision(t, err,
							contract+".file."+role, contract+".internal_log.file.default")
					}
					// 公開の検証も、OS名ではなく保存先directoryの判定を使います。
					want := validateMappedSyncCompressedLogFileUploadNameCollisionsForNameCase(
						label, cfg, pathidentity.DirectoryNameCase)
					err := ValidateSyncLoggerConfig(label, cfg)
					if want == nil {
						if err != nil {
							t.Fatalf("case-sensitive storage directories must accept case-distinct series: %v", err)
						}
						return
					}
					assertCompressedLogUploadNameCollision(t, err,
						contract+".file."+role, contract+".internal_log.file.default")
				})
			}
		}
	}
}

func TestMappedDeliveryCompressedLogFileUploadNameCaseFollowsStorageDirectories(t *testing.T) {
	base := compressedLogUploadCollisionTestDeliveryFile("delivery")
	base.FsstoreConfig.Layout.LogDir = "application"
	base.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	base.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
	base.FsstoreConfig.Rotation.MaxFileSize = 1 << 20
	base.LifecycleRunnerSettings.MaxFileBackups = 5
	base.LifecycleRunnerSettings.MaxFileAge = 5
	base.LifecycleRunnerSettings.MaxDirSize = 50 << 20
	base.LifecycleRunnerSettings.ObjectStorageUploadConfig.Error = compressedLogUploadCollisionTestTarget()
	base.LifecycleRunnerSettings.ObjectStorageUploadConfig.Error.S3.Bucket = "error-archive-bucket"
	internal := compressedLogUploadCollisionTestDeliveryFile("ERROR_DELIVERY")
	internal.FsstoreConfig.Layout.LogDir = "internal"
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File:                  base,
		InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
			File: internal,
		},
	}
	for _, rule := range compressedLogUploadNameCaseRules {
		t.Run(rule.name, func(t *testing.T) {
			err := validateMappedDeliveryCompressedLogFileUploadNameCollisionsForNameCase(
				"delivery logger", cfg, fixedCompressedLogUploadNameCase(rule.nameCase))
			if rule.nameCase == pathidentity.NameCaseSensitive {
				if err != nil {
					t.Fatalf("case-distinct series must remain valid: %v", err)
				}
				return
			}
			assertCompressedLogUploadNameCollision(t, err,
				"delivery_logger.file.error", "delivery_logger.internal_log.file.default")
		})
	}
	t.Run("directory rule", func(t *testing.T) {
		want := validateMappedDeliveryCompressedLogFileUploadNameCollisionsForNameCase(
			"delivery logger", cfg, pathidentity.DirectoryNameCase)
		err := ValidateDeliveryLoggerConfig("delivery logger", cfg)
		if want == nil {
			if err != nil {
				t.Fatalf("case-sensitive storage directories must accept case-distinct series: %v", err)
			}
			return
		}
		assertCompressedLogUploadNameCollision(t, err,
			"delivery_logger.file.error", "delivery_logger.internal_log.file.default")
	})
}

func TestMappedCompressedLogFileUploadSeriesCaseFollowsStorageDirectories(t *testing.T) {
	base := mappedCompressedLogFileUploadSeries{
		source: "logger.file.error", service: objectstorage.UploadTargetS3,
		prefix: "error_", target: "app", extension: "log",
		directories: []string{filepath.Join("logs", "application")},
	}
	tests := []struct {
		name      string
		prefix    string
		target    string
		extension string
		collides  bool
		caseOnly  bool
	}{
		{name: "same name across role boundary", target: "error_app", extension: "log", collides: true},
		{name: "prefix case", prefix: "ERROR_", target: "app", extension: "log", caseOnly: true},
		{name: "target case", prefix: "error_", target: "APP", extension: "log", caseOnly: true},
		{name: "extension case", prefix: "error_", target: "app", extension: "LOG", caseOnly: true},
		{name: "different target", prefix: "error_", target: "other", extension: "log"},
		{name: "different extension", prefix: "error_", target: "app", extension: "jsonl"},
	}
	for _, rule := range compressedLogUploadNameCaseRules {
		for _, tt := range tests {
			t.Run(rule.name+"/"+tt.name, func(t *testing.T) {
				internal := mappedCompressedLogFileUploadSeries{
					source: "logger.internal_log.file.default", service: objectstorage.UploadTargetS3,
					prefix: tt.prefix, target: tt.target, extension: tt.extension,
					directories: []string{filepath.Join("logs", "internal")},
				}
				err := validateMappedCompressedLogFileUploadSeriesCollisions(
					[]mappedCompressedLogFileUploadSeries{base},
					[]mappedCompressedLogFileUploadSeries{internal},
					fixedCompressedLogUploadNameCase(rule.nameCase),
				)
				// 大小文字だけの違いは、大小文字を区別しない保存先でだけ同じファイル名です。
				collides := tt.collides || (tt.caseOnly && rule.nameCase == pathidentity.NameCaseInsensitive)
				if (err != nil) != collides {
					t.Fatalf("collision error = %v, want collision %t", err, collides)
				}
				if err != nil {
					collision := requireCompressedLogUploadNameCollision(t, err, base.source, internal.source)
					if collision.Internal.Prefix != tt.prefix || collision.Internal.Target != tt.target ||
						collision.Internal.Extension != tt.extension || collision.Base.Prefix != base.prefix ||
						collision.Base.Target != base.target || collision.Base.Extension != base.extension {
						t.Fatalf("diagnostic must preserve configured spelling: %+v", collision)
					}
				}
			})
		}
	}
}

func TestMappedCompressedLogFileUploadSeriesUnicodeCaseFollowsStorageDirectories(t *testing.T) {
	t.Parallel()
	base := mappedCompressedLogFileUploadSeries{
		source: "logger.file.error", service: objectstorage.UploadTargetS3,
		prefix: "kernel_", target: "äpp", extension: "log",
		directories: []string{filepath.Join("logs", "application")},
	}
	internal := mappedCompressedLogFileUploadSeries{
		source: "logger.internal_log.file.default", service: objectstorage.UploadTargetS3,
		prefix: "Kernel_", target: "Äpp", extension: "Log",
		directories: []string{filepath.Join("logs", "internal")},
	}
	for _, rule := range compressedLogUploadNameCaseRules {
		err := validateMappedCompressedLogFileUploadSeriesCollisions(
			[]mappedCompressedLogFileUploadSeries{base},
			[]mappedCompressedLogFileUploadSeries{internal},
			fixedCompressedLogUploadNameCase(rule.nameCase),
		)
		if collides := err != nil; collides != (rule.nameCase == pathidentity.NameCaseInsensitive) {
			t.Fatalf("%s: Unicode case-only name change: collision = %t: %v", rule.name, collides, err)
		}
		if err != nil {
			collision := requireCompressedLogUploadNameCollision(t, err, base.source, internal.source)
			if collision.Internal.Prefix != internal.prefix || collision.Internal.Target != internal.target ||
				collision.Internal.Extension != internal.extension || collision.Base.Prefix != base.prefix ||
				collision.Base.Target != base.target || collision.Base.Extension != base.extension {
				t.Fatalf("diagnostic must preserve configured spelling: %+v", collision)
			}
		}
	}
}

func TestMappedCompressedLogFileUploadNameCaseUsesAnyInsensitiveStorageDirectory(t *testing.T) {
	t.Parallel()
	baseDirectory := filepath.Join("logs", "application")
	internalDirectory := filepath.Join("logs", "internal")
	sharedArchive := filepath.Join("state", "archive")
	base := mappedCompressedLogFileUploadSeries{
		source: "logger.file.default", service: objectstorage.UploadTargetS3,
		target: "app", extension: "log", directories: []string{baseDirectory, sharedArchive},
	}
	internal := mappedCompressedLogFileUploadSeries{
		source: "logger.internal_log.file.default", service: objectstorage.UploadTargetS3,
		target: "APP", extension: "log", directories: []string{internalDirectory, sharedArchive},
	}
	for _, insensitiveDirectory := range []string{"", baseDirectory, internalDirectory, sharedArchive} {
		var queried []string
		err := validateMappedCompressedLogFileUploadSeriesCollisions(
			[]mappedCompressedLogFileUploadSeries{base},
			[]mappedCompressedLogFileUploadSeries{internal},
			func(directory string) pathidentity.NameCase {
				queried = append(queried, directory)
				if directory == insensitiveDirectory {
					return pathidentity.NameCaseInsensitive
				}
				return pathidentity.NameCaseSensitive
			},
		)
		// 元ログの保存先・共有する退避先のどれか一つでも大小文字を区別しなければ衝突です。
		if collides := err != nil; collides != (insensitiveDirectory != "") {
			t.Fatalf("insensitive directory %q: collision = %t: %v", insensitiveDirectory, collides, err)
		}
		if insensitiveDirectory == "" {
			// 衝突がない場合は、元ログ・退避先の全保存先の規則を確認しています。問い合わせの
			// 順序や同じdirectoryの重複は実装の詳細のため、含まれていることだけを確認します。
			for _, want := range []string{baseDirectory, internalDirectory, sharedArchive} {
				if !slices.Contains(queried, want) {
					t.Fatalf("queried directories = %q, want %q to be queried", queried, want)
				}
			}
		}
	}
}

func TestMappedCompressedLogFileUploadSeriesIncludesLogAndFailureStoreDirectories(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	layout := pkgfsstore.FsstoreConfig{
		Target:                     "app",
		LogPath:                    filepath.Join(root, "logs"),
		LogDir:                     "application",
		ErrorLogDir:                "errors",
		IsSeparateErrorLogsEnabled: true,
		SeparateErrorLogsPrefix:    "error_",
		IsSeparateDebugLogsEnabled: true,
		SeparateDebugLogsPrefix:    "debug_",
	}
	archiveRoot := filepath.Join(root, "state", "app", "archive")
	spool := objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: filepath.Join(root, "spool"), Target: "app",
	})
	upload := compressedLogUploadCollisionTestConfig()
	upload.CompressedLogFileUploadSpool = spool
	upload.Default.CompressedLogFileUploadFailureStore = objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool},
		DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{
			FileArchiveConfig: objectstorage.CompressedLogFileUploadFailureArchiveConfig{RootPath: archiveRoot},
		},
	}
	upload.Error = compressedLogUploadCollisionTestTarget()
	upload.Debug = compressedLogUploadCollisionTestTarget()
	series := mappedCompressedLogFileUploadSeriesForConfig("logger.file", layout, upload)
	want := map[string][]string{
		"logger.file.default": {filepath.Join(root, "logs", "app", "application"), archiveRoot, filepath.Join(root, "spool", "app")},
		// 退避先を選択しない出力は、元ログの保存先だけで判定します。
		"logger.file.error": {filepath.Join(root, "logs", "app", "errors")},
		// debugの個別directoryが無効なら、通常ログのdirectoryを共有します。
		"logger.file.debug": {filepath.Join(root, "logs", "app", "application")},
	}
	if len(series) != len(want) {
		t.Fatalf("series = %+v, want %d series", series, len(want))
	}
	for _, current := range series {
		if !slices.Equal(current.directories, want[current.source]) {
			t.Fatalf("%s directories = %q, want %q", current.source, current.directories, want[current.source])
		}
	}
}
