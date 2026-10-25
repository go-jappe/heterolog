package appendlog

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestLogFileRolePurgeAdaptersResolveDirectoryCapacity(t *testing.T) {
	for _, tt := range []struct {
		name        string
		errorDir    string
		debugDir    string
		separateDir bool
		wantError   uint64
		wantDebug   uint64
		// caseAliasは、大小文字だけが異なるdirectoryの共有を保存先directoryの規則で決める行です。
		caseAlias bool
	}{
		{name: "independent", errorDir: "errors", debugDir: "debug", separateDir: true, wantError: 40, wantDebug: 30},
		{name: "all shared", errorDir: "logs", debugDir: "logs", separateDir: true, wantError: 20, wantDebug: 20},
		{name: "case aliases follow directory rules", errorDir: "LOGS", debugDir: "Logs", separateDir: true, caseAlias: true},
		{name: "error shares normal", errorDir: "unused/../logs", debugDir: "debug", separateDir: true, wantError: 20, wantDebug: 30},
		{name: "debug directory separation disabled", errorDir: "errors", wantError: 40, wantDebug: 20},
		{name: "disabled debug directory ignores stale path and capacity", errorDir: "errors", debugDir: "old-debug", wantError: 40, wantDebug: 20},
	} {
		rules := directoryCaseRulesForTest
		if !tt.caseAlias {
			// 大小文字に依存しない行は、規則を差し替えない保存先directoryの判定だけで確認します。
			rules = rules[len(rules)-1:]
		}
		for _, rule := range rules {
			t.Run(tt.name+"/"+rule.name, func(t *testing.T) {
				errorPurge := &pkgfsstore.FilePurgeConfig{MaxFileBackups: 6, MaxFileAge: 30, MaxDirSize: 40 << 20}
				debugPurge := &pkgfsstore.FilePurgeConfig{MaxFileBackups: 2, MaxFileAge: 1, MaxDirSize: 30 << 20}
				layout := pkgfsstore.FsstoreConfig{
					LogPath: t.TempDir(), Target: "app", LogDir: "logs",
					ErrorLogDir: tt.errorDir, DebugLogDir: tt.debugDir,
					IsSeparateDebugLogDirEnabled: tt.separateDir,
					IsSeparateErrorLogsEnabled:   true, IsSeparateDebugLogsEnabled: true,
				}
				wantError, wantDebug := tt.wantError, tt.wantDebug
				if ignoresCase := useDirectoryCaseRuleForTest(t, layout.LogPath, rule.nameCase); tt.caseAlias {
					// 大小文字を区別しないdirectoryでは、error・debugも通常ログと同じdirectoryの容量を使います。
					wantError, wantDebug = 40, 30
					if ignoresCase {
						wantError, wantDebug = 20, 20
					}
				}
				adapters := []ConfigProvider{
					NewConfigAdapter(pkgfsstore.Config{
						Layout: layout,
						Lifecycle: pkgfsstore.FileLifecycleConfig{
							UsageCacheTTL: 2 * time.Minute, MaxFileBackups: 4, MaxFileAge: 7,
							MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
							ErrorPurge: errorPurge, DebugPurge: debugPurge,
						},
					}),
					NewDeliveryFileLifecycleStoreConfigAdapter(pkgfsstore.DeliveryFileLifecycleStoreConfig{
						Layout: layout,
						Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
							UsageCacheTTL: 2 * time.Minute, MaxFileBackups: 4, MaxFileAge: 7,
							MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
							ErrorPurge: errorPurge, DebugPurge: debugPurge,
						},
					}),
				}
				for _, cfg := range adapters {
					require.EqualValues(t, 6, cfg.ErrorPurge().MaxFileBackups())
					require.EqualValues(t, 30, cfg.ErrorPurge().MaxFileAge())
					require.Equal(t, wantError<<20, cfg.ErrorPurge().MaxDirSize())
					require.EqualValues(t, 2, cfg.DebugPurge().MaxFileBackups())
					require.EqualValues(t, 1, cfg.DebugPurge().MaxFileAge())
					require.Equal(t, wantDebug<<20, cfg.DebugPurge().MaxDirSize())
					require.Equal(t, 2*time.Minute, cfg.ErrorPurge().UsageCacheTTL())
					require.Equal(t, 2*time.Minute, cfg.DebugPurge().UsageCacheTTL())
					require.NoError(t, ValidateLogFilePurgeConfigs(cfg))
				}
				// 容量の解決で入力pointerを書き換えず、adapter生成後の入力変更にも追従しない。
				require.EqualValues(t, 40<<20, errorPurge.MaxDirSize)
				require.EqualValues(t, 30<<20, debugPurge.MaxDirSize)
				errorPurge.MaxFileAge = 999
				debugPurge.MaxDirSize = 999
				for _, cfg := range adapters {
					require.EqualValues(t, 30, cfg.ErrorPurge().MaxFileAge())
					require.Equal(t, wantDebug<<20, cfg.DebugPurge().MaxDirSize())
				}
			})
		}
	}
}

func TestLogFileRolePurgeAdaptersInheritUnspecifiedSettings(t *testing.T) {
	override := &pkgfsstore.FilePurgeConfig{MaxFileBackups: 6, MaxFileAge: 30, MaxDirSize: 40 << 20}
	for _, tt := range []struct {
		name       string
		errorPurge *pkgfsstore.FilePurgeConfig
		debugPurge *pkgfsstore.FilePurgeConfig
	}{
		{name: "both inherit"},
		{name: "error override only", errorPurge: override},
		{name: "debug override only", debugPurge: override},
	} {
		t.Run(tt.name, func(t *testing.T) {
			layout := pkgfsstore.FsstoreConfig{
				LogDir: "logs", ErrorLogDir: "errors", DebugLogDir: "debug",
				IsSeparateDebugLogDirEnabled: true,
				IsSeparateErrorLogsEnabled:   true, IsSeparateDebugLogsEnabled: true,
			}
			adapters := []ConfigProvider{
				NewConfigAdapter(pkgfsstore.Config{
					Layout: layout,
					Lifecycle: pkgfsstore.FileLifecycleConfig{
						UsageCacheTTL: 2 * time.Minute, MaxFileBackups: 4, MaxFileAge: 7,
						MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
						ErrorPurge: tt.errorPurge, DebugPurge: tt.debugPurge,
					},
				}),
				NewDeliveryFileLifecycleStoreConfigAdapter(pkgfsstore.DeliveryFileLifecycleStoreConfig{
					Layout: layout,
					Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
						UsageCacheTTL: 2 * time.Minute, MaxFileBackups: 4, MaxFileAge: 7,
						MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
						ErrorPurge: tt.errorPurge, DebugPurge: tt.debugPurge,
					},
				}),
			}
			for _, cfg := range adapters {
				// nilはconsumer側の補完ではなく、adapter生成時点で通常Purgeへ解決します。
				for _, role := range []struct {
					purge    FilePurgeConfigProvider
					override *pkgfsstore.FilePurgeConfig
				}{
					{purge: cfg.ErrorPurge(), override: tt.errorPurge},
					{purge: cfg.DebugPurge(), override: tt.debugPurge},
				} {
					require.NotNil(t, role.purge)
					if role.override == nil {
						require.Same(t, cfg.Purge(), role.purge)
					} else {
						require.Equal(t, role.override.MaxFileBackups, role.purge.MaxFileBackups())
						require.Equal(t, role.override.MaxFileAge, role.purge.MaxFileAge())
						require.Equal(t, role.override.MaxDirSize, role.purge.MaxDirSize())
					}
					require.Equal(t, 2*time.Minute, role.purge.UsageCacheTTL())
				}
				require.Equal(t, 4, cfg.Purge().MaxFileBackups())
				require.Equal(t, 7, cfg.Purge().MaxFileAge())
				require.EqualValues(t, 20<<20, cfg.Purge().MaxDirSize())
				require.NoError(t, ValidateLogFilePurgeConfigs(cfg))
			}
		})
	}
}

func TestWithPurgeConfigPreservesDependenciesAndResolvedInheritance(t *testing.T) {
	codec := &userDefinedAppendLogCompressionCodec{}
	layout := pkgfsstore.FsstoreConfig{LogDir: "logs", ErrorLogDir: "errors"}
	errorPurge := &pkgfsstore.FilePurgeConfig{MaxFileAge: 30}
	adapters := []ConfigProvider{
		NewConfigAdapter(pkgfsstore.Config{
			CompressionCodec: codec,
			Layout:           layout,
			Lifecycle:        pkgfsstore.FileLifecycleConfig{MaxFileAge: 7, ErrorPurge: errorPurge},
		}),
		NewDeliveryFileLifecycleStoreConfigAdapter(pkgfsstore.DeliveryFileLifecycleStoreConfig{
			CompressionCodec: codec,
			Layout:           layout,
			Lifecycle:        pkgfsstore.DeliveryFileLifecycleConfig{MaxFileAge: 7, ErrorPurge: errorPurge},
		}),
	}
	for _, cfg := range adapters {
		require.Equal(t, 30, cfg.ErrorPurge().MaxFileAge())
		require.Equal(t, 7, cfg.Purge().MaxFileAge())
		require.Same(t, cfg.Purge(), cfg.DebugPurge())
		for _, purge := range []FilePurgeConfigProvider{cfg.ErrorPurge(), cfg.DebugPurge()} {
			selected := WithPurgeConfig(cfg, purge)
			require.Same(t, purge, selected.Purge())
			require.Same(t, cfg.Layout(), selected.Layout())
			require.Same(t, cfg.Lifecycle(), selected.Lifecycle())
			require.Same(t, cfg.Rotation(), selected.Rotation())
			require.Same(t, cfg.Duration(), selected.Duration())
			require.Same(t, cfg.Retry(), selected.Retry())
			require.Same(t, cfg.ErrorPurge(), selected.ErrorPurge())
			require.Same(t, cfg.DebugPurge(), selected.DebugPurge())
			require.Same(t, codec, selected.(CompressionCodecProvider).CompressionCodec())
		}
		require.Equal(t, 7, cfg.Purge().MaxFileAge())
	}
}

func TestLogFileRolePurgeValidationPreservesExplicitZeroAndNegative(t *testing.T) {
	layout := pkgfsstore.FsstoreConfig{
		LogDir: "logs", ErrorLogDir: "errors", DebugLogDir: "debug",
		IsSeparateDebugLogDirEnabled: true,
		IsSeparateErrorLogsEnabled:   true, IsSeparateDebugLogsEnabled: true,
	}
	errorPurge := &pkgfsstore.FilePurgeConfig{}
	debugPurge := &pkgfsstore.FilePurgeConfig{MaxFileBackups: -1, MaxFileAge: -1}
	for _, enabled := range []bool{true, false} {
		layout.IsSeparateErrorLogsEnabled = enabled
		layout.IsSeparateDebugLogsEnabled = enabled
		adapters := []ConfigProvider{
			NewConfigAdapter(pkgfsstore.Config{
				Layout: layout,
				Lifecycle: pkgfsstore.FileLifecycleConfig{
					MaxFileBackups: 4, MaxFileAge: 7, MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
					ErrorPurge: errorPurge, DebugPurge: debugPurge,
				},
			}),
			NewDeliveryFileLifecycleStoreConfigAdapter(pkgfsstore.DeliveryFileLifecycleStoreConfig{
				Layout: layout,
				Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
					MaxFileBackups: 4, MaxFileAge: 7, MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
					ErrorPurge: errorPurge, DebugPurge: debugPurge,
				},
			}),
		}
		for _, cfg := range adapters {
			// 非nilの明示値は補完せず、分離系列が有効な場合に検証で拒否します。
			require.Zero(t, cfg.ErrorPurge().MaxFileBackups())
			require.Zero(t, cfg.ErrorPurge().MaxFileAge())
			require.Zero(t, cfg.ErrorPurge().MaxDirSize())
			require.Equal(t, -1, cfg.DebugPurge().MaxFileBackups())
			require.Equal(t, -1, cfg.DebugPurge().MaxFileAge())
			require.Zero(t, cfg.DebugPurge().MaxDirSize())
			err := ValidateLogFilePurgeConfigs(cfg)
			if enabled {
				require.Equal(t, map[PurgeRole]bool{PurgeRoleError: true, PurgeRoleDebug: true}, purgeConfigErrorRolesForTest(err))
				require.ErrorIs(t, err, fsstore.ErrInvalidMaxFileBackups)
				require.ErrorIs(t, err, fsstore.ErrInvalidMaxFileAge)
			} else {
				require.NoError(t, err)
			}
		}
	}
}

func TestLogFileRolePurgeValidationRejectsUnresolvedProviders(t *testing.T) {
	cfg := NewConfigAdapter(pkgfsstore.Config{
		Layout: pkgfsstore.FsstoreConfig{
			LogDir: "logs", ErrorLogDir: "errors",
			IsSeparateErrorLogsEnabled: true, IsSeparateDebugLogsEnabled: true,
		},
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			MaxFileBackups: 4, MaxFileAge: 7, MaxFileSize: 1 << 20, MaxDirSize: 20 << 20,
		},
	}).(*ConfigAdapter)
	// adapterの返却契約に違反したproviderは、利用側で継承したりpanicする前に拒否します。
	cfg.ErrorPurgeConfig = nil
	cfg.DebugPurgeConfig = nil
	err := ValidateLogFilePurgeConfigs(cfg)
	require.ErrorIs(t, err, ErrPurgeConfigRequired)
	require.Equal(t, map[PurgeRole]bool{PurgeRoleError: true, PurgeRoleDebug: true}, purgeConfigErrorRolesForTest(err))
}

// purgeConfigErrorRolesForTestは、Joinされたerrorに含まれるPurgeConfigErrorの系列を返します。
// errors.Asは最初の一件だけを返すため、複数系列の確認にはerror treeを辿ります。
func purgeConfigErrorRolesForTest(err error) map[PurgeRole]bool {
	roles := make(map[PurgeRole]bool)
	var visit func(error)
	visit = func(err error) {
		switch wrapped := err.(type) {
		case *PurgeConfigError:
			roles[wrapped.Role] = true
		case interface{ Unwrap() []error }:
			for _, cause := range wrapped.Unwrap() {
				visit(cause)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return roles
}
