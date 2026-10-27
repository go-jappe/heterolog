package file

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// standardLogCaptureForTestは、標準loggerへの出力を記録します。
type standardLogCaptureForTest struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (c *standardLogCaptureForTest) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffer.Write(p)
}

func (c *standardLogCaptureForTest) lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return splitLogLinesForTest(c.buffer.String())
}

// captureStandardLogLinesForTestは、試験の間だけ標準loggerの出力を記録し、記録した行を返す
// 関数を返します。標準loggerはprocess全体で共有されるため、t.Parallelの試験では使いません。
func captureStandardLogLinesForTest(t *testing.T) func() []string {
	t.Helper()
	capture := &standardLogCaptureForTest{}
	previous := log.Writer()
	log.SetOutput(capture)
	t.Cleanup(func() { log.SetOutput(previous) })
	return capture.lines
}

// fileNotificationTargetForTestは、内部ログの通知先として使うfileのfallback handlerと、
// その書込み先を調べる手段です。
type fileNotificationTargetForTest struct {
	directories loggerDirectoryPaths
	// notifyは、error levelの内部ログを一件通知します。
	notify func(message string)
	// errorRoleFileは、error roleのloggerのactive fileを返します。
	errorRoleFile func() string
}

// 内部ログの通知先としてのfile（LoggerSet・DeliveryLoggerSetのfallback handler）は、fallbackだけで
// 使う構成でもfileを開かずに済むよう、roleのloggerを最初の通知で作ります。作成はdirectoryやfileの
// 状態に依存して実行時に失敗し得ます。失敗した通知は黙って捨てず、docs/reference.ja.mdのとおり失敗ごとに
// 標準loggerへ一行出力し、作成は次の通知で再試行します。失敗を記憶して再試行しない退行や、
// 出力を一度に抑える退行を検出します。
func TestFileNotificationTargetReportsEachCreationFailureAndRetries(t *testing.T) {
	ctx := context.Background()
	notifyWith := func(handler func(context.Context, error, *logging.LogEntry)) func(string) {
		return func(message string) {
			handler(ctx, os.ErrInvalid, &logging.LogEntry{Level: pkglogger.LogLevelError, Message: message})
		}
	}
	targets := []struct {
		name  string
		build func(t *testing.T) fileNotificationTargetForTest
	}{
		{name: "logger set", build: func(t *testing.T) fileNotificationTargetForTest {
			set, err := NewLoggerSet(NewConfigAdapter(newTestFileLoggerConfig(t.TempDir(), "logs", "notification-retry")), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			handler := set.NewNamedErrorFallbackHandler().Handler
			return fileNotificationTargetForTest{
				directories: set.directoryPaths,
				notify: notifyWith(func(ctx context.Context, err error, entry *logging.LogEntry) {
					handler(ctx, err, entry, nil, nil)
				}),
				errorRoleFile: func() string {
					logger, err := set.NewErrorFileLogger(ctx)
					require.NoError(t, err)
					return logger.Fsstore.ActiveFilePath
				},
			}
		}},
		{name: "delivery logger set", build: func(t *testing.T) fileNotificationTargetForTest {
			cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "notification-retry")
			set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			handler := set.NewNamedErrorFallbackHandler().Handler
			return fileNotificationTargetForTest{
				directories: set.directoryPaths,
				notify: notifyWith(func(ctx context.Context, err error, entry *logging.LogEntry) {
					handler(ctx, err, entry, nil, nil)
				}),
				errorRoleFile: func() string {
					logger, err := set.NewErrorFileLogger(ctx)
					require.NoError(t, err)
					return logger.Fsstore.ActiveFilePath
				},
			}
		}},
	}
	for _, target := range targets {
		t.Run(target.name, func(t *testing.T) {
			notificationTarget := target.build(t)
			// error roleの保存先を通常fileで塞ぎ、通知のたびのlogger作成を失敗させます。
			errorDirectory := notificationTarget.directories[ErrorLogger]
			require.NoError(t, os.MkdirAll(filepath.Dir(errorDirectory), 0o750))
			require.NoError(t, os.WriteFile(errorDirectory, []byte("obstruction"), 0o600))
			standardLogLines := captureStandardLogLinesForTest(t)

			const failedNotifications = 3
			for i := range failedNotifications {
				notificationTarget.notify(fmt.Sprintf("notification while the directory is obstructed %d", i))
			}
			// 失敗ごとに標準loggerへ一行出力することはdocs/reference.ja.mdに書かれた出力の要件のため、行の数と、
			// 各行が作成失敗の書式の文言を含むことを確認します。
			lines := standardLogLines()
			require.Len(t, lines, failedNotifications, "every failed notification must print one standard logger line: %q", lines)
			creationFailure := strings.SplitN(MsgFailedCreateFallbackFileLogger, "%", 2)[0]
			for _, line := range lines {
				require.Contains(t, line, creationFailure)
			}

			// 保存先が使えるようになれば、次の通知でloggerの作成を再試行し、その通知を書き込みます。
			require.NoError(t, os.Remove(errorDirectory))
			const recovered = "notification after the directory became usable"
			notificationTarget.notify(recovered)
			require.Len(t, standardLogLines(), failedNotifications, "the notification after recovery must not fail")
			assertLogFileContains(t, notificationTarget.errorRoleFile(), recovered)
		})
	}
}

// loggerの作成失敗は、原因のerrorをerrors.Isで判別できる形で返します。LoggerSetの遅延作成は
// 構築時に検証済みの設定を再検証しないため、作成処理そのものが返す時刻表示の設定errorと
// upload先の構築errorも、包んだ後に原因へ届く必要があります。
func TestFileLoggerCreationErrorsKeepTheirCause(t *testing.T) {
	ctx := context.Background()
	invalidTimeZone := pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: "Invalid/Time_Zone"}
	cases := []struct {
		name   string
		want   error
		create func(t *testing.T) (any, error)
	}{
		{name: "file logger time config", want: logging.ErrInvalidTimeZone, create: func(t *testing.T) (any, error) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "creation-cause")
			cfg.TimeConfig = invalidTimeZone
			adapter := NewConfigAdapter(cfg)
			directories, err := resolveLoggerDirectoryPaths(adapter.Fsstore().Layout())
			require.NoError(t, err)
			return newFileLoggerBase(ctx, adapter, ErrorLogger, InternalHandlers{}, true, nil, nil, directories)
		}},
		{name: "file logger upload destination", want: compose.ErrUploadTargetsRequired, create: func(t *testing.T) (any, error) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "creation-cause")
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
			// 送信先のtargetがないupload設定は、uploaderの構築で拒否されます。
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
				Enabled: true, Default: objectstorage.UploadTargetConfig{Enabled: true},
			}
			adapter := NewConfigAdapter(cfg)
			directories, err := resolveLoggerDirectoryPaths(adapter.Fsstore().Layout())
			require.NoError(t, err)
			return newFileLoggerBase(ctx, adapter, DefaultLogger, InternalHandlers{}, true, nil, nil, directories)
		}},
		{name: "delivery file logger time config", want: logging.ErrInvalidTimeZone, create: func(t *testing.T) (any, error) {
			cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "creation-cause")
			cfg.TimeConfig = invalidTimeZone
			adapter := NewDeliveryFileLoggerConfigAdapter(cfg)
			directories, err := resolveLoggerDirectoryPaths(adapter.Fsstore().Layout())
			require.NoError(t, err)
			return newDeliveryFileLoggerBase(ctx, adapter, ErrorLogger, nil, nil, directories[ErrorLogger])
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, err := tc.create(t)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, logger)
		})
	}
}

// logDirectoryOperationForTestは、loggerの作成が保存先のdirectoryに対して行う操作です。
type logDirectoryOperationForTest int

const (
	// createSubdirectoryForTestは、directoryの中にroleの保存先directoryを作る操作です。
	createSubdirectoryForTest logDirectoryOperationForTest = iota
	// listEntriesForTestは、directoryの既存fileを列挙して次の世代番号を決める操作です。
	listEntriesForTest
	// createFileForTestは、directoryの中にlog fileを作って開く操作です。
	createFileForTest
)

// requireLogDirectoryOperationDeniedForTestは、directoryに対するoperationが権限不足で失敗する
// ことを確かめます。実行userが権限を無視できる環境（rootなど）では拒否を再現できないためskipします。
func requireLogDirectoryOperationDeniedForTest(t *testing.T, directory string, operation logDirectoryOperationForTest) {
	t.Helper()
	var err error
	switch operation {
	case createSubdirectoryForTest:
		probe := filepath.Join(directory, "permission-probe")
		if err = os.Mkdir(probe, 0o750); err == nil {
			require.NoError(t, os.Remove(probe))
		}
	case listEntriesForTest:
		_, err = os.ReadDir(directory)
	case createFileForTest:
		var probe *os.File
		if probe, err = os.CreateTemp(directory, ".permission-probe-*"); err == nil {
			require.NoError(t, probe.Close())
			require.NoError(t, os.Remove(probe.Name()))
		}
	}
	if err == nil {
		t.Skip("current user or filesystem bypasses directory permissions")
	}
	require.ErrorIs(t, err, os.ErrPermission, "the permission fixture failed with another error")
}

// loggerの作成で、roleの保存先directoryの作成、既存fileの列挙、log fileのopenのどれが権限不足で
// 失敗しても、原因のerrorをerrors.Isで判別できる形で返します（os.ErrPermission）。
func TestFileLoggerCreationFileSystemErrorsKeepTheirCause(t *testing.T) {
	ctx := context.Background()
	operations := []struct {
		name      string
		operation logDirectoryOperationForTest
		// prepareは、前提のdirectoryを作り、operationを拒否するdirectoryを返します。
		prepare func(t *testing.T, roleDirectory string) string
	}{
		{name: "create directory", operation: createSubdirectoryForTest, prepare: func(t *testing.T, roleDirectory string) string {
			parent := filepath.Dir(roleDirectory)
			require.NoError(t, os.MkdirAll(parent, 0o750))
			return parent
		}},
		{name: "list existing files", operation: listEntriesForTest, prepare: func(t *testing.T, roleDirectory string) string {
			require.NoError(t, os.MkdirAll(roleDirectory, 0o750))
			return roleDirectory
		}},
		{name: "open log file", operation: createFileForTest, prepare: func(t *testing.T, roleDirectory string) string {
			require.NoError(t, os.MkdirAll(roleDirectory, 0o750))
			return roleDirectory
		}},
	}
	loggers := []struct {
		name string
		// createは、error roleがまだ作られていない集合を作り、error roleの保存先と、そのroleの
		// loggerを作成してerrorを返す関数を返します。
		create func(t *testing.T) (string, func() error)
	}{
		{name: "logger set", create: func(t *testing.T) (string, func() error) {
			set, err := NewLoggerSet(NewConfigAdapter(newTestFileLoggerConfig(t.TempDir(), "logs", "creation-permission")), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			return set.directoryPaths[ErrorLogger], func() error {
				logger, err := set.NewErrorFileLogger(ctx)
				require.Nil(t, logger)
				return err
			}
		}},
		{name: "delivery logger set", create: func(t *testing.T) (string, func() error) {
			cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "creation-permission")
			set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			return set.directoryPaths[ErrorLogger], func() error {
				logger, err := set.NewErrorFileLogger(ctx)
				require.Nil(t, logger)
				return err
			}
		}},
	}
	for _, operation := range operations {
		for _, logger := range loggers {
			t.Run(operation.name+"/"+logger.name, func(t *testing.T) {
				roleDirectory, createErrorRole := logger.create(t)
				denyLogDirectoryOperationForTest(t, operation.prepare(t, roleDirectory), operation.operation)
				require.ErrorIs(t, createErrorRole(), os.ErrPermission)
			})
		}
	}
}

// log本文の時刻表示の設定（local time表示のtimezoneと日時の書式）は、内部ログの通知先として
// だけ使うroleでも構築時に検証し、通知のたびのlogger作成の失敗として残しません。単体constructor、
// LoggerSet、DeliveryLoggerSetのどれも、ログ保存先を作成する前に拒否します。
func TestFileLoggerConstructorsRejectInvalidTimestampConfig(t *testing.T) {
	type timestampMutation func(*pkglogger.TimeConfig, *pkglogger.LogBodyConfig)
	settings := []struct {
		name   string
		want   error
		mutate timestampMutation
	}{
		{name: "time zone", want: logging.ErrInvalidTimeZone, mutate: func(timeConfig *pkglogger.TimeConfig, _ *pkglogger.LogBodyConfig) {
			*timeConfig = pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: "Invalid/Time_Zone"}
		}},
		{name: "date format", want: timestamp.ErrLayoutRequired, mutate: func(_ *pkglogger.TimeConfig, body *pkglogger.LogBodyConfig) {
			body.LogLineDateFormat = ""
		}},
	}
	ctx := context.Background()
	constructors := []struct {
		name      string
		construct func(t *testing.T, root string, mutate timestampMutation) error
	}{
		{name: "standalone", construct: func(t *testing.T, root string, mutate timestampMutation) error {
			cfg := newTestFileLoggerConfig(root, "logs", "timestamp-config")
			mutate(&cfg.TimeConfig, &cfg.LogBodyConfig)
			logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
			if logger != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
			}
			return err
		}},
		{name: "logger set", construct: func(t *testing.T, root string, mutate timestampMutation) error {
			cfg := newTestFileLoggerConfig(root, "logs", "timestamp-config")
			mutate(&cfg.TimeConfig, &cfg.LogBodyConfig)
			set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
			if set != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			}
			return err
		}},
		{name: "delivery standalone", construct: func(t *testing.T, root string, mutate timestampMutation) error {
			cfg := newTestDeliveryFileLoggerConfig(root, "logs", "timestamp-config")
			mutate(&cfg.TimeConfig, &cfg.LogBodyConfig)
			logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
			if logger != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
			}
			return err
		}},
		{name: "delivery logger set", construct: func(t *testing.T, root string, mutate timestampMutation) error {
			cfg := newTestDeliveryFileLoggerConfig(root, "logs", "timestamp-config")
			mutate(&cfg.TimeConfig, &cfg.LogBodyConfig)
			set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
			if set != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			}
			return err
		}},
	}
	for _, setting := range settings {
		for _, constructor := range constructors {
			t.Run(setting.name+"/"+constructor.name, func(t *testing.T) {
				// 対照の設定は、検証対象の設定だけが異なります。対照を受理し対象だけを拒否することで、
				// 拒否の原因がその設定であることを確認します。
				require.NoError(t, constructor.construct(t, t.TempDir(), func(*pkglogger.TimeConfig, *pkglogger.LogBodyConfig) {}))

				root := t.TempDir()
				err := constructor.construct(t, root, setting.mutate)
				require.ErrorIs(t, err, setting.want)
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				require.Empty(t, entries, "the configuration error must be reported before the storage directory is created")
			})
		}
	}
}
