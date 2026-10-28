package logger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	filelogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	stdoutlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// このfileとself_notification_network_wiring_test.goの試験は、本番と同じ配線
// （NewLogger・NewAsyncLogger・NewDeliveryLoggerとhandler selector）で、各出力先が自分自身
// （またはもう一方の出力先）を内部ログの通知先に選んだ構成を作り、通知の原因となる条件
// （書き込み遅延・書き込み失敗・過負荷・送信先の失敗や遅延）を続けます。handler関数は
// 注入しません。次を確認します。
//   - 元の記録一件あたりの内部ログの件数が決まった値で、通知が繰り返されないこと
//   - どの処理も停止しないこと（hang検出の上限付きで待ちます）
//   - 内部ログが通知先の出力に届くこと（観測できる出力先の場合）
//   - handler.ReentryGuardが一度も再入を検出しないこと。本番の通知先は通知元loggerの
//     通常の書き込み経路へ戻らないため、この構成はguardに依存していません。
//
// 標準出力・標準エラー・標準logger・再入の検出回数はprocess全体の状態なので、これらの試験は
// t.Parallelを使いません（並行試験は逐次試験がすべて終わってから実行されます）。

// selfNotificationWriteNestingLimitは、file書き込みのspanの入れ子の上限です。正しい配線では
// 元の書き込みと通知先の書き込みの2段です。通知先が通常の書き込み経路を使う壊れた配線でも、
// guardがあれば3段で止まります。guardもない壊れた実装が再帰した場合にだけ超えます。
const selfNotificationWriteNestingLimit = 6

// errSelfNotificationWriteNestingExceededは、file書き込みの入れ子が上限を超えた場合に
// selfNotificationTracerがpanicで通知の処理を打ち切る値です。
var errSelfNotificationWriteNestingExceeded = errors.New("file writes nested beyond the limit: notification targets kept writing through the notifying path")

type selfNotificationWriteDepthKey struct{}

// selfNotificationTracerは、loggerへ渡すtracerとして、contextの連鎖の中のfile書き込みspan
// （通常の書き込みと内部ログのfallback書き込み）の入れ子の深さを数えます。上限を超えた場合は
// 記録してpanicし、内部ログの通知のfan-outが持つrecoverで再帰を打ち切ります。壊れた実装でも
// stack overflowやgoroutineの増加でtest binaryを落とさず、試験の失敗として報告するための
// 安全網です。正しい実装では上限に達しません。
type selfNotificationTracer struct {
	exceeded atomic.Bool
}

func (tr *selfNotificationTracer) StartSpan(
	ctx context.Context,
	name string,
	attrs ...pkgobserver.Attr,
) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if name == filelogger.SpanFileWrite || name == filelogger.SpanFallbackFileWrite {
		depth, _ := ctx.Value(selfNotificationWriteDepthKey{}).(int)
		depth++
		if depth > selfNotificationWriteNestingLimit {
			tr.exceeded.Store(true)
			panic(errSelfNotificationWriteNestingExceeded)
		}
		ctx = context.WithValue(ctx, selfNotificationWriteDepthKey{}, depth)
	}
	return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
}

// selfNotificationMeterは、loggerへ渡すmeterとして、counterの名前ごとの累計を記録します。
type selfNotificationMeter struct {
	mu     sync.Mutex
	totals map[string]int64
}

func (m *selfNotificationMeter) Counter(name string) pkgobserver.Counter {
	return selfNotificationCounter{meter: m, name: name}
}

func (m *selfNotificationMeter) Gauge(string) pkgobserver.Gauge {
	return observer.NopGauge{}
}

func (m *selfNotificationMeter) Histogram(string) pkgobserver.Histogram {
	return observer.NopHistogram{}
}

func (m *selfNotificationMeter) total(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totals[name]
}

type selfNotificationCounter struct {
	meter *selfNotificationMeter
	name  string
}

func (c selfNotificationCounter) Add(_ context.Context, value int64, _ ...pkgobserver.Attr) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	if c.meter.totals == nil {
		c.meter.totals = make(map[string]int64)
	}
	c.meter.totals[c.name] += value
}

// selfNotificationSignalは、記録が増えるたびに待機中の呼び出し元を起こします。
type selfNotificationSignal struct {
	mu      sync.Mutex
	changed chan struct{}
}

func (s *selfNotificationSignal) wait() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

func (s *selfNotificationSignal) notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed != nil {
		close(s.changed)
		s.changed = nil
	}
}

// waitUntilは、condが成り立つまで記録の増加を待ちます。reentrytest.HangTimeout以内に
// 成り立たなければfalseを返します。
func (s *selfNotificationSignal) waitUntil(cond func() bool) bool {
	timer := time.NewTimer(reentrytest.HangTimeout)
	defer timer.Stop()
	for {
		changed := s.wait()
		if cond() {
			return true
		}
		select {
		case <-changed:
		case <-timer.C:
			return cond()
		}
	}
}

// selfNotificationStandardLogは、試験の間だけ標準loggerの出力を記録します。通知先の
// fallback自身の失敗は、通知されず標準loggerへ出力されます。
type selfNotificationStandardLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func captureSelfNotificationStandardLog(t *testing.T) *selfNotificationStandardLog {
	t.Helper()
	capture := &selfNotificationStandardLog{}
	previous := log.Writer()
	log.SetOutput(capture)
	t.Cleanup(func() { log.SetOutput(previous) })
	return capture
}

func (l *selfNotificationStandardLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// countは、substrを含む行の数を返します。
func (l *selfNotificationStandardLog) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return countSelfNotificationLines(selfNotificationLines(l.buf.String()), substr)
}

func (l *selfNotificationStandardLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// selfNotificationEnvは、一つの試験の観測をまとめます。tracerとmeterは本番の配線の
// constructorへ渡します。
type selfNotificationEnv struct {
	tracer      *selfNotificationTracer
	meter       *selfNotificationMeter
	standardLog *selfNotificationStandardLog
}

// runSelfNotificationScenarioは、本番の配線で構築したloggerを使うscenarioを実行し、その間に
// ReentryGuardが再入を一度も検出しなかったこと、file書き込みの入れ子が上限を超えなかった
// ことを確認します。scenarioはloggerをCloseしてから戻ります。scenarioの確認が先に失敗して
// 試験を終えた場合も、この二つは必ず確認します。
func runSelfNotificationScenario(t *testing.T, scenario func(t *testing.T, env *selfNotificationEnv)) {
	t.Helper()
	env := &selfNotificationEnv{
		tracer:      &selfNotificationTracer{},
		meter:       &selfNotificationMeter{},
		standardLog: captureSelfNotificationStandardLog(t),
	}
	detectionsBefore := handler.ReentryDetections()
	defer func() {
		if got := handler.ReentryDetections() - detectionsBefore; got != 0 {
			t.Errorf("ReentryGuard detected %d re-entrant writes in a production-wired configuration; production notification targets must never re-enter the notifying logger's write path", got)
		}
		if env.tracer.exceeded.Load() {
			t.Errorf("file writes nested deeper than %d: a notification target kept writing through the notifying write path", selfNotificationWriteNestingLimit)
		}
	}()
	scenario(t, env)
}

// completeSelfNotificationStepは、fを停止の検出付きで実行します。停止した場合はfが保持する
// lockを待つ後片付けを避けるため、試験をその場で終えます。
func completeSelfNotificationStep(t *testing.T, what string, f func() error) error {
	t.Helper()
	err, finished := reentrytest.CompleteWithin(t, what, f)
	if !finished {
		t.FailNow()
	}
	return err
}

// completeSelfNotificationStepWhileProgressingは、多数の書き込みをまとめたfを、completedが
// 増え続ける間は待ちます。completedがreentrytest.HangTimeoutの間増えなければ停止として
// 試験を失敗させ、I/Oの混雑で全体が遅いだけの実行と区別します。停止した場合、fは止まった
// まま残るため、completeSelfNotificationStepと同じく後片付けを行わずに終えます。
func completeSelfNotificationStepWhileProgressing(t *testing.T, what string, completed *atomic.Int64, f func() error) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- f() }()
	ticker := time.NewTicker(reentrytest.HangTimeout)
	defer ticker.Stop()
	last := completed.Load()
	for {
		select {
		case err := <-result:
			return err
		case <-ticker.C:
			current := completed.Load()
			if current == last {
				t.Errorf("%s made no progress within %s (%d completed)", what, reentrytest.HangTimeout, current)
				t.FailNow()
			}
			last = current
		}
	}
}

// selfNotificationHandlersは、全levelの内部ログの通知先にnamesを選ぶ設定です。
func selfNotificationHandlers(names ...pkglogger.LogOutputDestination) pkgloggerhandler.HandlerConfig {
	selectors := make([]string, 0, len(names))
	for _, name := range names {
		selectors = append(selectors, name.String())
	}
	return pkgloggerhandler.HandlerConfig{
		OnErrorConfig: append([]string(nil), selectors...),
		OnWarnConfig:  append([]string(nil), selectors...),
		OnInfoConfig:  append([]string(nil), selectors...),
		OnDebugConfig: append([]string(nil), selectors...),
	}
}

// selfNotificationEventsは、各試験で記録する元のログの件数です。
const selfNotificationEvents = 3

// selfNotificationApplicationMessageは、元のログのmessageに共通する文字列です。
const selfNotificationApplicationMessage = "application write"

func selfNotificationMessage(index int) string {
	return fmt.Sprintf("%s %d", selfNotificationApplicationMessage, index)
}

// selfNotificationContractは、loggerを構築するtop-level APIです。
type selfNotificationContract string

const (
	selfNotificationSync     selfNotificationContract = "sync"
	selfNotificationAsync    selfNotificationContract = "async"
	selfNotificationDelivery selfNotificationContract = "delivery"
)

// selfNotificationLoggerは、構築したtop-level loggerへの記録とCloseをcontractに依らず扱います。
type selfNotificationLogger struct {
	contract selfNotificationContract
	// writeはindex番目の元のログをINFOで記録し、同期API・Delivery APIの結果を返します。
	// 非同期APIは結果を返さないためnilです。
	write func(index int) error
	close func() error
	// fileLoggerSetとdeliveryFileLoggerSetは、file出力を持つ構成の共有LoggerSetです。
	fileLoggerSet         *filelogger.LoggerSet
	deliveryFileLoggerSet *filelogger.DeliveryLoggerSet
}

// newSelfNotificationLoggerは、contractのtop-level constructorでloggerを構築します。
// sync・asyncはcfg、deliveryはdeliveryCfgを使います。
func newSelfNotificationLogger(
	t *testing.T,
	env *selfNotificationEnv,
	contract selfNotificationContract,
	cfg pkglogger.LoggerConfig,
	deliveryCfg pkglogger.DeliveryLoggerConfig,
) *selfNotificationLogger {
	t.Helper()
	ctx := context.Background()
	switch contract {
	case selfNotificationSync:
		cfg.Timeout = time.Minute
		cfg.MaxLogEntryBytes = pkglogger.DefaultMaxLogEntryBytes
		logger, err := NewLogger(ctx, cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, env.tracer, env.meter)
		require.NoError(t, err)
		runtime, ok := logger.(*topLevelLoggerRuntime)
		require.True(t, ok)
		return &selfNotificationLogger{
			contract: contract,
			write: func(index int) error {
				return runtime.Info(ctx, selfNotificationMessage(index))
			},
			close:         runtime.Close,
			fileLoggerSet: runtime.fileLoggerSet,
		}
	case selfNotificationAsync:
		asyncCfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
			LogOutputDestinations:   cfg.LogOutputDestinations,
			Timeout:                 time.Minute,
			MaxParallel:             1,
			MaxLogEntryBytes:        pkglogger.DefaultMaxLogEntryBytes,
			File:                    cfg.File,
			Stdout:                  cfg.Stdout,
			Slack:                   cfg.Slack,
			OpenSearch:              cfg.OpenSearch,
			OpenSearchClient:        cfg.OpenSearchClient,
			Loki:                    cfg.Loki,
			LokiClient:              cfg.LokiClient,
			CloudWatchLogs:          cfg.CloudWatchLogs,
			CloudWatchLogsClient:    cfg.CloudWatchLogsClient,
			ClickHouse:              cfg.ClickHouse,
			ClickHouseClient:        cfg.ClickHouseClient,
			InternalLogDestinations: cfg.InternalLogDestinations,
		}
		logger, err := NewAsyncLogger(ctx, asyncCfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, env.tracer, env.meter)
		require.NoError(t, err)
		runtime, ok := logger.(*topLevelAsyncLoggerRuntime)
		require.True(t, ok)
		return &selfNotificationLogger{
			contract: contract,
			write: func(index int) error {
				runtime.Info(ctx, selfNotificationMessage(index))
				return nil
			},
			close:         runtime.Close,
			fileLoggerSet: runtime.fileLoggerSet,
		}
	case selfNotificationDelivery:
		deliveryCfg.Timeout = time.Minute
		deliveryCfg.MaxLogEntryBytes = pkglogger.DefaultMaxLogEntryBytes
		logger, err := NewDeliveryLogger(ctx, deliveryCfg, pkgcloudwatchlogs.StreamRoutingNone, env.tracer, env.meter)
		require.NoError(t, err)
		runtime, ok := logger.(*topLevelDeliveryLoggerRuntime)
		require.True(t, ok)
		return &selfNotificationLogger{
			contract: contract,
			write: func(index int) error {
				_, err := runtime.Info(ctx, selfNotificationMessage(index))
				return err
			},
			close:                 runtime.Close,
			deliveryFileLoggerSet: runtime.fileLoggerSet,
		}
	default:
		t.Fatalf("unknown contract %q", contract)
		return nil
	}
}

// writeEventsは、selfNotificationEvents件の元のログを停止の検出付きで記録し、各結果を返します。
func (l *selfNotificationLogger) writeEvents(t *testing.T) []error {
	t.Helper()
	results := make([]error, 0, selfNotificationEvents)
	for index := range selfNotificationEvents {
		results = append(results, completeSelfNotificationStep(t, "a write", func() error {
			return l.write(index)
		}))
	}
	return results
}

// closeLoggerは、loggerを停止の検出付きでCloseします。
func (l *selfNotificationLogger) closeLogger(t *testing.T) {
	t.Helper()
	require.NoError(t, completeSelfNotificationStep(t, "Close", l.close))
}

// newSelfNotificationFileConfigは、background処理を持たないdisable modeのfile出力設定です。
// 書き込み遅延の閾値は1時間にして、試験が意図しない遅延の通知を除きます。
func newSelfNotificationFileConfig(t *testing.T, target string) *pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := newActivationFileConfig(t, target)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = time.Hour
	return cfg
}

func newSelfNotificationDeliveryFileConfig(t *testing.T, target string) *pkglogger.DeliveryFileLoggerConfig {
	t.Helper()
	return newCloseOwnershipDeliveryFileConfig(t.TempDir(), target)
}

// reportEverySelfNotificationFileWriteAsDelayedは、全ての書き込みを閾値超過の遅延として
// errorで通知させます。負の閾値なので、時計の分解能（Windowsの約15.6ms）で計測値が0でも
// 毎回超過します。
func reportEverySelfNotificationFileWriteAsDelayed(cfg *pkglogger.FileLoggerConfig) {
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = -time.Nanosecond
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = -time.Nanosecond
}

// overloadEverySelfNotificationFileWriteは、空き容量の下限を最大値にして、全ての書き込みを
// 過負荷としてpolicyで処理させます（degradeの退避先は標準出力です）。
func overloadEverySelfNotificationFileWrite(cfg *pkglogger.FileLoggerConfig, policy pkgfsstore.OverloadPolicy) {
	cfg.FsstoreConfig.Overload = pkgfsstore.FileArchiveOverloadPolicyConfig{
		OverloadPolicy:        policy,
		SpillDestination:      pkgfsstore.SpillDestinationStdout,
		MinAvailableDiskSpace: math.MaxUint64,
	}
}

// selfNotificationClosedFileは、閉じたfileのhandleを返します。書き込みはos.ErrClosedで失敗します。
func selfNotificationClosedFile(t *testing.T) *os.File {
	t.Helper()
	closed, err := os.CreateTemp(t.TempDir(), "closed-*.log")
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	return closed
}

// failSelfNotificationFileWritesは、本番の配線が作った既定roleのfile loggerのactive fileの
// handleを、読み取り専用で開いた別のfileのhandleへ置き換え、全ての書き込みattemptを失敗させます。
// 条件は試験の終わりまで続き、loggerのCloseは置き換えたhandleを閉じます。元のactive fileの
// handleは後片付けで閉じます。
func failSelfNotificationFileWrites(t *testing.T, logger *selfNotificationLogger) {
	t.Helper()
	ctx := context.Background()
	var mu *sync.Mutex
	var file **os.File
	if logger.deliveryFileLoggerSet != nil {
		fileLogger, err := logger.deliveryFileLoggerSet.NewFileLogger(ctx)
		require.NoError(t, err)
		mu, file = &fileLogger.Fsstore.Mu, &fileLogger.Fsstore.File
	} else {
		require.NotNil(t, logger.fileLoggerSet)
		fileLogger, err := logger.fileLoggerSet.NewFileLogger(ctx)
		require.NoError(t, err)
		mu, file = &fileLogger.Fsstore.Mu, &fileLogger.Fsstore.File
	}
	readOnlyPath := filepath.Join(t.TempDir(), "read-only.log")
	require.NoError(t, os.WriteFile(readOnlyPath, nil, 0o600))
	readOnly, err := os.Open(readOnlyPath)
	require.NoError(t, err)
	mu.Lock()
	active := *file
	*file = readOnly
	mu.Unlock()
	t.Cleanup(func() { _ = active.Close() })
}

// selfNotificationLinesは、textの空でない行を返します。
func selfNotificationLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// selfNotificationFileLinesは、rootの配下の全てのfileの空でない行を返します。
func selfNotificationFileLines(t *testing.T, root string) []string {
	t.Helper()
	var lines []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines = append(lines, selfNotificationLines(string(content))...)
		return nil
	}))
	return lines
}

// countSelfNotificationLinesは、全てのsubstrを含む行の数を返します。
func countSelfNotificationLines(lines []string, substrs ...string) int {
	count := 0
	for _, line := range lines {
		matched := true
		for _, substr := range substrs {
			if !strings.Contains(line, substr) {
				matched = false
				break
			}
		}
		if matched {
			count++
		}
	}
	return count
}

// selfNotificationStdStreamsは、試験の間だけ標準出力と標準エラーを一時fileへ置き換えます。
// failStdoutなら標準出力を閉じたfileにして、標準出力への書き込みを失敗させます。
type selfNotificationStdStreams struct {
	stdoutPath string
	stderrPath string
}

func replaceSelfNotificationStdStreams(t *testing.T, failStdout bool) *selfNotificationStdStreams {
	t.Helper()
	return replaceSelfNotificationStdStreamsFailing(t, failStdout, false)
}

// replaceSelfNotificationStdStreamsFailingは、failStderrなら標準エラーも閉じたfileにします。
func replaceSelfNotificationStdStreamsFailing(t *testing.T, failStdout, failStderr bool) *selfNotificationStdStreams {
	t.Helper()
	dir := t.TempDir()
	streams := &selfNotificationStdStreams{
		stdoutPath: filepath.Join(dir, "stdout"),
		stderrPath: filepath.Join(dir, "stderr"),
	}
	var stdout *os.File
	if failStdout {
		stdout = selfNotificationClosedFile(t)
		streams.stdoutPath = stdout.Name()
	} else {
		var err error
		stdout, err = os.Create(streams.stdoutPath)
		require.NoError(t, err)
	}
	var stderr *os.File
	if failStderr {
		stderr = selfNotificationClosedFile(t)
		streams.stderrPath = stderr.Name()
	} else {
		var err error
		stderr, err = os.Create(streams.stderrPath)
		require.NoError(t, err)
	}
	previousStdout, previousStderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdout, stderr
	t.Cleanup(func() {
		os.Stdout, os.Stderr = previousStdout, previousStderr
		_ = stdout.Close()
		_ = stderr.Close()
	})
	return streams
}

func (s *selfNotificationStdStreams) read(t *testing.T, path string) []string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return selfNotificationLines(string(content))
}

func (s *selfNotificationStdStreams) stdoutLines(t *testing.T) []string {
	t.Helper()
	return s.read(t, s.stdoutPath)
}

func (s *selfNotificationStdStreams) stderrLines(t *testing.T) []string {
	t.Helper()
	return s.read(t, s.stderrPath)
}

// requireSelfNotificationResultsは、同期API・Delivery APIの各記録の結果を確認します。wantErrが
// falseなら成功、targetがnilでなければerrors.Isでtargetを含むerror、それ以外は何らかのerrorです。
// 非同期APIは結果を返さないため確認しません。
func requireSelfNotificationResults(
	t *testing.T,
	logger *selfNotificationLogger,
	results []error,
	wantErr bool,
	target error,
) {
	t.Helper()
	if logger.contract == selfNotificationAsync {
		return
	}
	for index, err := range results {
		switch {
		case !wantErr:
			require.NoError(t, err, "write %d", index)
		case target != nil:
			require.ErrorIs(t, err, target, "write %d", index)
		default:
			require.Error(t, err, "write %d", index)
		}
	}
}

const (
	// selfNotificationInternalMarkerは、PlainTextFormatterが内部ログの行の先頭に付ける印です。
	selfNotificationInternalMarker = "[INTERNAL]"
	// selfNotificationFileDelayMessageは、file書き込み遅延のerrorの内部ログの一部です。
	selfNotificationFileDelayMessage = "delayed error"
	// selfNotificationFileOverloadMessageは、過負荷で書き込みを止めた内部ログです。
	selfNotificationFileOverloadMessage = "file write skipped by overload policy"
	// selfNotificationFileFallbackFailureは、file通知先の書き込みの失敗を標準loggerへ残す行の
	// 先頭です（filelogger.ErrorMsgWriteLog）。
	selfNotificationFileFallbackFailure = "failed to write log to file; err "
	// selfNotificationFileOverloadDropCounterは、過負荷のdropを数えるcounterです。
	selfNotificationFileOverloadDropCounter = "file_overload_drop_total"
)

// selfNotificationStdoutFallbackFailureは、標準出力の通知先の書き込みの失敗を標準loggerへ残す行の
// 先頭です（stdout.MsgFailedWriteNotification）。
var selfNotificationStdoutFallbackFailure = strings.TrimSuffix(stdoutlogger.MsgFailedWriteNotification, "%s")

// selfNotificationFileCaseは、fileの内部ログを通知させる条件と、その結果の確認です。
type selfNotificationFileCase struct {
	name     string
	contract selfNotificationContract
	// prepareは、sync・asyncのfile設定へ構築前の条件を設定します。
	prepare func(cfg *pkglogger.FileLoggerConfig)
	// failWritesなら、構築後に既定roleのactive fileの書き込みを失敗させます。
	failWrites bool
	// notificationsPerWriteは、元の記録一件あたりの内部ログの件数です。
	notificationsPerWrite int
	// verifyはClose後の出力を確認します。
	verify func(t *testing.T, check selfNotificationFileCheck)
}

// selfNotificationFileCheckは、Close後の確認に使う値です。
type selfNotificationFileCheck struct {
	env      *selfNotificationEnv
	logger   *selfNotificationLogger
	results  []error
	fileRoot string
	streams  *selfNotificationStdStreams
	// notificationsは、全ての元の記録で発生するはずの内部ログの件数です。
	notifications int
}

// requireSelfNotificationDelayedFileLinesは、元の記録ごとに遅延の内部ログが一行だけ同じfileへ
// 書き込まれたことを確認します。通知先の書き込みは遅延を通知しないため、内部ログの内部ログは
// ありません。
func requireSelfNotificationDelayedFileLines(t *testing.T, check selfNotificationFileCheck) {
	t.Helper()
	requireSelfNotificationResults(t, check.logger, check.results, false, nil)
	lines := selfNotificationFileLines(t, check.fileRoot)
	require.Equal(t, selfNotificationEvents, countSelfNotificationLines(lines, selfNotificationApplicationMessage), "%q", lines)
	require.Equal(t, check.notifications, countSelfNotificationLines(lines, selfNotificationInternalMarker), "%q", lines)
	require.Equal(t, check.notifications, countSelfNotificationLines(lines, selfNotificationInternalMarker, selfNotificationFileDelayMessage), "%q", lines)
}

// requireSelfNotificationFailedFileWritesは、壊れたhandleへの通知先の書き込みも失敗し、
// 内部ログごとに一行だけ標準loggerへ残ったことを確認します。
func requireSelfNotificationFailedFileWrites(t *testing.T, check selfNotificationFileCheck) {
	t.Helper()
	requireSelfNotificationResults(t, check.logger, check.results, true, nil)
	require.Equal(t, check.notifications, check.env.standardLog.count(selfNotificationFileFallbackFailure), "%s", check.env.standardLog)
	require.Empty(t, selfNotificationFileLines(t, check.fileRoot), "no line may reach the file through the broken handle")
}

// TestSelfSelectedFileNotificationsAreWrittenWithoutRenotifyingは、fileの内部ログの通知先に
// file自身を選んだ本番の配線で、書き込み遅延・書き込み失敗・過負荷が続いても、元の記録ごとの
// 内部ログが決まった件数だけ同じfile（またはpolicyの処置）へ届き、通知が繰り返されないことを
// 確認します。file通知先はLoggerSet・DeliveryLoggerSetのfallback handlerで、内部ログを通知しない
// writeFallbackEntryで書き込むため、ReentryGuardは使われません。
func TestSelfSelectedFileNotificationsAreWrittenWithoutRenotifying(t *testing.T) {
	for _, test := range []selfNotificationFileCase{
		{
			name:                  "sync/write_delay",
			contract:              selfNotificationSync,
			prepare:               reportEverySelfNotificationFileWriteAsDelayed,
			notificationsPerWrite: 1,
			verify:                requireSelfNotificationDelayedFileLines,
		},
		{
			name:       "sync/write_failure",
			contract:   selfNotificationSync,
			failWrites: true,
			// MaxWriteRetriesは0です。attemptの失敗と書き込みの失敗の二件を通知します。
			notificationsPerWrite: 2,
			verify:                requireSelfNotificationFailedFileWrites,
		},
		{
			name:     "sync/overload_drop",
			contract: selfNotificationSync,
			prepare: func(cfg *pkglogger.FileLoggerConfig) {
				overloadEverySelfNotificationFileWrite(cfg, pkgfsstore.OverloadPolicyDrop)
			},
			notificationsPerWrite: 1,
			verify: func(t *testing.T, check selfNotificationFileCheck) {
				requireSelfNotificationResults(t, check.logger, check.results, true, pkglogger.ErrLogDropped)
				// 元の記録と、その過負荷の内部ログ（通知先のfileでも過負荷の処置を受けます）が
				// 一件ずつdropされます。
				require.EqualValues(t, selfNotificationEvents+check.notifications, check.env.meter.total(selfNotificationFileOverloadDropCounter))
				require.Empty(t, selfNotificationFileLines(t, check.fileRoot))
			},
		},
		{
			name:     "sync/overload_degrade",
			contract: selfNotificationSync,
			prepare: func(cfg *pkglogger.FileLoggerConfig) {
				overloadEverySelfNotificationFileWrite(cfg, pkgfsstore.OverloadPolicyDegrade)
			},
			notificationsPerWrite: 1,
			verify: func(t *testing.T, check selfNotificationFileCheck) {
				requireSelfNotificationResults(t, check.logger, check.results, false, nil)
				// 元の記録と、その過負荷の内部ログ（通知先のfileでも退避されます）が一行ずつ
				// 標準出力へ退避されます。
				lines := check.streams.stdoutLines(t)
				require.Equal(t, selfNotificationEvents, countSelfNotificationLines(lines, selfNotificationApplicationMessage), "%q", lines)
				require.Equal(t, check.notifications, countSelfNotificationLines(lines, selfNotificationInternalMarker, selfNotificationFileOverloadMessage), "%q", lines)
				require.Len(t, lines, selfNotificationEvents+check.notifications, "%q", lines)
				require.Empty(t, selfNotificationFileLines(t, check.fileRoot))
			},
		},
		{
			name:                  "async/write_delay",
			contract:              selfNotificationAsync,
			prepare:               reportEverySelfNotificationFileWriteAsDelayed,
			notificationsPerWrite: 1,
			verify:                requireSelfNotificationDelayedFileLines,
		},
		{
			name:                  "async/write_failure",
			contract:              selfNotificationAsync,
			failWrites:            true,
			notificationsPerWrite: 2,
			verify:                requireSelfNotificationFailedFileWrites,
		},
		{
			name:       "delivery/write_failure",
			contract:   selfNotificationDelivery,
			failWrites: true,
			// Delivery fileは書き込みの失敗だけを一件通知します。
			notificationsPerWrite: 1,
			verify:                requireSelfNotificationFailedFileWrites,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
				streams := replaceSelfNotificationStdStreams(t, false)
				self := selfNotificationHandlers(pkglogger.LogOutputFile)
				var cfg pkglogger.LoggerConfig
				var deliveryCfg pkglogger.DeliveryLoggerConfig
				var root string
				if test.contract == selfNotificationDelivery {
					fileCfg := newSelfNotificationDeliveryFileConfig(t, "self-file")
					fileCfg.HandlerConfig = self
					root = fileCfg.FsstoreConfig.Layout.LogPath
					deliveryCfg = pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
						File:                  fileCfg,
					}
				} else {
					fileCfg := newSelfNotificationFileConfig(t, "self-file")
					fileCfg.HandlerConfig = self
					if test.prepare != nil {
						test.prepare(fileCfg)
					}
					root = fileCfg.FsstoreConfig.Layout.LogPath
					cfg = pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
						File:                  fileCfg,
					}
				}
				logger := newSelfNotificationLogger(t, env, test.contract, cfg, deliveryCfg)
				if test.failWrites {
					failSelfNotificationFileWrites(t, logger)
				}
				results := logger.writeEvents(t)
				// 非同期APIのCloseは、受理した記録とその通知の処理が終わるまで待ちます。
				logger.closeLogger(t)
				notifications := selfNotificationEvents * test.notificationsPerWrite
				test.verify(t, selfNotificationFileCheck{
					env:           env,
					logger:        logger,
					results:       results,
					fileRoot:      root,
					streams:       streams,
					notifications: notifications,
				})
			})
		})
	}
}

// newSelfNotificationStdoutConfigsは、sync・async用とDelivery用の標準出力の設定を返します。
func newSelfNotificationStdoutConfigs(
	handlerCfg pkgloggerhandler.HandlerConfig,
) (*pkglogger.StdoutLoggerConfig, *pkglogger.DeliveryStdoutLoggerConfig) {
	stdoutCfg := newActivationStdoutConfig()
	stdoutCfg.HandlerConfig = handlerCfg
	deliveryStdoutCfg := newActivationDeliveryStdoutConfig()
	deliveryStdoutCfg.HandlerConfig = handlerCfg
	return stdoutCfg, deliveryStdoutCfg
}

var selfNotificationContracts = []selfNotificationContract{
	selfNotificationSync,
	selfNotificationAsync,
	selfNotificationDelivery,
}

// TestSelfSelectedStdoutNotificationsAreWrittenWithoutRenotifyingは、標準出力の内部ログの
// 通知先に標準出力自身を選んだ本番の配線で、標準出力への書き込みが失敗し続けても、元の記録
// ごとの書き込み失敗の内部ログが通知先へ一度だけ渡り、通知が繰り返されないことを確認します。
// 標準出力の通知先は通知元とは別のfallback instanceで、通知先を持たないため、その書き込みの
// 失敗は再び通知されず、失敗ごとに一行だけ標準loggerへ残ります。
func TestSelfSelectedStdoutNotificationsAreWrittenWithoutRenotifying(t *testing.T) {
	for _, contract := range selfNotificationContracts {
		for _, failStderr := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail_stderr=%t", contract, failStderr), func(t *testing.T) {
				runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
					// 標準出力（INFO）への書き込みを失敗させます。failStderrなら、内部ログ（ERROR）を
					// 書き込む標準エラーも失敗させます。
					streams := replaceSelfNotificationStdStreamsFailing(t, true, failStderr)
					stdoutCfg, deliveryStdoutCfg := newSelfNotificationStdoutConfigs(
						selfNotificationHandlers(pkglogger.LogOutputStdout),
					)
					outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
					logger := newSelfNotificationLogger(t, env, contract,
						pkglogger.LoggerConfig{LogOutputDestinations: outputs, Stdout: stdoutCfg},
						pkglogger.DeliveryLoggerConfig{LogOutputDestinations: outputs, Stdout: deliveryStdoutCfg},
					)
					results := logger.writeEvents(t)
					logger.closeLogger(t)

					requireSelfNotificationResults(t, logger, results, true, nil)
					// 標準出力（sync・async）の書き込み失敗の内部ログはERRORなので、通知先は標準エラーへ
					// 書き込みます。Delivery標準出力の内部ログは元の記録のlevel（INFO）を保つため、通知先も
					// 失敗し続ける標準出力へ書き込みます。通知先自身の失敗は通知されず、失敗ごとに一行だけ
					// 標準loggerへ残るため、Deliveryでも通知が一度ずつ行われたことを数えられます。
					notificationsWritten, notificationFailures := selfNotificationEvents, 0
					if contract == selfNotificationDelivery || failStderr {
						notificationsWritten, notificationFailures = 0, selfNotificationEvents
					}
					if !failStderr {
						lines := streams.stderrLines(t)
						require.Equal(t, notificationsWritten, countSelfNotificationLines(lines, selfNotificationInternalMarker), "%q", lines)
						require.Len(t, lines, notificationsWritten, "%q", lines)
					}
					require.Equal(t, notificationFailures, env.standardLog.count(selfNotificationStdoutFallbackFailure), "%s", env.standardLog)
					require.Len(t, selfNotificationLines(env.standardLog.String()), notificationFailures, "%s", env.standardLog)
				})
			})
		}
	}
}

// TestCrossSelectedFileAndStdoutNotificationsDoNotRepeatは、fileと標準出力が互いを内部ログの
// 通知先に選んだ本番の配線で、通知元の書き込みが失敗し続けても、元の記録ごとの内部ログが
// 決まった件数だけ相手の出力へ届き、通知が繰り返されないことを確認します。
func TestCrossSelectedFileAndStdoutNotificationsDoNotRepeat(t *testing.T) {
	t.Run("file_to_stdout", func(t *testing.T) {
		for _, contract := range selfNotificationContracts {
			t.Run(string(contract), func(t *testing.T) {
				runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
					streams := replaceSelfNotificationStdStreams(t, false)
					toStdout := selfNotificationHandlers(pkglogger.LogOutputStdout)
					stdoutCfg, deliveryStdoutCfg := newSelfNotificationStdoutConfigs(pkgloggerhandler.HandlerConfig{})
					fileCfg := newSelfNotificationFileConfig(t, "cross-file-source")
					fileCfg.HandlerConfig = toStdout
					deliveryFileCfg := newSelfNotificationDeliveryFileConfig(t, "cross-file-source")
					deliveryFileCfg.HandlerConfig = toStdout
					outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
					logger := newSelfNotificationLogger(t, env, contract,
						pkglogger.LoggerConfig{LogOutputDestinations: outputs, File: fileCfg, Stdout: stdoutCfg},
						pkglogger.DeliveryLoggerConfig{LogOutputDestinations: outputs, File: deliveryFileCfg, Stdout: deliveryStdoutCfg},
					)
					failSelfNotificationFileWrites(t, logger)
					results := logger.writeEvents(t)
					logger.closeLogger(t)
					// FileLoggerはattemptの失敗と書き込みの失敗の二件、Delivery fileは書き込みの失敗
					// だけを一件通知します。
					notifications := 2 * selfNotificationEvents
					if contract == selfNotificationDelivery {
						notifications = selfNotificationEvents
					}

					requireSelfNotificationResults(t, logger, results, true, nil)
					lines := streams.stderrLines(t)
					require.Equal(t, notifications, countSelfNotificationLines(lines, selfNotificationInternalMarker), "%q", lines)
					require.Len(t, lines, notifications, "%q", lines)
					require.Empty(t, streams.stdoutLines(t))
				})
			})
		}
	})
	t.Run("stdout_to_file", func(t *testing.T) {
		for _, contract := range selfNotificationContracts {
			t.Run(string(contract), func(t *testing.T) {
				runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
					streams := replaceSelfNotificationStdStreams(t, true)
					toFile := selfNotificationHandlers(pkglogger.LogOutputFile)
					stdoutCfg, deliveryStdoutCfg := newSelfNotificationStdoutConfigs(toFile)
					fileCfg := newSelfNotificationFileConfig(t, "cross-file-target")
					deliveryFileCfg := newSelfNotificationDeliveryFileConfig(t, "cross-file-target")
					root := fileCfg.FsstoreConfig.Layout.LogPath
					if contract == selfNotificationDelivery {
						root = deliveryFileCfg.FsstoreConfig.Layout.LogPath
					}
					outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
					logger := newSelfNotificationLogger(t, env, contract,
						pkglogger.LoggerConfig{LogOutputDestinations: outputs, Stdout: stdoutCfg, File: fileCfg},
						pkglogger.DeliveryLoggerConfig{LogOutputDestinations: outputs, Stdout: deliveryStdoutCfg, File: deliveryFileCfg},
					)
					results := logger.writeEvents(t)
					logger.closeLogger(t)

					requireSelfNotificationResults(t, logger, results, true, nil)
					// 標準出力の書き込み失敗の内部ログが、元の記録ごとに一行だけfileへ書き込まれます。
					lines := selfNotificationFileLines(t, root)
					require.Equal(t, selfNotificationEvents, countSelfNotificationLines(lines, selfNotificationInternalMarker), "%q", lines)
					require.Len(t, lines, selfNotificationEvents, "%q", lines)
					require.Empty(t, streams.stderrLines(t))
				})
			})
		}
	})
}

// TestSelfSelectedFileNotificationsReachTheInternalLogDestinationは、fileの内部ログの通知先に
// fileを選び、内部ログ専用のfile保存先（InternalLogDestinations.File）を指定した本番の配線で、
// 書き込み遅延が続いても、元の記録ごとに遅延の内部ログが一行だけ専用の保存先へ届き、通常の
// 保存先へは内部ログが書き込まれず、通知が繰り返されないことを確認します。内部ログの保存先も
// getHandlerが作るfile通知先（LoggerSetのfallback handler）を使います。
func TestSelfSelectedFileNotificationsReachTheInternalLogDestination(t *testing.T) {
	for _, contract := range []selfNotificationContract{selfNotificationSync, selfNotificationAsync} {
		t.Run(string(contract), func(t *testing.T) {
			runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
				fileCfg := newSelfNotificationFileConfig(t, "self-file-with-internal-destination")
				reportEverySelfNotificationFileWriteAsDelayed(fileCfg)
				fileCfg.HandlerConfig = selfNotificationHandlers(pkglogger.LogOutputFile)
				internalCfg := newSelfNotificationFileConfig(t, "self-file-internal-destination")
				outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				logger := newSelfNotificationLogger(t, env, contract, pkglogger.LoggerConfig{
					LogOutputDestinations:   outputs,
					File:                    fileCfg,
					InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{File: internalCfg},
				}, pkglogger.DeliveryLoggerConfig{})
				results := logger.writeEvents(t)
				logger.closeLogger(t)

				requireSelfNotificationResults(t, logger, results, false, nil)
				lines := selfNotificationFileLines(t, fileCfg.FsstoreConfig.Layout.LogPath)
				require.Equal(t, selfNotificationEvents, countSelfNotificationLines(lines, selfNotificationApplicationMessage), "%q", lines)
				require.Zero(t, countSelfNotificationLines(lines, selfNotificationInternalMarker), "%q", lines)
				internalLines := selfNotificationFileLines(t, internalCfg.FsstoreConfig.Layout.LogPath)
				require.Equal(t, selfNotificationEvents, countSelfNotificationLines(internalLines, selfNotificationInternalMarker, selfNotificationFileDelayMessage), "%q", internalLines)
				require.Len(t, internalLines, selfNotificationEvents, "%q", internalLines)
			})
		})
	}
}

// TestConcurrentSelfSelectedFileNotificationsAreAllDeliveredは、本番の配線でfile自身（または
// fileと標準出力）を内部ログの通知先に選び、書き込み遅延を続けたまま多数のgoroutineから同時に
// 記録しても、全ての内部ログが選んだ全ての通知先へ届き、ReentryGuardが再入を一度も検出しない
// ことを確認します。再入は通知の数では判定しないため、同じloggerで同時に実行中の通知が
// いくつあっても、互いに独立した書き込みの内部ログは全て通知されます。
//
// 同時に実行中になる通知の数はschedulingで変わるため、この試験は「実行中の通知の数で再入と
// 判定する上限」を再び加える変更を確定的には検出しません（GOMAXPROCS=1では"file"の場合を
// 見逃し得ます）。その確定的な検出は、32件の通知をchannelで同時に保持して判定する
// handler.TestReentryGuardBookkeepingSurvivesPanicsAndConcurrencyが担当し、この試験は本番の
// 配線で通知が欠けないことを補助的に確認します。
func TestConcurrentSelfSelectedFileNotificationsAreAllDelivered(t *testing.T) {
	const (
		writers          = 32
		recordsPerWriter = 50
		records          = writers * recordsPerWriter
	)
	for _, test := range []struct {
		name    string
		targets []pkglogger.LogOutputDestination
	}{
		{name: "file", targets: []pkglogger.LogOutputDestination{pkglogger.LogOutputFile}},
		{name: "file_and_stdout", targets: []pkglogger.LogOutputDestination{pkglogger.LogOutputFile, pkglogger.LogOutputStdout}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
				streams := replaceSelfNotificationStdStreams(t, false)
				fileCfg := newSelfNotificationFileConfig(t, "concurrent-self-file")
				reportEverySelfNotificationFileWriteAsDelayed(fileCfg)
				fileCfg.HandlerConfig = selfNotificationHandlers(test.targets...)
				stdoutCfg, _ := newSelfNotificationStdoutConfigs(pkgloggerhandler.HandlerConfig{})
				logger := newSelfNotificationLogger(t, env, selfNotificationSync, pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
					File:                  fileCfg,
					Stdout:                stdoutCfg,
				}, pkglogger.DeliveryLoggerConfig{})

				// 書き込みはfileへのI/Oを伴い、混雑した環境では全体が10秒を超えることがあるため、
				// 全体の所要時間ではなく、書き込みが進まなくなったことを停止として扱います。
				var completed atomic.Int64
				err := completeSelfNotificationStepWhileProgressing(t, "concurrent writes", &completed, func() error {
					// 全てのgoroutineを同時に始め、同じloggerの通知が多数同時に実行中になるようにします。
					start := make(chan struct{})
					errs := make(chan error, records)
					var wg sync.WaitGroup
					for writer := range writers {
						wg.Add(1)
						go func() {
							defer wg.Done()
							<-start
							for record := range recordsPerWriter {
								if err := logger.write(writer*recordsPerWriter + record); err != nil {
									errs <- err
								}
								completed.Add(1)
							}
						}()
					}
					close(start)
					wg.Wait()
					close(errs)
					var all []error
					for err := range errs {
						all = append(all, err)
					}
					return errors.Join(all...)
				})
				require.NoError(t, err)
				logger.closeLogger(t)

				// fileの通知先は、元の記録ごとの遅延の内部ログを一行ずつ同じfileへ書き込みます。
				lines := selfNotificationFileLines(t, fileCfg.FsstoreConfig.Layout.LogPath)
				require.Equal(t, records, countSelfNotificationLines(lines, selfNotificationApplicationMessage))
				require.Equal(t, records, countSelfNotificationLines(lines, selfNotificationInternalMarker, selfNotificationFileDelayMessage))
				require.Len(t, lines, 2*records)
				// 標準出力の通知先は、ERRORの内部ログを全て標準エラーへ書き込みます。
				wantStderr := 0
				for _, target := range test.targets {
					if target == pkglogger.LogOutputStdout {
						wantStderr = records
					}
				}
				stderr := streams.stderrLines(t)
				require.Equal(t, wantStderr, countSelfNotificationLines(stderr, selfNotificationInternalMarker, selfNotificationFileDelayMessage))
				require.Len(t, stderr, wantStderr)
			})
		})
	}
}
