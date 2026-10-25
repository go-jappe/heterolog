package stdout_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	appotel "github.com/go-jappe/heterolog/internal/application/otel"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type stdoutMultiLoggerTestConfig struct{}

func (stdoutMultiLoggerTestConfig) Timeout() time.Duration {
	return time.Second
}

func (stdoutMultiLoggerTestConfig) MaxLogEntryBytes() int {
	return pkglogger.DefaultMaxLogEntryBytes
}

func newStdoutTestMultiLogger(
	t *testing.T,
	loggers ...infralogging.BackendLogEntryHandler,
) *multi.MultiLogger {
	t.Helper()
	logger, err := multi.NewMultiLogger(stdoutMultiLoggerTestConfig{}, loggers...)
	require.NoError(t, err)
	return logger
}

// newStdoutTestDeliveryMultiLoggerは、Delivery stdoutのchildを本番と同じ論理出力名（stdout）で
// 登録したDelivery multi loggerを作ります。
func newStdoutTestDeliveryMultiLogger(
	t *testing.T,
	loggers ...infralogging.DeliveryBackendLogEntryHandler,
) *multi.DeliveryMultiLogger {
	t.Helper()
	targets := make([]multi.DeliveryTarget, 0, len(loggers))
	for _, child := range loggers {
		targets = append(targets, multi.DeliveryTarget{Backend: stdout.BackendServices, Logger: child})
	}
	logger, err := multi.NewDeliveryMultiLoggerWithTargets(stdoutMultiLoggerTestConfig{}, targets...)
	require.NoError(t, err)
	return logger
}

// stdoutLogTestOccurredAtは出力行全体をliteralで比較するため、application logの
// 発生時刻を固定します。testStdoutConfigのUTC・RFC3339Nanoで表示されます。
var stdoutLogTestOccurredAt = time.Date(2026, time.August, 20, 3, 4, 5, 123456789, time.UTC)

const stdoutLogTestTimestamp = "2026-08-20T03:04:05.123456789Z"

// 標準出力をキャプチャするヘルパー関数
func captureOutput(f func()) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	outC := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r)
		outC <- buf.String()
	}()

	f()
	_ = w.Close()
	return <-outC, nil
}

// 標準エラーをキャプチャするヘルパー関数
func captureErrorOutput(f func()) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old }()

	errC := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r)
		errC <- buf.String()
	}()

	f()
	_ = w.Close()
	return <-errC, nil
}

// captureStdoutAndStderrはfの実行中に標準出力と標準エラーを同時に置き換え、
// 出力先streamの選択も検証できるよう両方の内容を返します。
func captureStdoutAndStderr(t *testing.T, f func()) (string, string) {
	t.Helper()
	var stderrOutput string
	var stderrErr error
	stdoutOutput, stdoutErr := captureOutput(func() {
		stderrOutput, stderrErr = captureErrorOutput(f)
	})
	require.NoError(t, stdoutErr)
	require.NoError(t, stderrErr)
	return stdoutOutput, stderrOutput
}

// callerFrameは呼び出し元の位置を、production codeと同じruntime.CallersFramesで返します。
func callerFrame() runtime.Frame {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	frame, _ := runtime.CallersFrames(pcs[:]).Next()
	return frame
}

func TestStdoutLogger_Log(t *testing.T) {
	// 開発者の.envやprocess環境を読まず、出力形式とlevelを明示した設定を使います。
	cfg := testStdoutConfig()

	memExp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(memExp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	// global TracerProviderは変更せず、構築したproviderのtracerを直接渡します。
	tracer := appotel.NewOtelTracer(tp.Tracer("internal/infrastructure/logger/stdout/test"))
	ctx, testSpan := tracer.StartSpan(context.Background(), "testStdoutSpan")
	defer testSpan.End()
	parent := trace.SpanContextFromContext(ctx)
	require.True(t, parent.IsValid())

	logger, err := stdout.NewStdoutLogger(ctx, stdout.NewConfigAdapter(cfg), nil, tracer, observer.NopMeter{})
	require.NoError(t, err)
	multiLogger := newStdoutTestMultiLogger(t, logger)
	logCtx := applogging.WithSlogRecordTime(ctx, stdoutLogTestOccurredAt)
	ids := fmt.Sprintf("trace_id: %s - span_id: %s", parent.TraceID(), parent.SpanID())

	// WARN未満はstdoutだけへ出力します。
	var callErr error
	stdoutOutput, stderrOutput := captureStdoutAndStderr(t, func() {
		callErr = multiLogger.Info(logCtx, "Test message")
	})
	require.NoError(t, callErr)
	require.Equal(t, "[INFO] "+stdoutLogTestTimestamp+" - Test message - "+ids+"\n", stdoutOutput)
	require.Empty(t, stderrOutput)

	// WARN以上はstderrだけへ出力します。
	stdoutOutput, stderrOutput = captureStdoutAndStderr(t, func() {
		callErr = multiLogger.Error(logCtx, "Error message")
	})
	require.NoError(t, callErr)
	require.Empty(t, stdoutOutput)
	require.Equal(t, "[ERROR] "+stdoutLogTestTimestamp+" - Error message - "+ids+"\n", stderrOutput)

	// constructorのspanは、構築時contextのspanの子として終了済みで記録されます。
	require.NoError(t, tp.ForceFlush(context.Background()))
	var createSpans []tracetest.SpanStub
	for _, span := range memExp.GetSpans() {
		if span.Name == stdout.SpanStdoutLoggerCreate {
			createSpans = append(createSpans, span)
		}
	}
	require.Len(t, createSpans, 1, "NewStdoutLogger must record one create span")
	require.Equal(t, parent.TraceID(), createSpans[0].Parent.TraceID())
	require.Equal(t, parent.SpanID(), createSpans[0].Parent.SpanID())
	require.False(t, createSpans[0].EndTime.IsZero(), "create span must be ended")
}

func TestStdoutLogger_ErrorWithStack(t *testing.T) {
	cfg := testStdoutConfig()
	// 呼び出し位置のframeだけを出力し、stack traceを含む行全体を比較します。
	cfg.LogBodyConfig.ErrorTraceDepth = 0
	cfg.LogBodyConfig.ErrorTraceDepthDebugMode = 0
	ctx := applogging.WithSlogRecordTime(context.Background(), stdoutLogTestOccurredAt)
	logger, err := stdout.NewStdoutLogger(ctx, stdout.NewConfigAdapter(cfg), nil, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	multiLogger := newStdoutTestMultiLogger(t, logger)

	var (
		callErr  error
		callSite runtime.Frame
	)
	stdoutOutput, stderrOutput := captureStdoutAndStderr(t, func() {
		callSite = callerFrame()
		callErr = multiLogger.ErrorWithStack(ctx, "Error occurred", errors.New("test error")) // callSiteの次の行
	})
	require.NoError(t, callErr)
	require.Empty(t, stdoutOutput)
	want := fmt.Sprintf(
		"[ERROR] %s - Error occurred - err: test error (stacktrace): (1) at: (File: %s | Line: %d | FuncName: %s)\n",
		stdoutLogTestTimestamp,
		callSite.File,
		callSite.Line+1,
		callSite.Function,
	)
	require.Equal(t, want, stderrOutput)
}

// errorを渡さないErrorWithStackも、要求されたstackを持つERRORとしてstderrへ記録します。
// errorの欄だけを出力せず、呼び出し元にもerrorを返しません。
func TestStdoutLogger_ErrorWithStackWithoutError(t *testing.T) {
	cfg := testStdoutConfig()
	cfg.LogBodyConfig.ErrorTraceDepth = 0
	cfg.LogBodyConfig.ErrorTraceDepthDebugMode = 0
	ctx := applogging.WithSlogRecordTime(context.Background(), stdoutLogTestOccurredAt)
	logger, err := stdout.NewStdoutLogger(ctx, stdout.NewConfigAdapter(cfg), nil, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	multiLogger := newStdoutTestMultiLogger(t, logger)

	var (
		callErr  error
		callSite runtime.Frame
	)
	stdoutOutput, stderrOutput := captureStdoutAndStderr(t, func() {
		callSite = callerFrame()
		callErr = multiLogger.ErrorWithStack(ctx, "Error occurred", nil) // callSiteの次の行
	})
	require.NoError(t, callErr)
	require.Empty(t, stdoutOutput)
	want := fmt.Sprintf(
		"[ERROR] %s - Error occurred (stacktrace): (1) at: (File: %s | Line: %d | FuncName: %s)\n",
		stdoutLogTestTimestamp,
		callSite.File,
		callSite.Line+1,
		callSite.Function,
	)
	require.Equal(t, want, stderrOutput)
}
