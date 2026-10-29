package logrecovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/config"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	infralogrecovery "github.com/go-jappe/heterolog/internal/infrastructure/logrecovery"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// このfileの試験は、CLIが利用者へ出力するhelp、report（JSON・本文）、標準error出力の
// 文言を出力契約として確認します。errorの種類は、Recoverが返したerror値を受け取れる箇所で
// errors.Is・errors.Asにより判定し、文言の照合は出力された内容の確認に限ります。

func validArgs(extra ...string) []string {
	return append([]string{"--target", "service", "--application-stopped"}, extra...)
}

// consistentConfigは、CLIが復旧の前に行うconfig.ValidateApplicationConfigを通る最小の設定です。
// 空の環境設定から読み込む場合と同じmemory healthの既定値だけを持ち、保存先を使う出力先は
// ありません。Recoverを差し替える試験の読み込み結果に使います。
func consistentConfig() config.AppConfig {
	var cfg config.AppConfig
	cfg.RuntimeConfig.Health.Memory = configruntime.MemoryHealthConfig{
		CriticalRatio:           configruntime.DefaultMemoryCriticalRatio,
		MinAvailableBytes:       configruntime.DefaultMinAvailableMemoryMegaBytes * config.BytesPerMegabyte,
		CriticalConsecutiveHits: configruntime.DefaultMemoryCriticalConsecutiveHits,
	}
	return cfg
}

func unexpectedDependencies(t *testing.T) Dependencies {
	t.Helper()
	return Dependencies{
		LoadConfig: func(string) (config.AppConfig, error) {
			t.Fatal("configuration must not be loaded")
			return config.AppConfig{}, nil
		},
		Recover: func(context.Context, config.AppConfig, infralogrecovery.Options) (infralogrecovery.Report, error) {
			t.Fatal("recovery must not run")
			return infralogrecovery.Report{}, nil
		},
	}
}

func TestHelpHasNoConfigurationOrRecoveryEffects(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"-h"}} {
		var stdout, stderr bytes.Buffer
		if exit := Run(context.Background(), args, &stdout, &stderr, unexpectedDependencies(t)); exit != 0 {
			t.Fatalf("Run(%v) = %d; stderr=%s", args, exit, &stderr)
		}
		for _, want := range []string{"Usage:", "--application-stopped", "does not automatically detect running processes", "does not purge archives or replay record spools"} {
			if !strings.Contains(stdout.String(), want) {
				t.Errorf("help does not include %q: %s", want, &stdout)
			}
		}
		if strings.ContainsFunc(stdout.String(), func(r rune) bool { return r > 127 }) {
			t.Errorf("help contains non-English output: %s", &stdout)
		}
		if stderr.Len() != 0 {
			t.Errorf("help wrote stderr: %s", &stderr)
		}
	}
}

func TestInvalidArgumentsHaveNoConfigurationOrRecoveryEffects(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing target", []string{"--application-stopped"}, "--target is required"},
		{"target contains path traversal", []string{"--target", "../other", "--application-stopped"}, `invalid --target "../other"`},
		{"target uses a Windows device name", []string{"--target", "NUL", "--application-stopped"}, `invalid --target "NUL"`},
		{"missing stop acknowledgement", []string{"--target", "service"}, "--application-stopped is required"},
		{"false stop acknowledgement", validArgs("--application-stopped=false"), "--application-stopped is required"},
		{"unknown logger", validArgs("--logger", "invalid"), `invalid --logger "invalid"`},
		{"unknown scope", validArgs("--scope", "spool"), `invalid --scope "spool"`},
		{"empty env file", validArgs("--env-file="), "--env-file must name a nonempty path"},
		{"whitespace env file", validArgs("--env-file", " \t"), "--env-file must name a nonempty path"},
		{"unexpected positional argument", validArgs("stray"), "unexpected positional arguments: stray"},
		{"unknown flag", validArgs("--purge"), "flag provided but not defined"},
		{"missing flag value", validArgs("--logger"), "flag needs an argument"},
		{"malformed boolean", validArgs("--json=invalid"), "invalid boolean value"},
		{"interval without retry", validArgs("--retry-interval", "2s"), "--retry-interval requires --retry"},
		{"zero retry interval", validArgs("--retry", "--retry-interval", "0"), "--retry-interval must be greater than zero"},
		{"negative retry interval", validArgs("--retry", "--retry-interval", "-1s"), "--retry-interval must be greater than zero"},
		{"malformed retry interval", validArgs("--retry", "--retry-interval", "tomorrow"), "invalid value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if exit := Run(context.Background(), tt.args, &stdout, &stderr, unexpectedDependencies(t)); exit != 2 {
				t.Fatalf("Run() = %d, want 2; stderr=%s", exit, &stderr)
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Errorf("stderr = %q, want %q", stderr.String(), tt.want)
			}
			if stdout.Len() != 0 {
				t.Errorf("invalid arguments wrote stdout: %s", &stdout)
			}
		})
	}
}

func TestRunForwardsConfigurationAndSelectedOptionsOnce(t *testing.T) {
	for _, logger := range []struct {
		input    string
		contract pkglogger.LoggerContractKind
	}{
		{"sync", pkglogger.LoggerContractSync},
		{"async", pkglogger.LoggerContractAsyncBestEffort},
		{"delivery", pkglogger.LoggerContractDelivery},
	} {
		for _, scope := range []struct {
			input string
			value infralogrecovery.RecoveryScope
		}{
			{"file-logger", infralogrecovery.RecoveryScopeFileLogger},
			{"log-backend-failure-archive", infralogrecovery.RecoveryScopeLogBackendFailureArchive},
			{"all", infralogrecovery.RecoveryScopeAll},
		} {
			t.Run(logger.input+"/"+scope.input, func(t *testing.T) {
				loadCalls, recoveryCalls := 0, 0
				ctx := context.WithValue(context.Background(), struct{}{}, "test context")
				deps := Dependencies{
					LoadConfig: func(envFile string) (config.AppConfig, error) {
						loadCalls++
						if envFile != "/not-read/recovery.env" {
							t.Errorf("envFile = %q", envFile)
						}
						cfg := consistentConfig()
						cfg.RuntimeConfig.Target = "environment-target"
						cfg.RuntimeConfig.IsDebug = true
						cfg.MaxLogEntryBytes = 1234
						return cfg, nil
					},
					Recover: func(gotCtx context.Context, cfg config.AppConfig, opts infralogrecovery.Options) (infralogrecovery.Report, error) {
						recoveryCalls++
						if gotCtx != ctx {
							t.Error("recovery did not receive the caller's context")
						}
						if cfg.RuntimeConfig.Target != "service" || !cfg.RuntimeConfig.IsDebug || cfg.MaxLogEntryBytes != 1234 {
							t.Errorf("configuration overrides did not preserve loaded fields: target=%q debug=%t maxBytes=%d", cfg.RuntimeConfig.Target, cfg.RuntimeConfig.IsDebug, cfg.MaxLogEntryBytes)
						}
						if opts.LoggerContract != logger.contract || opts.Scope != scope.value {
							t.Errorf("recovery options = %+v", opts)
						}
						return infralogrecovery.Report{}, nil
					},
				}
				var stdout, stderr bytes.Buffer
				if exit := Run(ctx, validArgs("--env-file", "/not-read/recovery.env", "--logger", logger.input, "--scope", scope.input), &stdout, &stderr, deps); exit != 0 {
					t.Fatalf("Run() = %d; stderr=%s", exit, &stderr)
				}
				if loadCalls != 1 || recoveryCalls != 1 {
					t.Errorf("calls: load=%d recovery=%d, want one each", loadCalls, recoveryCalls)
				}
			})
		}
	}
}

func TestDefaultOptions(t *testing.T) {
	opts, err := parseOptions(validArgs(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.recovery.LoggerContract != pkglogger.LoggerContractSync || opts.recovery.Scope != infralogrecovery.RecoveryScopeAll || opts.envFile != "" || opts.json || opts.recovery.Retry || opts.recovery.RetryInterval != infralogrecovery.DefaultRetryInterval {
		t.Fatalf("defaults = %+v", opts)
	}
}

func TestCustomApplicationIdentifier(t *testing.T) {
	const identifier = "payments-api.v2"
	opts, err := parseOptions([]string{"--target", identifier, "--application-stopped"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.target != identifier {
		t.Fatalf("target = %q, want %q", opts.target, identifier)
	}
}

func TestRetryModeForwardsControlAndKeepsJSONSeparateFromProgress(t *testing.T) {
	force := make(chan struct{})
	want := sampleReport()
	deps := reportDependencies(want, nil)
	deps.ForceStop = force
	deps.Recover = func(_ context.Context, _ config.AppConfig, opts infralogrecovery.Options) (infralogrecovery.Report, error) {
		if !opts.Retry || opts.RetryInterval != 2*time.Second || opts.ForceStop != force || opts.OnProgress == nil {
			t.Fatalf("retry options = %+v", opts)
		}
		opts.OnProgress(infralogrecovery.Progress{Phase: infralogrecovery.ProgressPreparing, Scope: "file-logger"})
		directory := want.Directories[0]
		directory.FailedFiles = []infralogrecovery.FileFailureReport{{Path: "pending.log.gz", Error: "destination unavailable"}}
		opts.OnProgress(infralogrecovery.Progress{
			Phase: infralogrecovery.ProgressRetryWaiting, Scope: "file-logger", Attempt: 2,
			Directory: directory, Err: errors.New("retry upload"), NextRetryAt: time.Now().Add(2 * time.Second),
		})
		opts.OnProgress(infralogrecovery.Progress{Phase: infralogrecovery.ProgressStopping})
		opts.OnProgress(infralogrecovery.Progress{Phase: infralogrecovery.ProgressForced})
		opts.OnProgress(infralogrecovery.Progress{Phase: infralogrecovery.ProgressComplete})
		return want, nil
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs("--retry", "--retry-interval", "2s", "--json"), &stdout, &stderr, deps); exit != 0 {
		t.Fatalf("Run() = %d; stderr=%s", exit, &stderr)
	}
	var got infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("stdout is not the final JSON report: got=%+v err=%v stdout=%s", got, err, &stdout)
	}
	for _, fragment := range []string{"[recovery]", "scope=file-logger", "attempt=2", "retry-in=", "next-retry-at=", "scanned=5", "failed=1", "Failed \"pending.log.gz\"", "destination unavailable", "Error: retry upload", "accepted recovery operations and checkpoint saves", "Forced shutdown requested", "Completed"} {
		if !strings.Contains(stderr.String(), fragment) {
			t.Errorf("progress does not include %q: %s", fragment, &stderr)
		}
	}
	if strings.ContainsFunc(stderr.String(), func(r rune) bool { return r > 127 }) {
		t.Errorf("progress contains non-English output: %s", &stderr)
	}
}

func TestProgressOutputSerializesConcurrentNotifications(t *testing.T) {
	var output bytes.Buffer
	progress := &progressOutput{writer: &output}
	var senders sync.WaitGroup
	for range 8 {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for range 10 {
				progress.report(infralogrecovery.Progress{Phase: infralogrecovery.ProgressRunning, Scope: "file-logger"})
			}
		}()
	}
	senders.Wait()
	if err := progress.result(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(output.String(), "[recovery] Processing scope=file-logger\n"); got != 80 {
		t.Fatalf("progress lines = %d, want 80; output=%s", got, &output)
	}
}

func TestProgressPhaseDescriptions(t *testing.T) {
	tests := []struct {
		phase infralogrecovery.ProgressPhase
		want  string
	}{
		{infralogrecovery.ProgressPreparing, "Preparing configuration and storage directories"},
		{infralogrecovery.ProgressRunning, "Processing"},
		{infralogrecovery.ProgressRetryWaiting, "Waiting to retry failed operations"},
		{infralogrecovery.ProgressStopping, "Shutdown requested. Waiting for accepted recovery operations and checkpoint saves to complete. Press Ctrl+C again to force shutdown."},
		{infralogrecovery.ProgressForced, "Forced shutdown requested. Waiting for in-progress OS I/O to return, if any."},
		{infralogrecovery.ProgressComplete, "Completed"},
		{infralogrecovery.ProgressPhase("custom-phase"), "custom-phase"},
	}
	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			if got := progressPhaseDescription(tt.phase); got != tt.want {
				t.Errorf("progressPhaseDescription(%q) = %q, want %q", tt.phase, got, tt.want)
			}
		})
	}
}

func TestProgressOutputFailureReturnsFailure(t *testing.T) {
	deps := reportDependencies(sampleReport(), nil)
	deps.Recover = func(_ context.Context, _ config.AppConfig, opts infralogrecovery.Options) (infralogrecovery.Report, error) {
		opts.OnProgress(infralogrecovery.Progress{Phase: infralogrecovery.ProgressRunning})
		return sampleReport(), nil
	}
	var stdout bytes.Buffer
	if exit := Run(context.Background(), validArgs("--retry", "--json"), &stdout, failedWriter{}, deps); exit != 1 {
		t.Fatalf("Run() = %d, want 1", exit)
	}
	var report infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0], "write recovery progress: output closed") {
		t.Fatalf("report.Errors = %v", report.Errors)
	}
}

func reportDependencies(report infralogrecovery.Report, err error) Dependencies {
	return Dependencies{
		LoadConfig: func(string) (config.AppConfig, error) { return consistentConfig(), nil },
		Recover: func(context.Context, config.AppConfig, infralogrecovery.Options) (infralogrecovery.Report, error) {
			return report, err
		},
	}
}

func sampleReport() infralogrecovery.Report {
	return infralogrecovery.Report{
		Directories: []infralogrecovery.DirectoryReport{{
			Scope: "file-logger", DirectoryPath: "/archives with spaces", ScannedFiles: 5,
			CompressedFiles: []string{"new.log.gz"}, AcceptedFiles: []string{"new.log.gz", "old.log.gz"},
			RemovedFiles: []string{"old.log.gz"}, RecoveredDeletePendingFiles: []string{"pending.log.gz"},
		}},
		Warnings: []string{"archive checkpoint recreated; existing archives can be uploaded again"},
	}
}

func TestJSONReportRetainsDetailedResults(t *testing.T) {
	want := sampleReport()
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs("--json"), &stdout, &stderr, reportDependencies(want, nil)); exit != 0 {
		t.Fatalf("Run() = %d; stderr=%s", exit, &stderr)
	}
	var got infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v; stdout=%s", err, &stdout)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("report = %+v, want %+v", got, want)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"accepted_files"`)) || bytes.Contains(stdout.Bytes(), []byte(`"uploaded_files"`)) {
		t.Errorf("JSON must identify accepted files without implying remote upload success: %s", &stdout)
	}
	if stderr.Len() != 0 {
		t.Errorf("unexpected stderr: %s", &stderr)
	}
}

func TestHumanReportIncludesCountsAndWarnings(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs(), &stdout, &stderr, reportDependencies(sampleReport(), nil)); exit != 0 {
		t.Fatalf("Run() = %d; stderr=%s", exit, &stderr)
	}
	for _, want := range []string{`file-logger "/archives with spaces"`, "scanned=5 compressed=1 accepted=2 removed=1 recovered-delete-pending=1", sampleReport().Warnings[0]} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout = %q, want %q", stdout.String(), want)
		}
	}
}

func TestHumanReportIncludesEachFailedPathAndExactError(t *testing.T) {
	report := sampleReport()
	report.Directories[0].FailedFiles = []infralogrecovery.FileFailureReport{
		{Path: "/archives with spaces/first.log.gz", Error: "upload failed: destination unavailable"},
		{Path: "/archives with spaces/second.log.gz", Error: "checkpoint save failed: access denied\nretry after permission repair"},
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs(), &stdout, &stderr, reportDependencies(report, nil)); exit != 1 {
		t.Fatalf("Run() = %d, want 1; stderr=%s", exit, &stderr)
	}
	if !strings.Contains(stdout.String(), "failed=2") {
		t.Errorf("failure count missing: %s", &stdout)
	}
	for _, failure := range report.Directories[0].FailedFiles {
		if !strings.Contains(stdout.String(), failure.Path) || !strings.Contains(stdout.String(), failure.Error) {
			t.Errorf("failure %+v missing from report: %s", failure, &stdout)
		}
	}
}

func TestConfigurationFailureStopsBeforeRecovery(t *testing.T) {
	deps := unexpectedDependencies(t)
	deps.LoadConfig = func(string) (config.AppConfig, error) {
		return config.AppConfig{}, errors.New("environment file: permission denied")
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs("--json"), &stdout, &stderr, deps); exit != 1 {
		t.Fatalf("Run() = %d, want 1", exit)
	}
	var report infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	want := "load config: environment file: permission denied"
	if len(report.Errors) != 1 || report.Errors[0] != want || !strings.Contains(stderr.String(), want) {
		t.Fatalf("report errors=%v stderr=%s", report.Errors, &stderr)
	}
}

func TestRecoveryFailurePreservesPartialReportAndExactError(t *testing.T) {
	want := sampleReport()
	recoveryErr := errors.New("upload old.log.gz: destination unavailable\nclose checkpoint: access denied")
	want.Errors = []string{recoveryErr.Error()}
	want.Directories[0].FailedFiles = []infralogrecovery.FileFailureReport{
		{Path: "/archives with spaces/old.log.gz", Error: "destination unavailable"},
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs("--json"), &stdout, &stderr, reportDependencies(want, recoveryErr)); exit != 1 {
		t.Fatalf("Run() = %d, want 1", exit)
	}
	var got infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("report = %+v, want %+v", got, want)
	}
	if !strings.Contains(stderr.String(), recoveryErr.Error()) {
		t.Errorf("exact recovery error missing from stderr: %s", &stderr)
	}
}

func TestReportErrorsReturnFailureEvenWithoutAggregateError(t *testing.T) {
	for _, test := range []struct {
		report infralogrecovery.Report
		want   string
	}{
		{report: infralogrecovery.Report{Errors: []string{"checkpoint failed"}}, want: "Error: checkpoint failed\n"},
		{
			report: infralogrecovery.Report{Directories: []infralogrecovery.DirectoryReport{{Scope: "file-logger", DirectoryPath: "/archive", Error: "compression failed"}}},
			want:   "  Error: compression failed\n",
		},
	} {
		var stdout, stderr bytes.Buffer
		if exit := Run(context.Background(), validArgs(), &stdout, &stderr, reportDependencies(test.report, nil)); exit != 1 {
			t.Errorf("Run() = %d, want 1; report=%+v", exit, test.report)
		}
		// 件数表示のfailed=は常に出力されるため、error本文そのものを確認します。
		if !strings.Contains(stdout.String(), test.want) {
			t.Errorf("report error %q missing from output: %s", test.want, &stdout)
		}
	}
}

func TestRecoveryJoinedErrorsAreReportedOnce(t *testing.T) {
	first := errors.New(`recover file-logger directory "/archive": upload old.log.gz: destination unavailable`)
	second := errors.New(`recover log-backend-failure-archive/opensearch directory "/failures": close checkpoint: access denied`)
	tests := []struct {
		name   string
		args   []string
		report infralogrecovery.Report
		err    error
		stderr io.Writer
		want   []string
	}{
		{
			// Recoverは個々のerrorを記録し、同じerrorをerrors.Joinで返します。
			name: "recorded components", args: validArgs("--json"),
			report: infralogrecovery.Report{Errors: []string{first.Error(), second.Error()}}, err: errors.Join(first, second),
			want: []string{first.Error(), second.Error()},
		},
		{
			name: "unrecorded components", args: validArgs("--json"),
			report: infralogrecovery.Report{}, err: errors.Join(first, second),
			want: []string{first.Error(), second.Error()},
		},
		{
			name: "recorded components with progress failure", args: validArgs("--retry", "--json"),
			report: infralogrecovery.Report{Errors: []string{first.Error(), second.Error()}}, err: errors.Join(first, second),
			stderr: failedWriter{},
			want:   []string{first.Error(), second.Error(), "write recovery progress: output closed"},
		},
		{
			// 文脈を加えて包んだerrorは、構成要素が記録済みでも一つのerrorとして残します。
			name: "wrapped components", args: validArgs("--json"),
			report: infralogrecovery.Report{Errors: []string{first.Error()}}, err: fmt.Errorf("finish recovery: %w; %w", first, second),
			want: []string{first.Error(), "finish recovery: " + first.Error() + "; " + second.Error()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := reportDependencies(tt.report, tt.err)
			deps.Recover = func(_ context.Context, _ config.AppConfig, opts infralogrecovery.Options) (infralogrecovery.Report, error) {
				if opts.OnProgress != nil {
					opts.OnProgress(infralogrecovery.Progress{Phase: infralogrecovery.ProgressRunning, Scope: "file-logger"})
				}
				report := tt.report
				report.Errors = slices.Clone(tt.report.Errors)
				return report, tt.err
			}
			var stdout, stderr bytes.Buffer
			var errorOutput io.Writer = &stderr
			if tt.stderr != nil {
				errorOutput = tt.stderr
			}
			if exit := Run(context.Background(), tt.args, &stdout, errorOutput, deps); exit != 1 {
				t.Fatalf("Run() = %d, want 1; stderr=%s", exit, &stderr)
			}
			var got infralogrecovery.Report
			if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
				t.Fatalf("invalid JSON: %v; stdout=%s", err, &stdout)
			}
			if !slices.Equal(got.Errors, tt.want) {
				t.Errorf("report.Errors = %q, want %q", got.Errors, tt.want)
			}
			if tt.stderr == nil && !strings.Contains(stderr.String(), tt.err.Error()) {
				t.Errorf("exact recovery error missing from stderr: %s", &stderr)
			}
		})
	}
}

// 環境設定が空でも、各loggerの設定は認証情報なしで読み込めます。既定の出力先は
// 標準出力だけなので、設定エラーではなく復旧対象がないことを報告します。
func TestRunWithEmptyEnvironmentReportsNoConfiguredFiles(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "empty.env")
	if err := os.WriteFile(envFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	testutil.UnsetConfigEnvironment(t)
	for _, logger := range []string{"sync", "async", "delivery"} {
		t.Run(logger, func(t *testing.T) {
			var recoverErr error
			deps := Dependencies{
				// cmd/log-recoveryと同じく、指定した環境設定fileをHETEROLOG_ENV_PATHで読み込みます。
				LoadConfig: func(path string) (config.AppConfig, error) {
					if err := os.Setenv("HETEROLOG_ENV_PATH", path); err != nil {
						return config.AppConfig{}, err
					}
					return config.LoadConfig()
				},
				Recover: func(ctx context.Context, cfg config.AppConfig, opts infralogrecovery.Options) (infralogrecovery.Report, error) {
					report, err := bootstrap.RecoverLogs(ctx, cfg, opts)
					recoverErr = err
					return report, err
				},
			}
			var stdout, stderr bytes.Buffer
			if exit := Run(context.Background(), validArgs("--env-file", envFile, "--logger", logger), &stdout, &stderr, deps); exit != 1 {
				t.Fatalf("Run() = %d, want 1; stderr=%s", exit, &stderr)
			}
			// 設定を読み込めた場合だけ復旧処理まで進み、復旧対象がないことを返します。
			if !errors.Is(recoverErr, infralogrecovery.ErrNoConfiguredLogFiles) {
				t.Fatalf("Recover error = %v, want ErrNoConfiguredLogFiles; stderr=%s", recoverErr, &stderr)
			}
			if !strings.Contains(stdout.String(), "Error: "+recoverErr.Error()) {
				t.Fatalf("report does not include the recovery error: %s", &stdout)
			}
		})
	}
}

// writeRecoveryEnvFileは、保存先をrootの一時directoryに置いた環境設定fileを書き、設定loaderが読む
// 環境変数をprocess environmentから外します。cmd/log-recoveryと同じくHETEROLOG_ENV_PATHで読み込みます。
func writeRecoveryEnvFile(t *testing.T, root, settings string) string {
	t.Helper()
	envFile := filepath.Join(root, "recovery.env")
	content := fmt.Sprintf("HETEROLOG_SYSTEM_LOG_DIR=%s\nHETEROLOG_SYSTEM_LIB_DIR=%s\nHETEROLOG_APP_STATE_DIR=%s\n%s",
		filepath.Join(root, "log"), filepath.Join(root, "lib"), filepath.Join(root, "state"), settings)
	if err := os.WriteFile(envFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	testutil.UnsetConfigEnvironment(t)
	return envFile
}

func loadRecoveryEnvFile(path string) (config.AppConfig, error) {
	if err := os.Setenv("HETEROLOG_ENV_PATH", path); err != nil {
		return config.AppConfig{}, err
	}
	return config.LoadConfig()
}

// 読み込めても組み合わせが不整合な設定（同期loggerとDelivery loggerのfile保存先の重複）は、
// アプリケーションの起動と同じconfig.ValidateApplicationConfigで拒否し、復旧を始める前に
// 設定errorとして終えます（終了code 1）。保存先のdirectoryは作りません。
func TestRunRejectsInconsistentConfigurationBeforeRecovery(t *testing.T) {
	root := t.TempDir()
	envFile := writeRecoveryEnvFile(t, root,
		"HETEROLOG_LOG_TYPE=file\nHETEROLOG_DELIVERY_LOG_TYPE=file\nHETEROLOG_LOG_DIR=/shared\nHETEROLOG_DELIVERY_LOG_DIR=/shared\n")
	var loaded config.AppConfig
	deps := unexpectedDependencies(t)
	deps.LoadConfig = func(path string) (config.AppConfig, error) {
		cfg, err := loadRecoveryEnvFile(path)
		loaded = cfg
		return cfg, err
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs("--env-file", envFile, "--json"), &stdout, &stderr, deps); exit != 1 {
		t.Fatalf("Run() = %d, want 1; stderr=%s", exit, &stderr)
	}

	// 検証の対象は、--targetで上書きした後の設定です。報告するerrorは、その検証のerrorそのものです。
	loaded.RuntimeConfig.Target = "service"
	validationErr := config.ValidateApplicationConfig(loaded)
	var collision *config.StorageDirectoryCollisionError
	if !errors.As(validationErr, &collision) {
		t.Fatalf("ValidateApplicationConfig() = %v, want a storage directory collision", validationErr)
	}
	var report infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON: %v; stdout=%s", err, &stdout)
	}
	want := "invalid config: " + validationErr.Error()
	if !slices.Equal(report.Errors, []string{want}) || len(report.Directories) != 0 || !strings.Contains(stderr.String(), want) {
		t.Fatalf("report=%+v stderr=%s, want only the configuration error %q", report, &stderr, want)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "recovery.env" {
		t.Fatalf("storage root entries = %v, want only the environment file", entries)
	}
}

// 整合した最小の環境設定（同期loggerのfile出力だけ）は検証を通り、そのまま復旧を行います。
func TestRunWithConsistentMinimalEnvironmentRecovers(t *testing.T) {
	root := t.TempDir()
	envFile := writeRecoveryEnvFile(t, root, "HETEROLOG_LOG_TYPE=file\n")
	recoveries := 0
	deps := Dependencies{
		LoadConfig: loadRecoveryEnvFile,
		Recover: func(ctx context.Context, cfg config.AppConfig, opts infralogrecovery.Options) (infralogrecovery.Report, error) {
			recoveries++
			return bootstrap.RecoverLogs(ctx, cfg, opts)
		},
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(context.Background(), validArgs("--env-file", envFile, "--json"), &stdout, &stderr, deps); exit != 0 {
		t.Fatalf("Run() = %d, want 0; stdout=%s stderr=%s", exit, &stdout, &stderr)
	}
	var report infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON: %v; stdout=%s", err, &stdout)
	}
	if recoveries != 1 || len(report.Errors) != 0 || len(report.Directories) == 0 {
		t.Fatalf("recoveries=%d report=%+v, want one recovery of the configured file logger", recoveries, report)
	}
	for _, directory := range report.Directories {
		if directory.Scope != string(infralogrecovery.RecoveryScopeFileLogger) || directory.Error != "" {
			t.Errorf("directory report = %+v, want a successful file-logger directory", directory)
		}
	}
}

func TestCanceledContextDoesNotLoadConfiguration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if exit := Run(ctx, validArgs(), &stdout, &stderr, unexpectedDependencies(t)); exit != 130 {
		t.Fatalf("Run() = %d, want 130", exit)
	}
	if !strings.Contains(stdout.String(), context.Canceled.Error()) {
		t.Errorf("cancellation missing from report: %s", &stdout)
	}
}

func TestCancellationAfterConfigurationDoesNotStartRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := unexpectedDependencies(t)
	deps.LoadConfig = func(string) (config.AppConfig, error) {
		cancel()
		return config.AppConfig{}, nil
	}
	if exit := Run(ctx, validArgs(), io.Discard, io.Discard, deps); exit != 130 {
		t.Fatalf("Run() = %d, want 130", exit)
	}
}

func TestRecoveryCancellationPreservesPartialReport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := reportDependencies(sampleReport(), nil)
	deps.Recover = func(context.Context, config.AppConfig, infralogrecovery.Options) (infralogrecovery.Report, error) {
		cancel()
		return sampleReport(), errors.Join(errors.New("upload interrupted"), context.Canceled)
	}
	var stdout, stderr bytes.Buffer
	if exit := Run(ctx, validArgs("--json"), &stdout, &stderr, deps); exit != 130 {
		t.Fatalf("Run() = %d, want 130", exit)
	}
	var got infralogrecovery.Report
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	// 結合したerrorは、Recoverの記録と同じく構成要素ごとにreportへ残します。
	if !reflect.DeepEqual(got.Directories, sampleReport().Directories) || !slices.Equal(got.Errors, []string{"upload interrupted", context.Canceled.Error()}) {
		t.Fatalf("partial report = %+v", got)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("output closed") }

func TestOutputFailuresReturnFailure(t *testing.T) {
	for _, args := range [][]string{{"--help"}, validArgs(), validArgs("--json")} {
		var stderr bytes.Buffer
		if exit := Run(context.Background(), args, failedWriter{}, &stderr, reportDependencies(sampleReport(), nil)); exit != 1 {
			t.Errorf("Run(%v) = %d, want 1", args, exit)
		}
		if !strings.Contains(stderr.String(), "output closed") {
			t.Errorf("output failure missing: %s", &stderr)
		}
	}
}
