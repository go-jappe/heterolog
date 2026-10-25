// Package logrecovery はアプリケーション停止中の手動ログ復旧CLIを提供します。
package logrecovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	application "github.com/go-jappe/heterolog/internal/application/logrecovery"
	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/domain/target"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Dependencies は設定読込と復旧処理を注入し、CLIの検証時に実際の環境設定、
// archive、object storageへアクセスしないようにします。
type Dependencies struct {
	LoadConfig func(envFile string) (config.AppConfig, error)
	Recover    func(context.Context, config.AppConfig, application.Options) (application.Report, error)
	// ForceStopは、通常の終了待機を打ち切る明示的な要求です。
	ForceStop <-chan struct{}
}

type options struct {
	target             string
	envFile            string
	recovery           application.Options
	applicationStopped bool
	json               bool
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	var loggerInput, scopeInput string
	flags := flag.NewFlagSet("log-recovery", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&opts.target, "target", "", "Application storage identifier (required; letters, digits, dots, hyphens, underscores)")
	flags.StringVar(&opts.envFile, "env-file", "", "Environment file to load (default: HETEROLOG_ENV_PATH or .env)")
	flags.StringVar(&loggerInput, "logger", "sync", "Logger configuration to use: sync, async, delivery")
	flags.StringVar(&scopeInput, "scope", string(application.RecoveryScopeAll), fmt.Sprintf("File series to recover: %s, %s, %s",
		application.RecoveryScopeFileLogger, application.RecoveryScopeLogBackendFailureArchive, application.RecoveryScopeAll))
	flags.BoolVar(&opts.applicationStopped, "application-stopped", false, "Confirm that all applications sharing the selected directories are stopped")
	flags.BoolVar(&opts.json, "json", false, "Print detailed recovery results as JSON")
	flags.BoolVar(&opts.recovery.Retry, "retry", false, "Retry failed recovery operations; graceful shutdown waits for accepted work to complete")
	flags.DurationVar(&opts.recovery.RetryInterval, "retry-interval", application.DefaultRetryInterval, "Delay between retries (requires --retry; must be greater than zero)")
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: log-recovery --target <application-id> --application-stopped [options]")
		fmt.Fprintln(output, "\nLoad the application's environment configuration and run one recovery pass.")
		fmt.Fprintln(output, "With --retry, failed operations are retried after a delay and progress is written to stderr.")
		fmt.Fprintln(output, "The first Ctrl+C or SIGTERM requests graceful shutdown; the second requests forced shutdown.")
		fmt.Fprintln(output, "Stop all applications and other recovery CLIs sharing the selected directories before running this command.")
		fmt.Fprintln(output, "--application-stopped is your acknowledgement; it does not automatically detect running processes.")
		fmt.Fprintln(output, "This command does not purge archives or replay record spools.")
		fmt.Fprintln(output, "It also does not replay compressed-log spools or process .preserved files.")
		fmt.Fprintln(output, "\nOptions:")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if opts.target == "" {
		return opts, errors.New("--target is required")
	}
	if !target.IsValidTarget(opts.target) {
		return opts, fmt.Errorf("invalid --target %q: use a portable application identifier beginning with a letter or digit", opts.target)
	}
	if !opts.applicationStopped {
		return opts, errors.New("--application-stopped is required: stop every application sharing the selected archive directories first")
	}
	contract, err := parseLoggerContract(loggerInput)
	if err != nil {
		return opts, err
	}
	opts.recovery.LoggerContract = contract
	opts.recovery.Scope = application.RecoveryScope(scopeInput)
	if !opts.recovery.Scope.IsValid() {
		return opts, fmt.Errorf("invalid --scope %q: choose %s, %s, or %s", scopeInput,
			application.RecoveryScopeFileLogger, application.RecoveryScopeLogBackendFailureArchive, application.RecoveryScopeAll)
	}
	var emptyEnvFile, explicitRetryInterval bool
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "env-file" && strings.TrimSpace(opts.envFile) == "" {
			emptyEnvFile = true
		}
		if f.Name == "retry-interval" {
			explicitRetryInterval = true
		}
	})
	if emptyEnvFile {
		return opts, errors.New("--env-file must name a nonempty path")
	}
	if explicitRetryInterval && !opts.recovery.Retry {
		return opts, errors.New("--retry-interval requires --retry")
	}
	if opts.recovery.RetryInterval <= 0 {
		return opts, errors.New("--retry-interval must be greater than zero")
	}
	return opts, nil
}

// parseLoggerContractはCLIの指定を、applicationで共有するlogger分類へ変換します。
func parseLoggerContract(input string) (pkglogger.LoggerContractKind, error) {
	switch input {
	case "sync":
		return pkglogger.LoggerContractSync, nil
	case "async":
		return pkglogger.LoggerContractAsyncBestEffort, nil
	case "delivery":
		return pkglogger.LoggerContractDelivery, nil
	default:
		return "", fmt.Errorf("invalid --logger %q: choose sync, async, or delivery", input)
	}
}

// Run は成功時0、復旧・設定・出力失敗時1、引数不備時2、cancel時130を返します。
// processを終了せず、終了codeの適用は呼び出し側へ委ねます。
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, deps Dependencies) int {
	var flagOutput bytes.Buffer
	opts, err := parseOptions(args, &flagOutput)
	if errors.Is(err, flag.ErrHelp) {
		if _, err := io.Copy(stdout, &flagOutput); err != nil {
			fmt.Fprintf(stderr, "write help: %v\n", err)
			return 1
		}
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "log-recovery: %v\nUse --help for usage.\n", err)
		return 2
	}

	var report application.Report
	progress := &progressOutput{writer: stderr}
	opts.recovery.ForceStop = deps.ForceStop
	if opts.recovery.Retry {
		opts.recovery.OnProgress = progress.report
	}
	if err = ctx.Err(); err == nil {
		if deps.LoadConfig == nil || deps.Recover == nil {
			err = errors.New("recovery command dependencies are not configured")
		} else {
			var cfg config.AppConfig
			cfg, err = deps.LoadConfig(opts.envFile)
			if err != nil {
				err = fmt.Errorf("load config: %w", err)
			} else if err = ctx.Err(); err == nil {
				cfg.RuntimeConfig.Target = opts.target
				// 読み込みと--targetの上書きを終えた設定を、アプリケーションの起動と同じ全体の規則
				// （保存先directoryの所有権、upload対象、failure store、timeout階層、memory health）で
				// 検証してから復旧を始めます。不整合な設定は、保存先に触れずに設定errorとして終えます。
				if err = config.ValidateApplicationConfig(cfg); err != nil {
					err = fmt.Errorf("invalid config: %w", err)
				} else {
					report, err = deps.Recover(ctx, cfg, opts.recovery)
				}
			}
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if outputErr := progress.result(); outputErr != nil {
		err = errors.Join(err, fmt.Errorf("write recovery progress: %w", outputErr))
	}
	if err != nil {
		report.Errors = appendReportError(report.Errors, err)
	}
	if outputErr := writeReport(stdout, report, opts.json); outputErr != nil {
		fmt.Fprintf(stderr, "write recovery report: %v\n", outputErr)
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return 130
		}
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "log-recovery: %v\n", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return 130
	}
	if err != nil || len(report.Errors) > 0 {
		return 1
	}
	for _, directory := range report.Directories {
		if directory.Error != "" || len(directory.FailedFiles) > 0 {
			return 1
		}
	}
	return 0
}

// appendReportErrorは、reportに記録されていないerrorだけを追加します。
// Recoverは個々のerrorを別々に記録してerrors.Joinで返すため、結合したerrorは
// 構成要素ごとに照合し、記録済みの要素を結合後の文字列として重複させません。
func appendReportError(messages []string, err error) []string {
	if err == nil || slices.Contains(messages, err.Error()) {
		return messages
	}
	if parts, joined := joinedErrors(err); joined {
		for _, part := range parts {
			messages = appendReportError(messages, part)
		}
		return messages
	}
	return append(messages, err.Error())
}

// joinedErrorsは、errors.Joinと同じく構成要素のmessageを改行で連結しただけのerrorを
// 構成要素へ分けます。fmt.Errorfなどで文脈を加えたerrorは、その文脈を保つため分けません。
func joinedErrors(err error) ([]error, bool) {
	multiple, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return nil, false
	}
	var parts []error
	var messages []string
	for _, part := range multiple.Unwrap() {
		if part != nil {
			parts = append(parts, part)
			messages = append(messages, part.Error())
		}
	}
	if len(parts) == 0 || strings.Join(messages, "\n") != err.Error() {
		return nil, false
	}
	return parts, true
}

func writeReport(output io.Writer, report application.Report, asJSON bool) error {
	if asJSON {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	var text bytes.Buffer
	if len(report.Directories) == 0 {
		fmt.Fprintln(&text, "No archive directories were processed.")
	}
	for _, directory := range report.Directories {
		fmt.Fprintf(&text, "%s %q: scanned=%d compressed=%d accepted=%d removed=%d recovered-delete-pending=%d failed=%d\n",
			directory.Scope, directory.DirectoryPath, directory.ScannedFiles,
			len(directory.CompressedFiles), len(directory.AcceptedFiles), len(directory.RemovedFiles),
			len(directory.RecoveredDeletePendingFiles), len(directory.FailedFiles))
		for _, failure := range directory.FailedFiles {
			fmt.Fprintf(&text, "  Failed %q: %s\n", failure.Path, failure.Error)
		}
		if directory.Error != "" {
			fmt.Fprintf(&text, "  Error: %s\n", directory.Error)
		}
	}
	for _, warning := range report.Warnings {
		fmt.Fprintf(&text, "Warning: %s\n", warning)
	}
	for _, message := range report.Errors {
		fmt.Fprintf(&text, "Error: %s\n", message)
	}
	_, err := io.Copy(output, &text)
	return err
}
