// Package logdecryptは、暗号化したlog file・failure archive・OTLP/JSONのfileを秘密鍵で
// 復号するCLIと、その鍵を作るCLIです。
package logdecrypt

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
)

// Dependenciesは、鍵の生成と復号の処理を注入し、CLIの検証で実際の鍵やfileに触れないように
// します。
type Dependencies struct {
	Keygen      func(identityPath string) (recipient, keyID string, err error)
	SpoolKeygen func(keyPath string) (keyID string, err error)
	Decrypt     func(identityPath, keyPath, inputPath string, output io.Writer) (cipherfile.Report, error)
}

// 終了codeです。復号できたが読み飛ばした箇所があるfileは、errorとは別のcodeで知らせます。
const (
	exitOK         = 0
	exitError      = 1
	exitUsage      = 2
	exitIncomplete = 3
)

// Runは、argsのsubcommand（keygen・decrypt）を実行し、終了codeを返します。
func Run(args []string, stdout, stderr io.Writer, deps Dependencies) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		usage(stdout)
		if len(args) == 0 {
			return exitUsage
		}
		return exitOK
	}
	switch args[0] {
	case "keygen":
		return runKeygen(args[1:], stdout, stderr, deps)
	case "decrypt":
		return runDecrypt(args[1:], stdout, stderr, deps)
	default:
		fmt.Fprintf(stderr, "log-decrypt: unknown command %q\n", args[0])
		usage(stderr)
		return exitUsage
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "Usage: log-decrypt keygen --identity <file>")
	fmt.Fprintln(w, "       log-decrypt keygen --spool-key <file>")
	fmt.Fprintln(w, "       log-decrypt decrypt [--identity <file>] [--spool-key <file>] [--output <file> | --output-dir <dir>] <encrypted file>...")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "keygen --identity writes a new private key to the identity file (mode 0600, never overwritten)")
	fmt.Fprintln(w, "and prints the public key to set as HETEROLOG_ENCRYPTION_RECIPIENT. Keep the identity file")
	fmt.Fprintln(w, "outside the application: the loggers only need the public key.")
	fmt.Fprintln(w, "keygen --spool-key writes a new symmetric key for the spool files to the key file (mode 0600),")
	fmt.Fprintln(w, "to give to the application as HETEROLOG_SPOOL_ENCRYPTION_KEY_FILE.")
	fmt.Fprintln(w, "decrypt writes the plaintext of each encrypted file to stdout, to --output (one input), or to")
	fmt.Fprintln(w, "--output-dir under the input's file name. Log files, failure archives and OTLP files need")
	fmt.Fprintln(w, "--identity; spool files need --spool-key. Damaged bytes and segments without a matching key")
	fmt.Fprintln(w, "are skipped and reported on stderr; the exit code is then 3.")
}

func runKeygen(args []string, stdout, stderr io.Writer, deps Dependencies) int {
	var identityPath, keyPath string
	flags := flag.NewFlagSet("log-decrypt keygen", flag.ContinueOnError)
	var flagOutput bytes.Buffer
	flags.SetOutput(&flagOutput)
	flags.StringVar(&identityPath, "identity", "", "File to write the private key of the public-key encryption to")
	flags.StringVar(&keyPath, "spool-key", "", "File to write the symmetric key of the spool encryption to")
	if code, done := parseFlags(flags, args, &flagOutput, stdout, stderr); done {
		return code
	}
	if (identityPath == "") == (keyPath == "") || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "log-decrypt keygen: give exactly one of --identity <file> and --spool-key <file>, and no other arguments")
		return exitUsage
	}
	if keyPath != "" {
		if deps.SpoolKeygen == nil {
			fmt.Fprintln(stderr, "log-decrypt keygen: the command is not configured")
			return exitError
		}
		keyID, err := deps.SpoolKeygen(keyPath)
		if err != nil {
			fmt.Fprintf(stderr, "log-decrypt keygen: %v\n", err)
			return exitError
		}
		fmt.Fprintf(stdout, "spool key file: %s\nkey id: %s\nHETEROLOG_SPOOL_ENCRYPTION_KEY_FILE=%s\n", keyPath, keyID, keyPath)
		return exitOK
	}
	if deps.Keygen == nil {
		fmt.Fprintln(stderr, "log-decrypt keygen: the command is not configured")
		return exitError
	}
	recipient, keyID, err := deps.Keygen(identityPath)
	if err != nil {
		fmt.Fprintf(stderr, "log-decrypt keygen: %v\n", err)
		return exitError
	}
	fmt.Fprintf(stdout, "identity file: %s\nkey id: %s\nHETEROLOG_ENCRYPTION_RECIPIENT=%s\n", identityPath, keyID, recipient)
	return exitOK
}

func runDecrypt(args []string, stdout, stderr io.Writer, deps Dependencies) int {
	var identityPath, keyPath, outputPath, outputDir string
	flags := flag.NewFlagSet("log-decrypt decrypt", flag.ContinueOnError)
	var flagOutput bytes.Buffer
	flags.SetOutput(&flagOutput)
	flags.StringVar(&identityPath, "identity", "", "File holding the private key(s) for log files, failure archives and OTLP files")
	flags.StringVar(&keyPath, "spool-key", "", "File holding the symmetric key(s) for spool files")
	flags.StringVar(&outputPath, "output", "", "File to write the plaintext to (one input only; default: stdout)")
	flags.StringVar(&outputDir, "output-dir", "", "Directory to write one plaintext file per input, named as the input")
	if code, done := parseFlags(flags, args, &flagOutput, stdout, stderr); done {
		return code
	}
	inputs := flags.Args()
	switch {
	case identityPath == "" && keyPath == "":
		fmt.Fprintln(stderr, "log-decrypt decrypt: --identity <file> or --spool-key <file> is required")
		return exitUsage
	case len(inputs) == 0:
		fmt.Fprintln(stderr, "log-decrypt decrypt: give at least one encrypted file")
		return exitUsage
	case outputPath != "" && outputDir != "":
		fmt.Fprintln(stderr, "log-decrypt decrypt: --output and --output-dir cannot be combined")
		return exitUsage
	case outputPath != "" && len(inputs) != 1, outputPath == "" && outputDir == "" && len(inputs) != 1:
		fmt.Fprintln(stderr, "log-decrypt decrypt: several inputs need --output-dir")
		return exitUsage
	}
	if deps.Decrypt == nil {
		fmt.Fprintln(stderr, "log-decrypt decrypt: the command is not configured")
		return exitError
	}
	code := exitOK
	for _, input := range inputs {
		fileCode := decryptOne(input, identityPath, keyPath, outputPath, outputDir, stdout, stderr, deps)
		code = max(code, fileCode)
	}
	return code
}

// decryptOneは、inputを復号します。出力先のfileは、すでにあれば上書きしません。
func decryptOne(input, identityPath, keyPath, outputPath, outputDir string, stdout, stderr io.Writer, deps Dependencies) int {
	var output io.Writer = stdout
	var file *os.File
	if outputPath != "" || outputDir != "" {
		path := outputPath
		if path == "" {
			path = filepath.Join(outputDir, filepath.Base(input))
		}
		var err error
		file, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			fmt.Fprintf(stderr, "log-decrypt decrypt: create the output file: %v\n", err)
			return exitError
		}
		output = file
	}
	report, err := deps.Decrypt(identityPath, keyPath, input, output)
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if err != nil {
		fmt.Fprintf(stderr, "log-decrypt decrypt: %v\n", err)
		return exitError
	}
	writeReport(stderr, input, report)
	if !report.Complete() {
		return exitIncomplete
	}
	return exitOK
}

// writeReportは、復号の結果をstderrに書きます。平文はstdoutに出るため、混ざらないようにします。
func writeReport(w io.Writer, input string, report cipherfile.Report) {
	fmt.Fprintf(w, "%s: %d segments, %d records, %d bytes of plaintext\n", input, report.Segments, report.Records, report.PlaintextBytes)
	for _, id := range report.UnknownKeyIDs {
		fmt.Fprintf(w, "%s: no identity for key id %s\n", input, id)
	}
	for _, skip := range report.Skipped {
		fmt.Fprintf(w, "%s: skipped %d bytes at offset %d: %s\n", input, skip.Length, skip.Offset, skip.Reason)
	}
}

// parseFlagsは、flagを読み、--helpなら使い方を書いて終えます。doneなら呼び出し元はcodeを返します。
func parseFlags(flags *flag.FlagSet, args []string, flagOutput *bytes.Buffer, stdout, stderr io.Writer) (code int, done bool) {
	err := flags.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		usage(stdout)
		flags.SetOutput(stdout)
		flags.PrintDefaults()
		return exitOK, true
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", flags.Name(), err)
		return exitUsage, true
	}
	return exitOK, false
}
