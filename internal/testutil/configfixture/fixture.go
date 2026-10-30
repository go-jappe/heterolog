// Package configfixtureは、公開されている環境設定の例からofflineのfixtureを作成します。
package configfixture

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/joho/godotenv"
)

var safeNumber = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?$`)
var safeNumberList = regexp.MustCompile(`^[0-9]+(?:,[0-9]+)+$`)
var safeDirectory = regexp.MustCompile(`^(?:(?:async|delivery)_)?(?:opensearch|loki|cloudwatchlogs|clickhouse)_failure_(?:archives|spools)$`)
var fixtureStoragePaths = map[string]string{
	"HETEROLOG_SYSTEM_LOG_DIR": "fixture-storage/logs",
	"HETEROLOG_SYSTEM_LIB_DIR": "fixture-storage/spool",
	"HETEROLOG_APP_STATE_DIR":  "fixture-storage/state",
}
var safeValues = func() map[string]bool {
	values := make(map[string]bool)
	for _, value := range strings.Fields("true false DEV dev INFO DEBUG WARN ERROR info debug warn error daily weekly monthly gzip gz zstd zst lz4 xz inline runner disable strict unbounded bounded must_fit drop block stdout stderr file slack opensearch loki cloudwatchlogs clickhouse spool fileArchive none retryPolicy retry_policy s3 basic otel OTLP http grpc retryable permanent logs log error debug default file_logger async_file_logger delivery_file_logger /logs /error_logs /async_logs /async_logs_error /delivery_logs /delivery_logs_error /audit_logs /traces otlp traces_otlp traces_otlp_failure_archives opensearch_async loki_async clickhouse_async cloudwatchlogs_async compressed_log_file_upload_failures") {
		values[value] = true
	}
	return values
}()

// Generateが返すerrorです。messageに設定値を含めることはありません。
var (
	// ErrOperationalEnvSourceは、名前が.env.exampleではないsource pathを表します。
	ErrOperationalEnvSource = errors.New("source must be the public .env.example, not an operational .env")
	// ErrSourceNotRegularFileは、symbolic linkか、その他の通常fileではないsource pathを表します。
	ErrSourceNotRegularFile = errors.New("public .env.example must be a regular file, not a symbolic link")
	// ErrOutputDirectoryExistsは、すでに存在する出力directoryを表します。
	ErrOutputDirectoryExists = errors.New("output directory must not already exist")
	// ErrNonAssignmentLineは、'='を含まないcomment以外の行を表します。
	ErrNonAssignmentLine = errors.New("non-assignment line in public fixture")
	// ErrDuplicateKeyは、複数回代入されたkeyを表します。DuplicateKeyErrorがこれをwrapします。
	ErrDuplicateKey = errors.New("duplicate key")
	// ErrInvalidEnvironmentSyntaxは、dotenv parserが拒否するsourceを表します。
	ErrInvalidEnvironmentSyntax = errors.New("public .env.example has invalid environment syntax")
	// ErrUnreviewedKeysは、値がreview済みの一覧にないkeyを表します。UnreviewedKeysErrorがこれをwrapします。
	ErrUnreviewedKeys = errors.New("unreviewed keys")
)

// DuplicateKeyErrorは、sourceの中で複数回代入されたkeyのうち、最初のものを表します。
type DuplicateKeyError struct {
	Key string
}

func (e *DuplicateKeyError) Error() string { return "duplicate key: " + e.Key }

func (e *DuplicateKeyError) Unwrap() error { return ErrDuplicateKey }

// UnreviewedKeysErrorは、値がreviewされていないkeyを、sort済みの順で表します。
// それらの値を保存したり表示したりすることはありません。
type UnreviewedKeysError struct {
	Keys []string
}

func (e *UnreviewedKeysError) Error() string {
	return "unreviewed keys (values not printed): " + strings.Join(e.Keys, ", ")
}

func (e *UnreviewedKeysError) Unwrap() error { return ErrUnreviewedKeys }

// Generateは、公開された.env.example fileから新しいfixture directoryを作成します。
func Generate(sourcePath, outputDirectory string) error {
	// 実運用.envの読み取りや、既存出力の上書きは許可しません。
	if filepath.Base(sourcePath) != ".env.example" {
		return ErrOperationalEnvSource
	}
	info, err := os.Lstat(sourcePath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return ErrSourceNotRegularFile
	}
	if _, err := os.Lstat(outputDirectory); err == nil {
		return fmt.Errorf("%w: %s", ErrOutputDirectoryExists, outputDirectory)
	} else if !os.IsNotExist(err) {
		return err
	}
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(source), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		if !ok {
			return ErrNonAssignmentLine
		}
		key = strings.TrimSpace(key)
		if seen[key] {
			return &DuplicateKeyError{Key: key}
		}
		seen[key] = true
	}
	values, err := godotenv.Unmarshal(string(source))
	if err != nil {
		// parserの診断には元の設定値が含まれる場合があるため、そのまま表示しません。
		return ErrInvalidEnvironmentSyntax
	}
	var unknown []string
	for key, value := range values {
		if _, ok := sanitize(key, value, "sample"); !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) != 0 {
		sort.Strings(unknown)
		return &UnreviewedKeysError{Keys: unknown}
	}
	if err := os.Mkdir(outputDirectory, 0o700); err != nil {
		return err
	}
	for _, variant := range []struct{ name, value string }{{".env", "sample"}, {".env.example", "example"}} {
		sanitized := make(map[string]string, len(values))
		for key, value := range values {
			sanitized[key], _ = sanitize(key, value, variant.value)
		}
		// Windowsの環境消去後も、個人用directoryの既定値に依存しない保存先を明示します。
		for key, value := range fixtureStoragePaths {
			sanitized[key] = value
		}
		// 公開例のstdoutだけの既定値とは関係なく、offlineのcredentialですべての契約を動かします。
		// network clientはtestが差し替えます。
		sanitized["HETEROLOG_LOG_TYPE"] = "file,stdout,slack,opensearch,loki"
		sanitized["HETEROLOG_ASYNC_LOG_TYPE"] = "file,stdout,slack,opensearch"
		sanitized["HETEROLOG_DELIVERY_LOG_TYPE"] = "file,stdout,slack,opensearch,loki"
		for _, key := range []string{"HETEROLOG_SLACK_API_KEY", "HETEROLOG_ASYNC_SLACK_API_KEY", "HETEROLOG_DELIVERY_SLACK_API_KEY", "HETEROLOG_OPENSEARCH_USERNAME", "HETEROLOG_OPENSEARCH_PASSWORD", "HETEROLOG_ASYNC_OPENSEARCH_USERNAME", "HETEROLOG_ASYNC_OPENSEARCH_PASSWORD", "HETEROLOG_DELIVERY_OPENSEARCH_USERNAME", "HETEROLOG_DELIVERY_OPENSEARCH_PASSWORD"} {
			sanitized[key] = "fixture-" + variant.value
		}
		for _, key := range []string{"HETEROLOG_SLACK_NORMAL_CHANNEL", "HETEROLOG_SLACK_ERROR_CHANNEL"} {
			sanitized[key] = "C000FIXTURE"
		}
		body, err := godotenv.Marshal(sanitized)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(outputDirectory, variant.name), []byte("# Synthetic public configuration fixture; never a copy of the operational .env.\n"+body+"\n"), 0o600); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(outputDirectory, "go.mod"), []byte("module windows-config-fixture\n\ngo 1.25.0\n"), 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outputDirectory, "README.txt"), []byte(fixtureReadme), 0o600)
}

const fixtureReadme = `Synthetic offline configuration test root.
Both environment files were generated exclusively from the public .env.example.
Credentials, endpoints, channels, destination identifiers, and metadata were replaced with generated dummy values.
Unrecognized string values fail generation. Comments were discarded.
The operational .env was not read or copied.
The synthetic .env enables the normal Loki backend for the existing spool configuration tests.
Storage base paths are fixed relative fixture-storage/{logs,spool,state} values, never copied from the source.
Configuration tests must not create files under these paths; writer tests use their own temporary directories.
Run config.exe with this directory as its working directory. Keep TEMP and TMP in a separate writable guest directory.
Passing tests here validate fixture loading/mapping contracts, not the private deployment .env.
`

func sanitize(key, value, variant string) (string, bool) {
	if fixed, ok := fixtureStoragePaths[key]; ok {
		return fixed, true
	}
	if value == "" {
		return "", true
	}
	if strings.HasSuffix(key, "_HEADERS") {
		return `{"X-Config-Fixture":"` + variant + `"}`, true
	}
	if strings.HasSuffix(key, "_LABELS") {
		return `{"fixture":"` + variant + `"}`, true
	}
	if strings.HasSuffix(key, "_ENDPOINT") || strings.HasSuffix(key, "_ADDR") {
		if strings.HasSuffix(key, "_ADDR") {
			return "127.0.0.1:4317", true
		}
		return "http://" + variant + ".invalid:18080", true
	}
	for _, token := range []string{"PASSWORD", "TOKEN", "SECRET", "ACCESS_KEY", "API_KEY", "USERNAME"} {
		if strings.Contains(key, token) {
			return "fixture-" + variant, true
		}
	}
	if strings.Contains(key, "SLACK") && strings.Contains(key, "CHANNEL") {
		return "C000FIXTURE", true
	}
	if strings.HasSuffix(key, "_INDEX") || strings.HasSuffix(key, "_TABLE") || strings.HasSuffix(key, "_DATABASE") || strings.HasSuffix(key, "_TENANT_ID") {
		return "fixture_" + variant + "_" + strings.ToLower(strings.TrimPrefix(key, "HETEROLOG_")), true
	}
	if strings.HasSuffix(key, "_LOG_GROUP_NAME") || strings.HasSuffix(key, "_LOG_STREAM_NAME") {
		return "fixture-" + variant + "-" + strings.ToLower(strings.TrimPrefix(key, "HETEROLOG_")), true
	}
	if strings.HasSuffix(key, "_REGION") {
		return "us-east-1", true
	}
	if strings.HasSuffix(key, "_BUCKET") {
		return "offline-config-fixture", true
	}
	if strings.Contains(key, "OBJECT_STORAGE") && strings.HasSuffix(key, "_PREFIX") {
		return "fixture/" + strings.ToLower(strings.TrimPrefix(key, "HETEROLOG_")), true
	}
	if strings.HasSuffix(key, "_CERT_FILE") || strings.HasSuffix(key, "_KEY_FILE") || strings.HasSuffix(key, "_CA_FILE") {
		return "", true
	}
	switch key {
	case "HETEROLOG_APP_NAME", "HETEROLOG_TARGET":
		return "heterolog", true
	case "HETEROLOG_TIMEZONE_LOG":
		return "Asia/Tokyo", true
	case "HETEROLOG_CLOUD_PROVIDER":
		return "aws", true
	case "HETEROLOG_RUNTIME_TYPE":
		return "local", true
	case "HETEROLOG_INSTANCE_ID":
		return "offline-config-fixture", true
	}
	if safeNumber.MatchString(value) || safeNumberList.MatchString(value) || safeValues[value] || safeDirectory.MatchString(value) {
		return value, true
	}
	if strings.Contains(value, ",") {
		for _, part := range strings.Split(value, ",") {
			if !safeValues[part] {
				return "", false
			}
		}
		return value, true
	}
	return "", false
}
