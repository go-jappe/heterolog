package configfixture

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/joho/godotenv"
)

func TestGenerateFixtureSanitizesFreeValues(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, ".env.example")
	const privateMarker = "private-value-do-not-export"
	body := strings.Join([]string{
		"# " + privateMarker,
		"HETEROLOG_LOG_TYPE=file,stdout",
		"HETEROLOG_APP_NAME=" + privateMarker,
		"HETEROLOG_TARGET=" + privateMarker,
		"HETEROLOG_TIMEZONE_LOG=Asia/Tokyo",
		"HETEROLOG_OPENSEARCH_PASSWORD=" + privateMarker,
		"HETEROLOG_OPENSEARCH_ENDPOINT=https://" + privateMarker + ".example",
		"HETEROLOG_OPENSEARCH_INDEX=" + privateMarker,
		"HETEROLOG_SLACK_API_KEY=" + privateMarker,
		"HETEROLOG_SLACK_NORMAL_CHANNEL=" + privateMarker,
		`HETEROLOG_LOKI_HEADERS={"Authorization":"` + privateMarker + `"}`,
		"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=" + privateMarker,
		"HETEROLOG_CLOUDWATCH_REGION=" + privateMarker,
		"HETEROLOG_LOKI_TENANT_ID=" + privateMarker,
		"HETEROLOG_INSTANCE_ID=" + privateMarker,
		"HETEROLOG_SYSTEM_LOG_DIR=" + privateMarker,
		"HETEROLOG_SYSTEM_LIB_DIR=" + privateMarker,
		"HETEROLOG_APP_STATE_DIR=" + privateMarker,
		"HETEROLOG_MAX_LOG_FILE_BACKUPS=5",
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=must_fit",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=loki_failure_archives",
		"HETEROLOG_OPENSEARCH_API_KEY_ID=",
	}, "\n")
	writeSource(t, source, body)
	output := filepath.Join(root, "generated")
	if err := Generate(source, output); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".env", ".env.example", "go.mod", "README.txt"} {
		contents, err := os.ReadFile(filepath.Join(output, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), privateMarker) {
			t.Fatalf("%s retained a private source value", name)
		}
	}
	sample, err := godotenv.Read(filepath.Join(output, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	example, err := godotenv.Read(filepath.Join(output, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"HETEROLOG_OPENSEARCH_PASSWORD":       "fixture-sample",
		"HETEROLOG_OPENSEARCH_ENDPOINT":       "http://sample.invalid:18080",
		"HETEROLOG_OPENSEARCH_INDEX":          "fixture_sample_opensearch_index",
		"HETEROLOG_MAX_LOG_FILE_BACKUPS":      "5",
		"HETEROLOG_ENABLE_LOGGING":            "true",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY": "must_fit",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR":  "loki_failure_archives",
		"HETEROLOG_OPENSEARCH_API_KEY_ID":     "",
		"HETEROLOG_LOG_TYPE":                  "file,stdout,slack,opensearch,loki",
		"HETEROLOG_SYSTEM_LOG_DIR":            "fixture-storage/logs",
		"HETEROLOG_SYSTEM_LIB_DIR":            "fixture-storage/spool",
		"HETEROLOG_APP_STATE_DIR":             "fixture-storage/state",
	} {
		if sample[key] != want {
			t.Fatalf("sample %s = %q, want %q", key, sample[key], want)
		}
	}
	if example["HETEROLOG_LOG_TYPE"] != "file,stdout,slack,opensearch,loki" || example["HETEROLOG_OPENSEARCH_ENDPOINT"] != "http://example.invalid:18080" {
		t.Fatalf("example selection or separate dummy endpoint changed: %+v", example)
	}
	if len(sample) != len(example) {
		t.Fatalf("sample keys = %d, example keys = %d", len(sample), len(example))
	}
	for key := range example {
		if _, ok := sample[key]; !ok {
			t.Fatalf("sample omitted key %s", key)
		}
	}
}

func TestGenerateFixtureProvidesStoragePathsWhenSourceOmitsThem(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, ".env.example")
	writeSource(t, source, "HETEROLOG_LOG_TYPE=stdout\n")
	output := filepath.Join(root, "generated")
	if err := Generate(source, output); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".env", ".env.example"} {
		values, err := godotenv.Read(filepath.Join(output, name))
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range fixtureStoragePaths {
			if values[key] != want {
				t.Fatalf("%s %s = %q, want fixed path %q", name, key, values[key], want)
			}
		}
	}
}

func TestGenerateFixtureRejectsUnknownValuesWithoutPrintingThem(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, ".env.example")
	const privateMarker = "unknown-private-value"
	writeSource(t, source, "NEW_SETTING="+privateMarker+"\n")
	output := filepath.Join(root, "generated")
	err := Generate(source, output)
	var unreviewed *UnreviewedKeysError
	// 値を表示しないことが要件なので、表示文に値が含まれないことも確認します。
	if !errors.As(err, &unreviewed) || !slices.Equal(unreviewed.Keys, []string{"NEW_SETTING"}) ||
		strings.Contains(err.Error(), privateMarker) {
		t.Fatalf("generation error = %v, want key-only unknown value rejection", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("failed validation created output: %v", err)
	}
}

func TestGenerateFixtureRejectsOperationalEnvInput(t *testing.T) {
	root := t.TempDir()
	// 入力fileは作成しません。読み取る前に名前で拒否されることを確認します。
	err := Generate(filepath.Join(root, ".env"), filepath.Join(root, "generated"))
	if !errors.Is(err, ErrOperationalEnvSource) {
		t.Fatalf("generation error = %v, want %v", err, ErrOperationalEnvSource)
	}
}

func TestGenerateFixtureRejectsExistingOutput(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, ".env.example")
	writeSource(t, source, "HETEROLOG_ENABLE_LOGGING=true\n")
	output := filepath.Join(root, "existing")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(output, ".env")
	writeSource(t, marker, "keep-existing-data")
	err := Generate(source, output)
	if !errors.Is(err, ErrOutputDirectoryExists) {
		t.Fatalf("generation error = %v, want %v", err, ErrOutputDirectoryExists)
	}
	contents, err := os.ReadFile(marker)
	if err != nil || string(contents) != "keep-existing-data" {
		t.Fatalf("existing output changed: contents=%q error=%v", contents, err)
	}
}

func TestGenerateFixtureRejectsDuplicateKeysAndInvalidSyntax(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       error
		wantKey    string
	}{
		{"duplicate", "HETEROLOG_ENABLE_LOGGING=true\nHETEROLOG_ENABLE_LOGGING=false\n", ErrDuplicateKey, "HETEROLOG_ENABLE_LOGGING"},
		{"syntax", `HETEROLOG_LOKI_HEADERS="unclosed-private-source`, ErrInvalidEnvironmentSyntax, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, ".env.example")
			writeSource(t, source, tc.body)
			err := Generate(source, filepath.Join(root, "generated"))
			if !errors.Is(err, tc.want) {
				t.Fatalf("generation error = %v, want %v", err, tc.want)
			}
			var duplicate *DuplicateKeyError
			if tc.wantKey != "" && (!errors.As(err, &duplicate) || duplicate.Key != tc.wantKey) {
				t.Fatalf("generation error = %v, want duplicate key %s", err, tc.wantKey)
			}
			// 元の値を表示しないことが要件なので、表示文に値が含まれないことを確認します。
			if strings.Contains(err.Error(), "unclosed-private-source") {
				t.Fatalf("generation error = %v, want %v without original value", err, tc.want)
			}
		})
	}
}

func writeSource(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
