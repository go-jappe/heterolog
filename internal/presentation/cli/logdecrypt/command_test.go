package logdecrypt

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
)

// realDependenciesは、cipherfileをそのまま使う依存です。bootstrapと同じ組み立てを、
// presentation層の試験の中で行います。
func realDependencies() Dependencies {
	return Dependencies{
		Keygen: func(path string) (string, string, error) {
			identity, err := cipherfile.GenerateIdentity()
			if err != nil {
				return "", "", err
			}
			if err := cipherfile.WriteIdentityFile(path, identity); err != nil {
				return "", "", err
			}
			return identity.Recipient().String(), identity.KeyID(), nil
		},
		Decrypt: func(identityPath, inputPath string, output io.Writer) (cipherfile.Report, error) {
			identities, err := cipherfile.LoadIdentityFile(identityPath)
			if err != nil {
				return cipherfile.Report{}, err
			}
			input, err := os.Open(inputPath)
			if err != nil {
				return cipherfile.Report{}, err
			}
			defer input.Close()
			return cipherfile.Decrypt(input, identities, output)
		},
	}
}

func run(t *testing.T, deps Dependencies, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(args, &stdout, &stderr, deps)
	return code, stdout.String(), stderr.String()
}

func encryptedFile(t *testing.T, path string, recipient *cipherfile.Recipient, lines ...string) {
	t.Helper()
	header, sealer, err := cipherfile.NewSealer(recipient)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(nil), header...)
	for _, line := range lines {
		chunk, err := sealer.Seal([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, chunk...)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestKeygenAndDecrypt(t *testing.T) {
	dir := t.TempDir()
	identity := filepath.Join(dir, "identity")
	code, stdout, stderr := run(t, realDependencies(), "keygen", "--identity", identity)
	if code != exitOK {
		t.Fatalf("keygen exit %d: %s", code, stderr)
	}
	var recipientText string
	for _, line := range strings.Split(stdout, "\n") {
		if rest, ok := strings.CutPrefix(line, "HETEROLOG_ENCRYPTION_RECIPIENT="); ok {
			recipientText = rest
		}
	}
	if recipientText == "" || !strings.Contains(stdout, "key id: ") {
		t.Fatalf("keygen output lacks the public key or key id:\n%s", stdout)
	}
	recipient, err := cipherfile.ParseRecipient(recipientText)
	if err != nil {
		t.Fatal(err)
	}
	// 同じfileへの2回目は、鍵を上書きしません。
	if code, _, _ := run(t, realDependencies(), "keygen", "--identity", identity); code != exitError {
		t.Fatalf("second keygen exit %d, want %d", code, exitError)
	}

	input := filepath.Join(dir, "app.log")
	encryptedFile(t, input, recipient, "one\n", "two\n")
	code, stdout, stderr = run(t, realDependencies(), "decrypt", "--identity", identity, input)
	if code != exitOK || stdout != "one\ntwo\n" {
		t.Fatalf("decrypt exit %d, stdout %q, stderr %s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "1 segments, 2 records") {
		t.Fatalf("decrypt report missing: %s", stderr)
	}

	// --outputはfileに書き、既存のfileは上書きしません。
	output := filepath.Join(dir, "app.txt")
	if code, _, stderr := run(t, realDependencies(), "decrypt", "--identity", identity, "--output", output, input); code != exitOK {
		t.Fatalf("decrypt --output exit %d: %s", code, stderr)
	}
	if contents, _ := os.ReadFile(output); string(contents) != "one\ntwo\n" {
		t.Fatalf("output file %q", contents)
	}
	if code, _, _ := run(t, realDependencies(), "decrypt", "--identity", identity, "--output", output, input); code != exitError {
		t.Fatalf("decrypt onto an existing file exit %d, want %d", code, exitError)
	}

	// --output-dirは、入力ごとにその名前のfileを作ります。
	second := filepath.Join(dir, "second.log")
	encryptedFile(t, second, recipient, "three\n")
	outDir := filepath.Join(dir, "out")
	os.Mkdir(outDir, 0o700)
	if code, _, stderr := run(t, realDependencies(), "decrypt", "--identity", identity, "--output-dir", outDir, input, second); code != exitOK {
		t.Fatalf("decrypt --output-dir exit %d: %s", code, stderr)
	}
	if contents, _ := os.ReadFile(filepath.Join(outDir, "second.log")); string(contents) != "three\n" {
		t.Fatalf("second output %q", contents)
	}
}

func TestDecryptReportsSkippedBytes(t *testing.T) {
	dir := t.TempDir()
	identity := filepath.Join(dir, "identity")
	other := filepath.Join(dir, "other")
	if code, _, stderr := run(t, realDependencies(), "keygen", "--identity", identity); code != exitOK {
		t.Fatal(stderr)
	}
	_, otherOut, _ := run(t, realDependencies(), "keygen", "--identity", other)
	otherRecipient, err := cipherfile.ParseRecipient(strings.TrimPrefix(strings.Split(strings.TrimSpace(otherOut), "\n")[2], "HETEROLOG_ENCRYPTION_RECIPIENT="))
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(dir, "foreign.log")
	encryptedFile(t, input, otherRecipient, "secret\n")
	code, stdout, stderr := run(t, realDependencies(), "decrypt", "--identity", identity, input)
	if code != exitIncomplete || stdout != "" {
		t.Fatalf("exit %d, stdout %q", code, stdout)
	}
	if !strings.Contains(stderr, "no identity for key id") || !strings.Contains(stderr, cipherfile.SkipUnknownKey) {
		t.Fatalf("report does not explain the skip: %s", stderr)
	}

	plain := filepath.Join(dir, "plain.log")
	os.WriteFile(plain, []byte("not encrypted\n"), 0o600)
	if code, _, stderr := run(t, realDependencies(), "decrypt", "--identity", identity, plain); code != exitError || !strings.Contains(stderr, cipherfile.ErrNotEncrypted.Error()) {
		t.Fatalf("plain file exit %d: %s", code, stderr)
	}
}

func TestUsageErrors(t *testing.T) {
	deps := Dependencies{
		Keygen: func(string) (string, string, error) { return "", "", errors.New("must not run") },
		Decrypt: func(string, string, io.Writer) (cipherfile.Report, error) {
			return cipherfile.Report{}, errors.New("must not run")
		},
	}
	cases := [][]string{
		{},
		{"unknown"},
		{"keygen"},
		{"keygen", "--identity", "x", "extra"},
		{"decrypt", "a.log"},
		{"decrypt", "--identity", "id"},
		{"decrypt", "--identity", "id", "--output", "o", "--output-dir", "d", "a.log"},
		{"decrypt", "--identity", "id", "a.log", "b.log"},
		{"decrypt", "--identity", "id", "--output", "o", "a.log", "b.log"},
		{"decrypt", "--bogus"},
	}
	for _, args := range cases {
		if code, _, _ := run(t, deps, args...); code != exitUsage {
			t.Errorf("Run(%q) exit %d, want %d", args, code, exitUsage)
		}
	}
	for _, args := range [][]string{{"--help"}, {"help"}, {"keygen", "--help"}, {"decrypt", "-h"}} {
		code, stdout, _ := run(t, deps, args...)
		if code != exitOK || !strings.Contains(stdout, "Usage: log-decrypt keygen") {
			t.Errorf("Run(%q) exit %d, stdout %q", args, code, stdout)
		}
	}
}
