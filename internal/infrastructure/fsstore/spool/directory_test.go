package spool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestNewRootDirBuildsRootAndStateDirs(t *testing.T) {
	t.Parallel()

	// rootは解決済みのパスを返すため、一時ディレクトリの上位のリンクを解決して比べます。
	base := resolvedTempDirForTest(t)
	dir, err := NewRootDir(base, "heterolog", "opensearch")
	if err != nil {
		t.Fatalf("NewRootDir returned error: %v", err)
	}

	wantRoot := filepath.Join(base, "heterolog", "spool", "opensearch")
	if dir.String() != wantRoot {
		t.Fatalf("Dir.String = %q, want %q", dir.String(), wantRoot)
	}

	tests := []struct {
		name  string
		got   StateDir
		state pkgspool.State
	}{
		{name: "active", got: dir.ActiveDir(), state: pkgspool.StateActive},
		{name: "processing", got: dir.ProcessingDir(), state: pkgspool.StateProcessing},
		{name: "done", got: dir.DoneDir(), state: pkgspool.StateDone},
		{name: "dead", got: dir.DeadDir(), state: pkgspool.StateDead},
		{name: "quarantine", got: dir.QuarantineDir(), state: pkgspool.StateQuarantine},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wantPath := filepath.Join(wantRoot, string(tt.state))
			if tt.got.Path() != wantPath {
				t.Fatalf("StateDir.Path = %q, want %q", tt.got.Path(), wantPath)
			}
			if tt.got.State() != tt.state {
				t.Fatalf("StateDir.State = %q, want %q", tt.got.State(), tt.state)
			}
			if tt.got.IsZero() {
				t.Fatal("StateDir.IsZero = true, want false")
			}
		})
	}
}

func TestStateDirTimePartitionDir(t *testing.T) {
	t.Parallel()

	stateDir := StateDir{path: filepath.Join("root", "active"), state: pkgspool.StateActive}

	got := stateDir.TimePartitionDir("2026", "06", "10", "23", "45")
	want := filepath.Join("root", "active", "2026", "06", "10", "23", "45")
	if got != want {
		t.Fatalf("TimePartitionDir = %q, want %q", got, want)
	}
}

func TestStateDirIsZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dir  StateDir
		want bool
	}{
		{name: "empty path", dir: StateDir{state: pkgspool.StateActive}, want: true},
		{name: "invalid state", dir: StateDir{path: "root", state: pkgspool.State("unknown")}, want: true},
		{name: "valid", dir: StateDir{path: "root", state: pkgspool.StateDone}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.dir.IsZero(); got != tt.want {
				t.Fatalf("IsZero = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewRootDirSanitizesTargetAndDirName(t *testing.T) {
	t.Parallel()

	base := resolvedTempDirForTest(t)
	dir, err := NewRootDir(base, " ../search app. ", " logs/primary:write? ")
	if err != nil {
		t.Fatalf("NewRootDir returned error: %v", err)
	}

	want := filepath.Join(base, "search_app", "spool", "logs_primary_write")
	if dir.String() != want {
		t.Fatalf("Dir.String = %q, want %q", dir.String(), want)
	}
}

func TestNewRootDirRejectsEmptyParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		spoolPath string
		target    string
		dirName   string
		wantErr   error
	}{
		{
			name:      "empty spool path",
			spoolPath: " \t ",
			target:    "target",
			dirName:   "dir",
			wantErr:   ErrEmptySpoolPath,
		},
		{
			name:      "empty target",
			spoolPath: t.TempDir(),
			target:    " --..__ ",
			dirName:   "dir",
			wantErr:   ErrEmptyTarget,
		},
		{
			name:      "empty dir",
			spoolPath: t.TempDir(),
			target:    "target",
			dirName:   " /// ",
			wantErr:   ErrEmptyDirName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewRootDir(tt.spoolPath, tt.target, tt.dirName)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestSanitizePathPart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "trims whitespace", in: "  target  ", want: "target"},
		{name: "replaces unsafe characters", in: `a/b\c:d*e?f"g<h>i|j k`, want: "a_b_c_d_e_f_g_h_i_j_k"},
		{name: "trims path-like edges", in: "._-target-_.", want: "target"},
		{name: "empty after trim", in: " ._- ", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := sanitizePathPart(tt.in); got != tt.want {
				t.Fatalf("sanitizePathPart(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNewRootDirDistinguishesInputErrorsFromResolutionErrors(t *testing.T) {
	t.Parallel()

	// 解決できないspool pathでも、空の入力は解決前に判定し、入力エラーとして判別できます。
	// 解決の失敗は入力エラーとして扱いません。
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputErrors := []error{ErrEmptySpoolPath, ErrEmptyTarget, ErrEmptyDirName}
	tests := []struct {
		name    string
		target  string
		dirName string
		wantErr error
	}{
		{name: "empty target", target: " ", dirName: "dir", wantErr: ErrEmptyTarget},
		{name: "empty dir", target: "target", dirName: " ", wantErr: ErrEmptyDirName},
		{name: "resolution failure", target: "target", dirName: "dir"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewRootDir(file, tt.target, tt.dirName)
			if err == nil {
				t.Fatal("NewRootDir returned nil error, want error")
			}
			for _, inputErr := range inputErrors {
				if got, want := errors.Is(err, inputErr), inputErr == tt.wantErr; got != want {
					t.Fatalf("errors.Is(%v, %v) = %t, want %t", err, inputErr, got, want)
				}
			}
		})
	}
}
