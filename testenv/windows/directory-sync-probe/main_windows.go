// directory-sync-probe compares Windows directory flush APIs on a newly created
// test directory. It is not a power-loss test and does not change product code.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"
)

type failure struct {
	Message string `json:"message"`
	Code    uint32 `json:"windows_error_code,omitempty"`
}

type flushResult struct {
	OpenError  *failure `json:"open_error,omitempty"`
	SyncError  *failure `json:"sync_error,omitempty"`
	CloseError *failure `json:"close_error,omitempty"`
}

type observation struct {
	Mode      string      `json:"mode"`
	Iteration int         `json:"iteration"`
	Stage     string      `json:"stage"`
	Result    flushResult `json:"result"`
}

type report struct {
	WindowsVersion string        `json:"windows_version"`
	Elevated       bool          `json:"elevated"`
	FileSystem     string        `json:"filesystem"`
	Directory      string        `json:"directory"`
	FileSyncs      int           `json:"successful_file_syncs"`
	Observations   []observation `json:"observations"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: directory-sync-probe.exe NEW_TEST_DIRECTORY")
	}
	directory, err := filepath.Abs(os.Args[1])
	if err != nil {
		return err
	}
	// Refuse existing targets, including previous test runs. Each removal below
	// affects only a file this invocation creates; test directories are retained.
	if err := os.Mkdir(directory, 0700); err != nil {
		return fmt.Errorf("create new test directory: %w", err)
	}
	root, err := windows.UTF16PtrFromString(filepath.VolumeName(directory) + `\`)
	if err != nil {
		return err
	}
	var filesystem [256]uint16
	if err := windows.GetVolumeInformation(root, nil, 0, nil, nil, nil, &filesystem[0], uint32(len(filesystem))); err != nil {
		return fmt.Errorf("identify filesystem: %w", err)
	}
	version := windows.RtlGetVersion()
	results := report{
		WindowsVersion: fmt.Sprintf("%d.%d.%d", version.MajorVersion, version.MinorVersion, version.BuildNumber),
		Elevated:       windows.GetCurrentProcessToken().IsElevated(),
		FileSystem:     windows.UTF16ToString(filesystem[:]),
		Directory:      directory,
	}
	modes := []struct {
		name   string
		access uint32
		flags  uint32
	}{
		{name: "os_open_readonly"},
		{name: "native_readonly", access: windows.GENERIC_READ},
		{name: "native_write", access: windows.GENERIC_WRITE},
		{name: "native_readwrite", access: windows.GENERIC_READ | windows.GENERIC_WRITE},
		{name: "native_readwrite_write_through", access: windows.GENERIC_READ | windows.GENERIC_WRITE, flags: windows.FILE_FLAG_WRITE_THROUGH},
	}
	for _, mode := range modes {
		for iteration := 1; iteration <= 10; iteration++ {
			trial := filepath.Join(directory, fmt.Sprintf("%s-%02d", mode.name, iteration))
			if err := os.Mkdir(trial, 0700); err != nil {
				return err
			}
			source := filepath.Join(trial, "source.txt")
			pending := filepath.Join(trial, "source.delete-pending.txt")
			payload := []byte(fmt.Sprintf("directory flush probe: %s, iteration %d\n", mode.name, iteration))
			if err := writeSyncedFile(source, payload); err != nil {
				return err
			}
			results.FileSyncs++
			observe := func(stage string) {
				var result flushResult
				if mode.name == "os_open_readonly" {
					result = flushUsingOSOpen(trial)
				} else {
					result = flushUsingNativeHandle(trial, mode.access, mode.flags)
				}
				results.Observations = append(results.Observations, observation{mode.name, iteration, stage, result})
			}
			observe("after_file_create")
			// Continue after a failed flush to record OS API behavior at every
			// stage. This diagnostic is NOT the production commit protocol.
			if err := os.Rename(source, pending); err != nil {
				return fmt.Errorf("rename probe file: %w", err)
			}
			content, err := os.ReadFile(pending)
			// この判定と後続の2つの存在確認では、失敗時にerrがnil（内容の不一致、fileが残る）の場合が
			// あります。%wはnilを"%!w(<nil>)"と表示して文言が変わるため、原因は%vのまま表示だけにします。
			if err != nil || !bytes.Equal(content, payload) {
				return fmt.Errorf("renamed file content mismatch or read failure: %v", err)
			}
			if _, err := os.Lstat(source); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("source should be absent after rename, got: %v", err)
			}
			observe("after_file_rename")
			if err := os.Remove(pending); err != nil {
				return fmt.Errorf("remove probe file: %w", err)
			}
			if _, err := os.Lstat(pending); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("pending file should be absent after remove, got: %v", err)
			}
			observe("after_file_remove")
		}
	}
	encoded, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	if err := writeSyncedFile(filepath.Join(directory, "results.json"), append(encoded, '\n')); err != nil {
		return err
	}
	// Exit zero means the comparison completed, not that directory flushes
	// succeeded. Each API outcome is recorded independently in results.json.
	fmt.Printf("Probe complete: elevated=%t filesystem=%s file_syncs=%d observations=%d\n", results.Elevated, results.FileSystem, results.FileSyncs, len(results.Observations))
	for _, mode := range modes {
		var succeeded, failed int
		for _, observation := range results.Observations {
			if observation.Mode != mode.name {
				continue
			}
			result := observation.Result
			if result.OpenError == nil && result.SyncError == nil && result.CloseError == nil {
				succeeded++
			} else {
				failed++
			}
		}
		fmt.Printf("%s: flush_success=%d flush_failure=%d\n", mode.name, succeeded, failed)
	}
	return nil
}

func writeSyncedFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	var syncErr error
	if writeErr == nil {
		syncErr = file.Sync()
	}
	return errors.Join(writeErr, syncErr, file.Close())
}

func flushUsingOSOpen(directory string) flushResult {
	file, err := os.Open(directory)
	if err != nil {
		return flushResult{OpenError: describe(err)}
	}
	return flushResult{SyncError: describe(file.Sync()), CloseError: describe(file.Close())}
}

func flushUsingNativeHandle(directory string, access, additionalFlags uint32) flushResult {
	path, err := windows.UTF16PtrFromString(directory)
	if err != nil {
		return flushResult{OpenError: describe(err)}
	}
	handle, err := windows.CreateFile(path, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|additionalFlags, 0)
	if err != nil {
		return flushResult{OpenError: describe(err)}
	}
	return flushResult{SyncError: describe(windows.FlushFileBuffers(handle)), CloseError: describe(windows.CloseHandle(handle))}
}

func describe(err error) *failure {
	if err == nil {
		return nil
	}
	result := &failure{Message: err.Error()}
	var code syscall.Errno
	if errors.As(err, &code) {
		result.Code = uint32(code)
	}
	return result
}
