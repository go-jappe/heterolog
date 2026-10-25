//go:build windows

package memorypressure_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

const (
	windowsCommitTestChildEnv = "HETEROLOG_MEMORYPRESSURE_JOB_CHILD"
	windowsCommitTestBytes    = 48 << 20
	windowsCommitTestLimit    = 256 << 20
)

func TestWindowsMemoryPressureTracksCurrentCommitWithinJob(t *testing.T) {
	if scope := os.Getenv(windowsCommitTestChildEnv); scope != "" {
		runWindowsCommitTestChild(t, scope)
		return
	}
	for _, scope := range []string{"job", "process"} {
		t.Run(scope, func(t *testing.T) {
			if scope == "job" {
				runWindowsMemoryCommitChildInJob(t, "TestWindowsMemoryPressureTracksCurrentCommitWithinJob",
					scope, windowsCommitTestLimit, 0)
			} else {
				runWindowsMemoryCommitChildInJob(t, "TestWindowsMemoryPressureTracksCurrentCommitWithinJob",
					scope, 0, windowsCommitTestLimit)
			}
		})
	}
}

func TestWindowsMemoryPressureTracksBothJobAndProcessLimits(t *testing.T) {
	if scope := os.Getenv(windowsCommitTestChildEnv); scope != "" {
		runWindowsDualCommitTestChild(t, scope)
		return
	}
	for _, scope := range []string{"process", "job"} {
		t.Run(scope, func(t *testing.T) {
			jobLimit, processLimit := uintptr(windowsCommitTestLimit*4), uintptr(windowsCommitTestLimit)
			if scope == "job" {
				jobLimit, processLimit = processLimit, jobLimit
			}
			runWindowsMemoryCommitChildInJob(t, "TestWindowsMemoryPressureTracksBothJobAndProcessLimits",
				scope, jobLimit, processLimit)
		})
	}
}

func runWindowsMemoryCommitChildInJob(t *testing.T, testName, scope string, jobLimit, processLimit uintptr) {
	t.Helper()
	// 親test自身はJobへ入れず、専用の子だけに上限を設定します。
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(job)
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if jobLimit != 0 {
		limits.BasicLimitInformation.LimitFlags |= windows.JOB_OBJECT_LIMIT_JOB_MEMORY
		limits.JobMemoryLimit = jobLimit
	}
	if processLimit != 0 {
		limits.BasicLimitInformation.LimitFlags |= windows.JOB_OBJECT_LIMIT_PROCESS_MEMORY
		limits.ProcessMemoryLimit = processLimit
	}
	r1, _, err := windows.NewLazySystemDLL("kernel32.dll").NewProc("SetInformationJobObject").Call(
		uintptr(job), uintptr(windows.JobObjectExtendedLimitInformation),
		uintptr(unsafe.Pointer(&limits)), unsafe.Sizeof(limits))
	if r1 == 0 {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable,
		"-test.run=^"+testName+"$", "-test.v", "-test.timeout=20s")
	command.Env = append(os.Environ(), windowsCommitTestChildEnv+"="+scope)
	command.WaitDelay = time.Second
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false, uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	assignErr := windows.AssignProcessToJobObject(job, process)
	closeErr := windows.CloseHandle(process)
	if assignErr != nil || closeErr != nil {
		t.Fatalf("assign child to job: %v; close process handle: %v", assignErr, closeErr)
	}
	// Assign完了前に子がproviderを読む競合をstdin handshakeで防ぎます。
	if _, err := input.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	if err != nil {
		t.Fatalf("%s limit child failed: %v (context: %v)\n%s", scope, err, ctx.Err(), output.String())
	}
	t.Logf("%s limit child:\n%s", scope, output.String())
}

func runWindowsCommitTestChild(t *testing.T, scope string) {
	t.Helper()
	if scope != "job" && scope != "process" {
		t.Fatalf("unexpected child scope %q", scope)
	}
	var assigned [1]byte
	if _, err := io.ReadFull(os.Stdin, assigned[:]); err != nil || assigned[0] != 1 {
		t.Fatalf("wait for job assignment: %v, handshake=%v", err, assigned)
	}
	provider := memorypressure.New()
	readPressure := func() memorypressure.MemoryPressure {
		t.Helper()
		pressure, err := provider.ReadMemoryPressure(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !pressure.Reliable || pressure.DirectHost || !pressure.Container || pressure.LimitBytes != windowsCommitTestLimit {
			t.Fatalf("expected current usage against the assigned %s limit: %+v", scope, pressure)
		}
		if !pressure.RSSReliable {
			t.Fatalf("expected process working-set measurement: %+v", pressure)
		}
		if pressure.ProcessLimit != (memorypressure.MemoryLimitUsage{}) {
			t.Fatalf("a single limit unexpectedly gained an additional process limit: %+v", pressure)
		}
		return pressure
	}
	readPeak := func() uint64 {
		t.Helper()
		var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
		r1, _, err := windows.NewLazySystemDLL("kernel32.dll").NewProc("QueryInformationJobObject").Call(
			0, uintptr(windows.JobObjectExtendedLimitInformation),
			uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info), 0)
		if r1 == 0 {
			t.Fatal(err)
		}
		if scope == "job" {
			return uint64(info.PeakJobMemoryUsed)
		}
		return uint64(info.PeakProcessMemoryUsed)
	}

	baseline := readPressure()
	// 未参照のcommitを使い、RSSではなくcommit量を測っていることも確かめます。
	address, err := windows.VirtualAlloc(0, windowsCommitTestBytes,
		windows.MEM_RESERVE|windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if address != 0 {
			if err := windows.VirtualFree(address, 0, windows.MEM_RELEASE); err != nil {
				t.Error(err)
			}
		}
	}()
	committed := readPressure()
	committedPeak := readPeak()
	if err := windows.VirtualFree(address, 0, windows.MEM_RELEASE); err != nil {
		t.Fatal(err)
	}
	address = 0
	released := readPressure()
	releasedPeak := readPeak()
	t.Logf("scope=%s current=%d/%d/%d limit=%d peaks=%d/%d",
		scope, baseline.UsedBytes, committed.UsedBytes, released.UsedBytes,
		windowsCommitTestLimit, committedPeak, releasedPeak)

	// Go runtime等の小さな増減を許容しつつ、48MiBのcommitの大半を検出します。
	const minimumChange = windowsCommitTestBytes * 2 / 3
	if committed.UsedBytes < baseline.UsedBytes+minimumChange {
		t.Fatalf("untouched committed memory was not counted: before=%d during=%d",
			baseline.UsedBytes, committed.UsedBytes)
	}
	if committed.UsedBytes < released.UsedBytes+minimumChange {
		t.Fatalf("usage did not fall after release: during=%d after=%d",
			committed.UsedBytes, released.UsedBytes)
	}
	if committed.Ratio-released.Ratio < float64(minimumChange)/windowsCommitTestLimit {
		t.Fatalf("pressure ratio did not fall after release: during=%f after=%f",
			committed.Ratio, released.Ratio)
	}
	criticalRatio := (committed.Ratio + released.Ratio) / 2
	if !memorypressure.IsCriticalOverload(committed, criticalRatio) ||
		memorypressure.IsCriticalOverload(released, criticalRatio) {
		t.Fatal("critical overload did not clear after current usage fell")
	}
	if committed.RSSBytes > baseline.RSSBytes+windowsCommitTestBytes/4 {
		t.Fatalf("untouched allocation unexpectedly became resident: before=%d during=%d",
			baseline.RSSBytes, committed.RSSBytes)
	}
	if releasedPeak < committedPeak || releasedPeak < released.UsedBytes+minimumChange {
		t.Fatalf("expected a retained peak above current usage: peak before=%d after=%d current=%d",
			committedPeak, releasedPeak, released.UsedBytes)
	}
}

func runWindowsDualCommitTestChild(t *testing.T, scope string) {
	t.Helper()
	if scope != "job" && scope != "process" {
		t.Fatalf("unexpected child scope %q", scope)
	}
	var assigned [1]byte
	if _, err := io.ReadFull(os.Stdin, assigned[:]); err != nil || assigned[0] != 1 {
		t.Fatalf("wait for job assignment: %v, handshake=%v", err, assigned)
	}
	wantJobLimit, wantProcessLimit := uint64(windowsCommitTestLimit*4), uint64(windowsCommitTestLimit)
	if scope == "job" {
		wantJobLimit, wantProcessLimit = wantProcessLimit, wantJobLimit
	}
	provider := memorypressure.New()
	readPressure := func() memorypressure.MemoryPressure {
		t.Helper()
		p, err := provider.ReadMemoryPressure(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !p.Reliable || p.DirectHost || !p.Container || !p.RSSReliable ||
			p.LimitBytes != wantJobLimit || !p.ProcessLimit.Reliable ||
			p.ProcessLimit.LimitBytes != wantProcessLimit {
			t.Fatalf("expected both assigned limits, with the Job tuple kept intact: %+v", p)
		}
		if p.UsedBytes > p.LimitBytes || p.AvailableBytes != p.LimitBytes-p.UsedBytes ||
			p.Ratio != float64(p.UsedBytes)/float64(p.LimitBytes) {
			t.Fatalf("the primary Job tuple mixed measurements from different limits: %+v", p)
		}
		if err := p.Validate(); err != nil {
			t.Fatal(err)
		}
		return p
	}
	jobUsage := func(p memorypressure.MemoryPressure) memorypressure.MemoryLimitUsage {
		return memorypressure.MemoryLimitUsage{
			UsedBytes: p.UsedBytes, LimitBytes: p.LimitBytes, Reliable: p.Reliable,
		}
	}
	tightAndLoose := func(p memorypressure.MemoryPressure) (memorypressure.MemoryLimitUsage, memorypressure.MemoryLimitUsage) {
		if scope == "process" {
			return p.ProcessLimit, jobUsage(p)
		}
		return jobUsage(p), p.ProcessLimit
	}
	ratio := func(p memorypressure.MemoryLimitUsage) float64 {
		return float64(min(p.UsedBytes, p.LimitBytes)) / float64(p.LimitBytes)
	}
	available := func(p memorypressure.MemoryLimitUsage) uint64 {
		return p.LimitBytes - min(p.UsedBytes, p.LimitBytes)
	}

	baseline := readPressure()
	// 常駐量の増加に依存せず、両方のcommit上限へ同じ実割当を反映させます。
	address, err := windows.VirtualAlloc(0, windowsCommitTestBytes,
		windows.MEM_RESERVE|windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if address != 0 {
			if err := windows.VirtualFree(address, 0, windows.MEM_RELEASE); err != nil {
				t.Error(err)
			}
		}
	}()
	committed := readPressure()
	if err := windows.VirtualFree(address, 0, windows.MEM_RELEASE); err != nil {
		t.Fatal(err)
	}
	address = 0
	released := readPressure()

	beforeTight, beforeLoose := tightAndLoose(baseline)
	duringTight, duringLoose := tightAndLoose(committed)
	afterTight, afterLoose := tightAndLoose(released)
	const minimumChange = windowsCommitTestBytes * 2 / 3
	for _, measurements := range []struct {
		name                  string
		before, during, after memorypressure.MemoryLimitUsage
	}{
		{name: "tight", before: beforeTight, during: duringTight, after: afterTight},
		{name: "loose", before: beforeLoose, during: duringLoose, after: afterLoose},
	} {
		if measurements.during.UsedBytes < measurements.before.UsedBytes+minimumChange ||
			measurements.during.UsedBytes < measurements.after.UsedBytes+minimumChange {
			t.Fatalf("%s limit did not track commit and release: before=%+v during=%+v after=%+v",
				measurements.name, measurements.before, measurements.during, measurements.after)
		}
	}

	// 固定したruntime基礎使用量を仮定せず、片方だけが閾値を超えることを先に確認します。
	criticalRatio := (ratio(duringTight) + max(ratio(beforeTight), ratio(afterTight))) / 2
	if max(ratio(beforeLoose), max(ratio(duringLoose), ratio(afterLoose))) >= criticalRatio {
		t.Fatalf("fixture did not isolate the %s ratio limit: tight=%+v loose=%+v threshold=%f",
			scope, duringTight, duringLoose, criticalRatio)
	}
	minimumAvailable := (available(duringTight) + min(available(beforeTight), available(afterTight))) / 2
	if min(available(beforeLoose), min(available(duringLoose), available(afterLoose))) <= minimumAvailable {
		t.Fatalf("fixture did not isolate the %s available-memory limit: tight=%+v loose=%+v threshold=%d",
			scope, duringTight, duringLoose, minimumAvailable)
	}
	var ratioState, availableState memorypressure.CriticalState
	for index, p := range []memorypressure.MemoryPressure{baseline, committed, released} {
		wantCritical := index == 1
		if got := memorypressure.IsCriticalOverload(p, criticalRatio); got != wantCritical {
			t.Fatalf("%s ratio sample %d: critical=%t, want %t; pressure=%+v threshold=%f",
				scope, index, got, wantCritical, p, criticalRatio)
		}
		if got := ratioState.Evaluate(p, memorypressure.CriticalPolicy{
			CriticalRatio: criticalRatio, ConsecutiveCriticalHits: 1,
		}); got != wantCritical {
			t.Fatalf("%s ratio policy sample %d: critical=%t, want %t", scope, index, got, wantCritical)
		}
		// 使用率を無効にし、追加process上限の残量条件だけでも検出できることを確認します。
		if got := availableState.Evaluate(p, memorypressure.CriticalPolicy{
			MinAvailableBytes: minimumAvailable, ConsecutiveCriticalHits: 1,
		}); got != wantCritical {
			t.Fatalf("%s available policy sample %d: critical=%t, want %t; pressure=%+v threshold=%d",
				scope, index, got, wantCritical, p, minimumAvailable)
		}
		if !wantCritical && (ratioState.ConsecutiveHits != 0 || availableState.ConsecutiveHits != 0) {
			t.Fatalf("sample %d kept stale critical history: ratio=%+v available=%+v",
				index, ratioState, availableState)
		}
	}
	t.Logf("tight=%s before=%+v committed=%+v released=%+v ratio_threshold=%f available_threshold=%d",
		scope, beforeTight, duringTight, afterTight, criticalRatio, minimumAvailable)
}
