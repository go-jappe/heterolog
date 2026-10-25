package logger

import (
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	gconstant "go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewTargetDeliveryReport(t *testing.T) {
	want := DeliveryResult{
		Backend: "opensearch:primary",
		Status:  DeliveryFailed,
		Err:     errors.New("send failed"),
	}

	report := NewTargetDeliveryReport(want)
	if len(report.Targets) != 1 || report.Targets[0] != want {
		t.Fatalf("targets = %+v, want only %+v", report.Targets, want)
	}
	if report.Durables != nil {
		t.Fatalf("durables = %+v, want nil", report.Durables)
	}
	if report.Evacuations != nil {
		t.Fatalf("evacuations = %+v, want nil", report.Evacuations)
	}
}

func TestParseLogLevelNormalizesCaseAndWhitespace(t *testing.T) {
	got, err := ParseLogLevel(" \tWaRn\n")
	if err != nil {
		t.Fatalf("ParseLogLevel() error = %v", err)
	}
	if got != LogLevelWarn {
		t.Fatalf("ParseLogLevel() = %q, want %q", got, LogLevelWarn)
	}
}

func TestParseLogLevelRejectsUnknownValue(t *testing.T) {
	if _, err := ParseLogLevel("verbose"); err == nil {
		t.Fatal("ParseLogLevel() error = nil, want invalid log level error")
	}
}

func TestParseLogOutputDestinationSupportsCloudWatchLogs(t *testing.T) {
	tests := []string{"cloudwatchlogs", "cloudwatch", " CloudWatchLogs "}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			got, err := ParseLogOutputDestination(tt)
			if err != nil {
				t.Fatalf("ParseLogOutputDestination() error = %v", err)
			}
			if got != LogOutputCloudWatchLogs {
				t.Fatalf("destination = %q, want %q", got, LogOutputCloudWatchLogs)
			}
		})
	}
}

func TestAllLogOutputDestinationsReturnsEveryValidDestinationWithoutSharingStorage(
	t *testing.T,
) {
	destinations := AllLogOutputDestinations()
	if len(destinations) == 0 {
		t.Fatal("AllLogOutputDestinations() is empty")
	}

	seen := make(map[LogOutputDestination]struct{}, len(destinations))
	for _, destination := range destinations {
		if !destination.IsValid() {
			t.Fatalf("destination %q from canonical list is invalid", destination)
		}
		if _, duplicate := seen[destination]; duplicate {
			t.Fatalf("canonical destination list contains duplicate %q", destination)
		}
		seen[destination] = struct{}{}

		parsed, err := ParseLogOutputDestination(destination.String())
		if err != nil {
			t.Fatalf("parse canonical destination %q: %v", destination, err)
		}
		if parsed != destination {
			t.Fatalf("parsed destination = %q, want %q", parsed, destination)
		}
	}

	first := destinations[0]
	destinations[0] = LogOutputDestination("mutated-by-caller")
	if got := AllLogOutputDestinations()[0]; got != first {
		t.Fatalf("canonical destination list was mutated through returned slice: got %q, want %q", got, first)
	}
}

// TestAllLogOutputDestinationsContainsEveryDeclaredConstantは、新しいdestination定数だけを
// 追加してcanonical listの更新を忘れる事故を検出します。internal/config側の
// failure store catalog網羅試験はこのcanonical listを基準にするため、その入口自体の
// 完全性をここで保証します。同じ値を持つalias定数は、canonical listの一つの要素に
// 対応します。
func TestAllLogOutputDestinationsContainsEveryDeclaredConstant(t *testing.T) {
	declared := declaredLogOutputDestinationConstants(t)
	canonical := AllLogOutputDestinations()
	canonicalCounts := make(map[LogOutputDestination]int, len(canonical))
	for _, destination := range canonical {
		canonicalCounts[destination]++
	}

	for destination, constantNames := range declared {
		if got := canonicalCounts[destination]; got != 1 {
			t.Fatalf(
				"declared destination constants %s=%q occur %d times in allLogOutputDestinations, want 1",
				strings.Join(constantNames, ", "),
				destination,
				got,
			)
		}
	}
	for destination := range canonicalCounts {
		if _, exists := declared[destination]; !exists {
			t.Fatalf(
				"allLogOutputDestinations contains %q without a declared LogOutputDestination constant",
				destination,
			)
		}
	}
}

// declaredLogOutputDestinationConstantsは、このpackageでcompileされるsourceから
// LogOutputDestination型の定数を値ごとに集めます。
//
// go testはpackage directoryでtestを実行するため、-trimpathでsource pathが記録されない
// 場合も"."から読み込めます。build constraintは現在のbuild contextで評価し、定数の型と値は
// go/typesで判定します。そのため名前の接頭辞ではなく宣言された型で判定し、型変換や他の定数を
// 参照するalias定数も扱えます。importしたpackageの内容はこれらの定数の判定に不要なので、
// 読み込みに失敗させ、それに伴うtype errorは無視します。
func declaredLogOutputDestinationConstants(t *testing.T) map[LogOutputDestination][]string {
	t.Helper()
	buildPackage, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatalf("resolve logger package source files: %v", err)
	}
	fileSet := token.NewFileSet()
	files := make([]*ast.File, 0, len(buildPackage.GoFiles))
	for _, name := range buildPackage.GoFiles {
		file, err := parser.ParseFile(fileSet, filepath.Join(buildPackage.Dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse logger package source %s: %v", name, err)
		}
		files = append(files, file)
	}
	config := types.Config{
		Importer: unavailableImporter{},
		Error:    func(error) {},
	}
	checked, _ := config.Check(buildPackage.ImportPath, fileSet, files, nil)
	destinationType, ok := checked.Scope().Lookup("LogOutputDestination").(*types.TypeName)
	if !ok {
		t.Fatal("LogOutputDestination type not found in pkg/logger")
	}

	declared := make(map[LogOutputDestination][]string)
	for _, name := range checked.Scope().Names() {
		constant, ok := checked.Scope().Lookup(name).(*types.Const)
		if !ok || !types.Identical(constant.Type(), destinationType.Type()) {
			continue
		}
		if constant.Val().Kind() != gconstant.String {
			t.Fatalf("destination constant %s must have a constant string value, got %s", name, constant.Val())
		}
		destination := LogOutputDestination(gconstant.StringVal(constant.Val()))
		declared[destination] = append(declared[destination], name)
	}
	if len(declared) == 0 {
		t.Fatal("no LogOutputDestination constants found in pkg/logger")
	}
	return declared
}

// unavailableImporterは、定数の型検査に不要なimportを読み込まずに失敗させます。
// go/typesは失敗したimportを空のpackageとして扱い、残りの宣言の検査を続けます。
type unavailableImporter struct{}

func (unavailableImporter) Import(path string) (*types.Package, error) {
	return nil, fmt.Errorf("import %s is not needed to evaluate logger constants", path)
}

func TestDeliveryStatusClassification(t *testing.T) {
	tests := []struct {
		name        string
		status      DeliveryStatus
		registered  bool
		failed      bool
		pending     bool
		unconfirmed bool
		enqueued    bool
	}{
		{
			name:       "succeeded",
			status:     DeliverySucceeded,
			registered: true,
		},
		{
			name:   "failed",
			status: DeliveryFailed,
			failed: true,
		},
		{
			name:   "rejected",
			status: DeliveryRejected,
			failed: true,
		},
		{
			name:   "skipped",
			status: DeliverySkipped,
		},
		{
			name:     "enqueued",
			status:   DeliveryEnqueued,
			pending:  true,
			enqueued: true,
		},
		{
			name:        "unconfirmed",
			status:      DeliveryUnconfirmed,
			unconfirmed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsRegistered(); got != tt.registered {
				t.Fatalf("IsRegistered() = %v, want %v", got, tt.registered)
			}
			if got := tt.status.IsFailed(); got != tt.failed {
				t.Fatalf("IsFailed() = %v, want %v", got, tt.failed)
			}
			if got := tt.status.IsPending(); got != tt.pending {
				t.Fatalf("IsPending() = %v, want %v", got, tt.pending)
			}
			if got := tt.status.IsUnconfirmed(); got != tt.unconfirmed {
				t.Fatalf("IsUnconfirmed() = %v, want %v", got, tt.unconfirmed)
			}
			if got := tt.status.IsEnqueued(); got != tt.enqueued {
				t.Fatalf("IsEnqueued() = %v, want %v", got, tt.enqueued)
			}
		})
	}
}

func TestDeliveryReportTargetClassificationSeparatesUnregisteredFromFailure(t *testing.T) {
	tests := []struct {
		name              string
		report            DeliveryReport
		unregistered      bool
		targetFailure     bool
		allRegistered     bool
		targetErrorIsNil  bool
		targetErrorTarget error
	}{
		{
			// 出力先なしは構築時に拒否するため、空のreportは内部の不整合です。
			// TargetErrorと同じく送信先なしの失敗として扱います。
			name:              "no targets is unregistered and a failure",
			report:            DeliveryReport{},
			unregistered:      true,
			targetFailure:     true,
			targetErrorTarget: ErrNoDeliveryTargets,
		},
		{
			name: "succeeded is registered",
			report: DeliveryReport{Targets: []DeliveryResult{
				{Backend: "file", Status: DeliverySucceeded},
			}},
			allRegistered:    true,
			targetErrorIsNil: true,
		},
		{
			name: "enqueued is unregistered but not explicit failure",
			report: DeliveryReport{Targets: []DeliveryResult{
				{Backend: "clickhouse", Status: DeliveryEnqueued},
			}},
			unregistered:     true,
			targetErrorIsNil: true,
		},
		{
			name: "unconfirmed is unregistered but not explicit failure",
			report: DeliveryReport{Targets: []DeliveryResult{
				{Backend: "future", Status: DeliveryUnconfirmed},
			}},
			unregistered:     true,
			targetErrorIsNil: true,
		},
		{
			name: "skipped is unregistered but not explicit failure",
			report: DeliveryReport{Targets: []DeliveryResult{
				{Backend: "slack", Status: DeliverySkipped},
			}},
			unregistered:     true,
			targetErrorIsNil: true,
		},
		{
			name: "failed is unregistered and explicit failure",
			report: DeliveryReport{Targets: []DeliveryResult{
				{Backend: "opensearch", Status: DeliveryFailed},
			}},
			unregistered:      true,
			targetFailure:     true,
			targetErrorTarget: ErrTargetDeliveryNotRegistered,
		},
		{
			name: "rejected is unregistered and explicit failure",
			report: DeliveryReport{Targets: []DeliveryResult{
				{Backend: "stdout", Status: DeliveryRejected},
			}},
			unregistered:      true,
			targetFailure:     true,
			targetErrorTarget: ErrTargetDeliveryNotRegistered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.report.HasUnregisteredTarget(); got != tt.unregistered {
				t.Fatalf("HasUnregisteredTarget() = %v, want %v", got, tt.unregistered)
			}
			if got := tt.report.HasTargetFailure(); got != tt.targetFailure {
				t.Fatalf("HasTargetFailure() = %v, want %v", got, tt.targetFailure)
			}
			if got := tt.report.HasAnyFailure(); got != tt.targetFailure {
				t.Fatalf("HasAnyFailure() = %v, want %v", got, tt.targetFailure)
			}
			if got := tt.report.AllTargetsRegistered(); got != tt.allRegistered {
				t.Fatalf("AllTargetsRegistered() = %v, want %v", got, tt.allRegistered)
			}

			err := tt.report.TargetError()
			reportErr := tt.report.Error()
			if tt.targetErrorIsNil {
				if err != nil {
					t.Fatalf("TargetError() = %v, want nil", err)
				}
				if reportErr != nil {
					t.Fatalf("Error() = %v, want nil", reportErr)
				}
				return
			}
			if !errors.Is(err, tt.targetErrorTarget) {
				t.Fatalf("TargetError() = %v, want errors.Is(..., %v)", err, tt.targetErrorTarget)
			}
			if !errors.Is(reportErr, tt.targetErrorTarget) {
				t.Fatalf("Error() = %v, want errors.Is(..., %v)", reportErr, tt.targetErrorTarget)
			}
		})
	}
}

func TestDeliveryReportDurableSuccessDoesNotRegisterTarget(t *testing.T) {
	report := DeliveryReport{
		Durables: []DurableResult{
			{
				Backend: "opensearch",
				Kind:    DurableArchive,
				Status:  DurableSucceeded,
			},
		},
	}

	if !report.HasDurable() {
		t.Fatal("HasDurable() = false, want true")
	}
	if report.HasDurableFailure() {
		t.Fatal("HasDurableFailure() = true, want false")
	}
	if report.HasRegisteredTarget() {
		t.Fatal("HasRegisteredTarget() = true, want false")
	}
	if !report.HasUnregisteredTarget() {
		t.Fatal("HasUnregisteredTarget() = false, want true")
	}
	// 耐久保存に成功しても送信先の結果がないreportは、Errorと同じく失敗です。
	if !report.HasTargetFailure() || !report.HasAnyFailure() {
		t.Fatalf(
			"HasTargetFailure() = %v, HasAnyFailure() = %v, want true for a report without targets",
			report.HasTargetFailure(),
			report.HasAnyFailure(),
		)
	}
	if !errors.Is(report.Error(), ErrNoDeliveryTargets) {
		t.Fatalf("Error() = %v, want ErrNoDeliveryTargets", report.Error())
	}
}

func TestDeliveryReportAggregatesDurableAndEvacuationFailuresIndependentlyFromTarget(t *testing.T) {
	durableErr := errors.New("archive failed")
	evacuationErr := errors.New("spill failed")
	report := DeliveryReport{
		Targets: []DeliveryResult{
			{Backend: "file", Status: DeliverySucceeded},
		},
		Durables: []DurableResult{
			{Backend: "loki", Kind: DurableLocalFile, Status: DurableSucceeded},
			{Backend: "opensearch", Kind: DurableArchive, Status: DurableFailed, Err: durableErr},
		},
		Evacuations: []EvacuationResult{
			{Kind: EvacuationDropped, Status: EvacuationSucceeded},
			{Kind: EvacuationSpilled, Status: EvacuationFailed, Err: evacuationErr},
		},
	}

	if !report.AllTargetsRegistered() {
		t.Fatal("AllTargetsRegistered() = false, want true")
	}
	if report.HasTargetFailure() {
		t.Fatal("HasTargetFailure() = true, want false")
	}
	if !report.HasDurable() || !report.HasDurableFailure() {
		t.Fatalf(
			"HasDurable() = %v, HasDurableFailure() = %v, want true, true",
			report.HasDurable(),
			report.HasDurableFailure(),
		)
	}
	if !report.HasEvacuation() || !report.HasEvacuationFailure() {
		t.Fatalf(
			"HasEvacuation() = %v, HasEvacuationFailure() = %v, want true, true",
			report.HasEvacuation(),
			report.HasEvacuationFailure(),
		)
	}
	if !report.HasDrop() {
		t.Fatal("HasDrop() = false, want true")
	}
	if !report.HasAnyFailure() {
		t.Fatal("HasAnyFailure() = false, want true")
	}
	if err := report.TargetError(); err != nil {
		t.Fatalf("TargetError() = %v, want nil", err)
	}
	if !errors.Is(report.DurableError(), durableErr) {
		t.Fatalf("DurableError() = %v, want durable cause", report.DurableError())
	}
	if !errors.Is(report.EvacuationError(), evacuationErr) {
		t.Fatalf("EvacuationError() = %v, want evacuation cause", report.EvacuationError())
	}
	if !errors.Is(report.Error(), durableErr) {
		t.Fatalf("Error() = %v, want durable cause", report.Error())
	}
	if !errors.Is(report.Error(), evacuationErr) {
		t.Fatalf("Error() = %v, want evacuation cause", report.Error())
	}
}

func TestDeliveryReportDerivesFailedAndSkippedTargets(t *testing.T) {
	report := DeliveryReport{Targets: []DeliveryResult{
		{Backend: "file", Status: DeliverySucceeded},
		{Backend: "loki", Status: DeliveryFailed},
		{Backend: "slack", Status: DeliverySkipped},
		{Backend: "opensearch", Status: DeliveryUnconfirmed},
	}}
	if !report.HasRegisteredTarget() {
		t.Fatal("HasRegisteredTarget() = false, want true")
	}

	failed := report.FailedTargets()
	if len(failed) != 1 || failed[0].Backend != "loki" {
		t.Fatalf("FailedTargets() = %#v, want loki only", failed)
	}

	skipped := report.SkippedTargets()
	if len(skipped) != 1 || skipped[0].Backend != "slack" {
		t.Fatalf("SkippedTargets() = %#v, want slack only", skipped)
	}
}

func TestRetryClientSelectionPolicyWithBreakerUsesBothWrappers(t *testing.T) {
	selection := OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: RetryClientModeRetryPolicyWithBreaker,
	}.RetryClientSelection()

	if !selection.UseRetryPolicyClient {
		t.Fatalf("UseRetryPolicyClient = false, want true")
	}
	if selection.UseRetryingClient {
		t.Fatalf("UseRetryingClient = true, want false when retry_policy_with_breaker mode is selected")
	}
	if !selection.UseCircuitBreakerClient {
		t.Fatalf("UseCircuitBreakerClient = false, want true when retry_policy_with_breaker mode is selected")
	}
}

func TestRetryClientSelectionNonPolicyCanCombineRetryingAndCircuitBreaker(t *testing.T) {
	selection := LokiRetryableClientCaseTransportErrorConfig{
		RetryClientMode: RetryClientModeRetryWithBreaker,
	}.RetryClientSelection()

	if selection.UseRetryPolicyClient {
		t.Fatalf("UseRetryPolicyClient = true, want false")
	}
	if !selection.UseRetryingClient {
		t.Fatalf("UseRetryingClient = false, want true")
	}
	if !selection.UseCircuitBreakerClient {
		t.Fatalf("UseCircuitBreakerClient = false, want true")
	}
}

func TestRetryClientSelectionModes(t *testing.T) {
	tests := []struct {
		mode        RetryClientMode
		wantRetry   bool
		wantPolicy  bool
		wantBreaker bool
		wantWrapper bool
	}{
		{mode: RetryClientModeNone},
		{mode: RetryClientModeRetry, wantRetry: true, wantWrapper: true},
		{mode: RetryClientModeRetryPolicy, wantPolicy: true, wantWrapper: true},
		{mode: RetryClientModeCircuitBreaker, wantBreaker: true, wantWrapper: true},
		{mode: RetryClientModeRetryWithBreaker, wantRetry: true, wantBreaker: true, wantWrapper: true},
		{mode: RetryClientModeRetryPolicyWithBreaker, wantPolicy: true, wantBreaker: true, wantWrapper: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			selection := OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: tt.mode,
			}.RetryClientSelection()

			if selection.UseRetryingClient != tt.wantRetry {
				t.Fatalf("UseRetryingClient = %t, want %t", selection.UseRetryingClient, tt.wantRetry)
			}
			if selection.UseRetryPolicyClient != tt.wantPolicy {
				t.Fatalf("UseRetryPolicyClient = %t, want %t", selection.UseRetryPolicyClient, tt.wantPolicy)
			}
			if selection.UseCircuitBreakerClient != tt.wantBreaker {
				t.Fatalf("UseCircuitBreakerClient = %t, want %t", selection.UseCircuitBreakerClient, tt.wantBreaker)
			}
			if selection.UsesWrapper() != tt.wantWrapper {
				t.Fatalf("UsesWrapper() = %t, want %t", selection.UsesWrapper(), tt.wantWrapper)
			}
		})
	}
}

func TestParseRetryClientModePreservesInvalidValueForValidation(t *testing.T) {
	defaultMode := RetryClientModeRetryPolicyWithBreaker
	for _, value := range []string{"off", "disabled", "disable", "retryable"} {
		t.Run(value, func(t *testing.T) {
			got := ParseRetryClientMode(value, defaultMode)
			if got != RetryClientMode(value) {
				t.Fatalf("ParseRetryClientMode(%q) = %q, want preserved value %q", value, got, value)
			}
			if got.IsValid() {
				t.Fatalf("ParseRetryClientMode(%q).IsValid() = true, want false", value)
			}
		})
	}
}

func TestDeliveryReportErrorPreservesTargetCause(t *testing.T) {
	targetErr := errors.New("target failed")
	report := DeliveryReport{Targets: []DeliveryResult{
		{
			Backend: "loki",
			Status:  DeliveryFailed,
			Err:     targetErr,
		},
	}}

	err := report.TargetError()
	if !errors.Is(err, ErrTargetDeliveryNotRegistered) {
		t.Fatalf("TargetError() = %v, want ErrTargetDeliveryNotRegistered", err)
	}
	if !errors.Is(report.Error(), ErrTargetDeliveryNotRegistered) {
		t.Fatalf("Error() = %v, want ErrTargetDeliveryNotRegistered", report.Error())
	}
	if !errors.Is(report.Error(), targetErr) {
		t.Fatalf("Error() = %v, want target cause", report.Error())
	}
}
