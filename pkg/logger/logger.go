package logger

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type LogLevel int

type LogOutputDestination string
type LogOutputDestinations []LogOutputDestination

// DefaultMaxLogEntryBytesは、applicationから一回のlog呼び出しで受け付ける入力を
// 正規化JSONで表したbyte数の上限です。backend固有のmetadataやwire formatは、各backendの
// 最終検証で別に扱います。
const DefaultMaxLogEntryBytes = 1 * 1024 * 1024

// 出力先を追加する場合は、直下のallLogOutputDestinationsにも追加してください。
// parser、handler target収集、網羅テストがその正規一覧から自動的に追従します。
const (
	LogOutputFile           LogOutputDestination = "file"
	LogOutputStdout         LogOutputDestination = "stdout"
	LogOutputSlack          LogOutputDestination = "slack"
	LogOutputOpenSearch     LogOutputDestination = "opensearch"
	LogOutputLoki           LogOutputDestination = "loki"
	LogOutputCloudWatchLogs LogOutputDestination = "cloudwatchlogs"
	LogOutputClickHouse     LogOutputDestination = "clickhouse"
)

// allLogOutputDestinationsは、利用可能な出力先とその安定した列挙順の唯一の定義です。
// 出力先を追加する場合は、この一覧にも追加します。IsValid、設定のhandler target収集、
// それらを網羅するテストはすべてこの一覧を参照します。
var allLogOutputDestinations = LogOutputDestinations{
	LogOutputFile,
	LogOutputStdout,
	LogOutputSlack,
	LogOutputOpenSearch,
	LogOutputLoki,
	LogOutputCloudWatchLogs,
	LogOutputClickHouse,
}

const (
	LogLevelDebug LogLevel = iota + 1 // 1: Debug (詳細なデバッグ情報)
	LogLevelInfo                      // 2: Info (通常の情報)
	LogLevelWarn                      // 3: Warn (警告)
	LogLevelError                     // 4: Error (エラー)
	LogLevelFatal                     // 5: Fatal (致命的エラー)
)

// LogLevelToString は LogLevel から文字列へのマッピングです。
var LogLevelToString = map[LogLevel]string{
	LogLevelDebug: "DEBUG",
	LogLevelInfo:  "INFO",
	LogLevelWarn:  "WARN",
	LogLevelError: "ERROR",
	LogLevelFatal: "FATAL",
}

// LogLevelMaps は文字列から LogLevel へのマッピングです。
var LogLevelMaps = map[string]LogLevel{
	"DEBUG": LogLevelDebug,
	"INFO":  LogLevelInfo,
	"WARN":  LogLevelWarn,
	"ERROR": LogLevelError,
	"FATAL": LogLevelFatal,
}

var validLogOutputDestinations = func() map[LogOutputDestination]struct{} {
	destinations := make(map[LogOutputDestination]struct{}, len(allLogOutputDestinations))
	for _, destination := range allLogOutputDestinations {
		destinations[destination] = struct{}{}
	}
	return destinations
}()

var (
	// ErrNoDeliveryTargetsは、DeliveryReportに送信先の結果が一件もないことを表します。
	// 出力先のない設定は構築時にErrNoActiveLogOutputで拒否するため、内部の不整合でのみ生じます。
	ErrNoDeliveryTargets           = errors.New("delivery report has no target results")
	ErrTargetDeliveryNotRegistered = errors.New("target delivery was not registered")
	// ErrNoActiveLogOutputは、通常出力として起動するbackendが一つもない設定を表します。
	// LogOutputDestinationsが空ならnon-nilのbackend設定をすべて通常出力として扱うため、
	// backend設定を一つも持たない設定もこのerrorです。handler-onlyのbackendと
	// InternalLogDestinationsの専用保存先は通常出力に数えません。出力先がないとlogが
	// 黙って失われるため、sync/async/Delivery loggerは構築前にこのerrorを返します。
	ErrNoActiveLogOutput = errors.New("no active log output: configure at least one backend as a log output")
)

// String は LogLevel を文字列に変換します。
func (l LogLevel) String() string {
	if str, ok := LogLevelToString[l]; ok {
		return str
	}
	return fmt.Sprintf("UNKNOWN(%d)", l)
}

func (l LogLevel) IsValid() bool {
	_, ok := LogLevelToString[l]
	return ok
}

// ParseLogLevel は文字列から LogLevel に変換します。
// 大文字小文字と前後の空白は無視します。
func ParseLogLevel(levelStr string) (LogLevel, error) {
	normalized := strings.ToUpper(strings.TrimSpace(levelStr))
	if normalized == "" {
		return 0, errors.New("log level is empty")
	}

	if level, ok := LogLevelMaps[normalized]; ok {
		return level, nil
	}
	return 0, fmt.Errorf("invalid log level: %q", levelStr)
}

func (d LogOutputDestination) String() string {
	return string(d)
}

func (d LogOutputDestination) IsValid() bool {
	_, ok := validLogOutputDestinations[d]
	return ok
}

// AllLogOutputDestinationsは、利用可能な全出力先を定義順で返します。
// 呼出側から正規一覧を書き換えられないよう、毎回複製して返します。
func AllLogOutputDestinations() LogOutputDestinations {
	return slices.Clone(allLogOutputDestinations)
}

func ParseLogOutputDestination(value string) (LogOutputDestination, error) {
	d := LogOutputDestination(strings.ToLower(strings.TrimSpace(value)))
	if d == "" {
		return "", errors.New("log output destination is empty")
	}
	if d == "cloudwatch" {
		d = LogOutputCloudWatchLogs
	}
	if !d.IsValid() {
		return "", fmt.Errorf("unsupported log output destination: %q", value)
	}
	return d, nil
}

func ParseLogOutputDestinations(values []string) (LogOutputDestinations, error) {
	destinations := make(LogOutputDestinations, 0, len(values))
	seen := make(map[LogOutputDestination]struct{}, len(values))

	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}

		destination, err := ParseLogOutputDestination(v)
		if err != nil {
			return nil, err
		}

		if _, ok := seen[destination]; ok {
			continue
		}

		seen[destination] = struct{}{}
		destinations = append(destinations, destination)
	}

	return destinations, nil
}

func (ds LogOutputDestinations) Has(target LogOutputDestination) bool {
	for _, d := range ds {
		if d == target {
			return true
		}
	}
	return false
}

func (ds LogOutputDestinations) Strings() []string {
	values := make([]string, 0, len(ds))
	for _, d := range ds {
		values = append(values, d.String())
	}
	return values
}

func (ds LogOutputDestinations) String() string {
	return strings.Join(ds.Strings(), ",")
}

// DeliveryReport はDeliveryLoggerの実行結果です。
// 監査ログなどの停止判定ではHasUnregisteredTargetを使ってfail-closedに判定します。
type DeliveryReport struct {
	// Targets は本来の目的backendごとの結果です。
	// success、enqueue、skip、failure、unconfirmedを省略せず、loggerの設定順で保持します。
	Targets []DeliveryResult

	// Durables は、本来の目的backendへの配送失敗後に行った耐久保存の結果です。
	// archive、local file、dead letterなどへの保存成功を表しますが、
	// 本来の目的backendへの配送成功を代替するものではありません。
	Durables []DurableResult

	// Evacuations は、failure storeを使わない場合の直接退避、または
	// durable storeのoverload時に行ったspill、drop、rejectなどの結果です。
	// 退避成功も、本来の目的backendへの配送成功を意味しません。
	Evacuations []EvacuationResult
}

// DeliveryResult は単一backendに対する送信結果です。
// 複数backendの集約にはDeliveryReportを使用し、この型へ子結果を持たせません。
type DeliveryResult struct {
	// Backend はこの結果を返した単一の送信先です。
	Backend string

	// Status はこの送信先に対する結果です。
	Status DeliveryStatus

	// Err はこの送信先で発生した原因を保持します。
	// DeliveryLoggerの戻り値errorは呼び出し制御用、ErrはReport内の送信先別詳細用です。
	Err error
}

// NewTargetDeliveryReportは、resultを唯一の本来の送信先結果とするreportを返します。
// durable保存や退避の結果は含めず、それらを実行する呼び出し側が後から追加します。
func NewTargetDeliveryReport(result DeliveryResult) DeliveryReport {
	return DeliveryReport{Targets: []DeliveryResult{result}}
}

type DeliveryStatus string

const (
	// DeliverySucceeded は本来の目的地への登録・出力が確認できた状態です。
	// durableによる受け皿への保存を意味しません。
	DeliverySucceeded DeliveryStatus = "succeeded"

	// DeliveryFailed は 本来の目的地である backend への登録が明確に失敗した状態です。
	DeliveryFailed DeliveryStatus = "failed"

	// DeliveryRejected はoverloadやlocal validationなどにより、backend送信を開始せず拒否された状態です。
	DeliveryRejected DeliveryStatus = "rejected"

	// DeliverySkipped はレベル設定などにより、この backend が今回の送信対象外だった状態です。
	// backend への登録成功ではないため IsRegistered は false のままです。
	DeliverySkipped DeliveryStatus = "skipped"

	// DeliveryEnqueued はローカルの送信キューに受け付けられた状態です。
	// backend への送信完了や保存完了は未確認なので、IsRegistered は false のままです。
	DeliveryEnqueued DeliveryStatus = "enqueued"

	// DeliveryUnconfirmed は timeout などで登録成否を確認できない状態です。
	// 監査ログ用途では成功扱いしません。
	DeliveryUnconfirmed DeliveryStatus = "unconfirmed"
)

func (s DeliveryStatus) String() string {
	return string(s)
}

func (s DeliveryStatus) IsRegistered() bool {
	return s == DeliverySucceeded
}

func (s DeliveryStatus) IsFailed() bool {
	return s == DeliveryFailed || s == DeliveryRejected
}

func (s DeliveryStatus) IsPending() bool {
	return s == DeliveryEnqueued
}

func (s DeliveryStatus) IsUnconfirmed() bool {
	return s == DeliveryUnconfirmed
}

func (s DeliveryStatus) IsEnqueued() bool {
	return s.IsPending()
}

// DurableResult は、本来の目的backendへの配送失敗後に行った耐久保存の結果です。
type DurableResult struct {
	Backend string
	Kind    DurableKind
	Status  DurableStatus
	Err     error
}

type DurableKind string

const (
	DurableArchive    DurableKind = "archive"
	DurableDeadLetter DurableKind = "dead_letter"
	DurableLocalFile  DurableKind = "local_file"
)

func (k DurableKind) String() string {
	return string(k)
}

type DurableStatus string

const (
	DurableSucceeded DurableStatus = "succeeded"
	DurableFailed    DurableStatus = "failed"
	DurableSkipped   DurableStatus = "skipped"
	DurableUnknown   DurableStatus = "unknown"
)

func (s DurableStatus) String() string {
	return string(s)
}

func (s DurableStatus) IsSucceeded() bool {
	return s == DurableSucceeded
}

// EvacuationResult は、failure store外への退避またはoverload時の処置結果です。
type EvacuationResult struct {
	Kind   EvacuationKind
	Status EvacuationStatus
	Err    error
}

type EvacuationKind string

const (
	EvacuationSpilled  EvacuationKind = "spilled"
	EvacuationDropped  EvacuationKind = "dropped"
	EvacuationRejected EvacuationKind = "rejected"
)

func (k EvacuationKind) String() string {
	return string(k)
}

type EvacuationStatus string

const (
	EvacuationSucceeded EvacuationStatus = "succeeded"
	EvacuationFailed    EvacuationStatus = "failed"
	EvacuationSkipped   EvacuationStatus = "skipped"
	EvacuationUnknown   EvacuationStatus = "unknown"
)

func (s EvacuationStatus) String() string {
	return string(s)
}

func (s EvacuationStatus) IsSucceeded() bool {
	return s == EvacuationSucceeded
}

// FailedTargets は明確に失敗または拒否された送信先だけを返します。
// DeliveryUnconfirmedやDeliverySkippedは未登録ですが、明確な失敗ではないため含めません。
func (r DeliveryReport) FailedTargets() []DeliveryResult {
	targets := make([]DeliveryResult, 0)
	for _, target := range r.Targets {
		if target.Status.IsFailed() {
			targets = append(targets, target)
		}
	}
	return targets
}

// SkippedTargets は送信対象外だった送信先だけを返します。
func (r DeliveryReport) SkippedTargets() []DeliveryResult {
	targets := make([]DeliveryResult, 0)
	for _, target := range r.Targets {
		if target.Status == DeliverySkipped {
			targets = append(targets, target)
		}
	}
	return targets
}

// HasUnregisteredTarget は本来の目的 backend への登録が確認できていない場合 true を返します。
// fail-closed にするため、Targets が空の場合も true を返します。
// DeliveryEnqueued / DeliveryUnconfirmed / DeliverySkipped は成功未確認なので true です。
func (r DeliveryReport) HasUnregisteredTarget() bool {
	if len(r.Targets) == 0 {
		return true
	}

	for _, target := range r.Targets {
		if !target.Status.IsRegistered() {
			return true
		}
	}

	return false
}

// HasRegisteredTarget は少なくとも1つの送信先で登録・出力が確認できた場合trueを返します。
// 一部の送信先がskipされた場合でも、登録済み送信先の有無を判定できます。
func (r DeliveryReport) HasRegisteredTarget() bool {
	for _, target := range r.Targets {
		if target.Status.IsRegistered() {
			return true
		}
	}
	return false
}

// HasTargetFailure は本来の目的 backend への明確な登録失敗がある場合 true を返します。
// DeliveryEnqueued / DeliveryUnconfirmed / DeliverySkipped は成功ではありませんが、明確な失敗でもないため false です。
// Targets が空の report は、出力先のない設定を構築時に拒否するため内部の不整合でのみ生じます。
// TargetError が ErrNoDeliveryTargets を返すのと同じく、送信先なしの失敗として true を返します。
// fail-closed な未登録判定には HasUnregisteredTarget を使ってください。
func (r DeliveryReport) HasTargetFailure() bool {
	if len(r.Targets) == 0 {
		return true
	}
	for _, target := range r.Targets {
		if target.Status.IsFailed() {
			return true
		}
	}

	return false
}

func (r DeliveryReport) AllTargetsRegistered() bool {
	return !r.HasUnregisteredTarget()
}

func (r DeliveryReport) HasDurable() bool {
	return len(r.Durables) > 0
}

func (r DeliveryReport) HasDurableFailure() bool {
	for _, durable := range r.Durables {
		if !durable.Status.IsSucceeded() {
			return true
		}
	}
	return false
}

func (r DeliveryReport) HasEvacuation() bool {
	return len(r.Evacuations) > 0
}

func (r DeliveryReport) HasEvacuationFailure() bool {
	for _, evacuation := range r.Evacuations {
		if !evacuation.Status.IsSucceeded() {
			return true
		}
	}
	return false
}

func (r DeliveryReport) HasDrop() bool {
	for _, evacuation := range r.Evacuations {
		if evacuation.Kind == EvacuationDropped {
			return true
		}
	}
	return false
}

// HasAnyFailure はtarget、durable、evacuationのいずれかに明確な失敗があればtrueを返します。
// Targets が空の report も HasTargetFailure と同じく失敗です。HasAnyFailure が true で
// あることと、Error が nil 以外を返すことは一致します。
// 監査ログの停止判定には通常 HasUnregisteredTarget を使ってください。
func (r DeliveryReport) HasAnyFailure() bool {
	return r.HasTargetFailure() || r.HasDurableFailure() || r.HasEvacuationFailure()
}

// TargetError は target への明確な登録失敗だけを error として返します。
// DeliveryEnqueued / DeliveryUnconfirmed / DeliverySkipped は含めません。
// Targets が空の report では、HasTargetFailure と同じく ErrNoDeliveryTargets を返します。
func (r DeliveryReport) TargetError() error {
	if len(r.Targets) == 0 {
		return ErrNoDeliveryTargets
	}

	errs := make([]error, 0, len(r.Targets))
	for _, target := range r.Targets {
		if !target.Status.IsFailed() {
			continue
		}
		errs = append(errs, deliveryResultError(target))
	}

	return errors.Join(errs...)
}

// DurableError は失敗した耐久保存結果をまとめて返します。
// 耐久保存の成功・失敗は、本来の目的backendへの配送結果とは独立しています。
func (r DeliveryReport) DurableError() error {
	errs := make([]error, 0, len(r.Durables))
	for _, durable := range r.Durables {
		if durable.Status.IsSucceeded() {
			continue
		}
		errs = append(errs, durableResultError(durable))
	}

	return errors.Join(errs...)
}

// EvacuationError は失敗した退避結果をまとめて返します。
// 退避の成功・失敗は、本来の目的backendへの配送結果とは独立しています。
func (r DeliveryReport) EvacuationError() error {
	errs := make([]error, 0, len(r.Evacuations))
	for _, evacuation := range r.Evacuations {
		if evacuation.Status.IsSucceeded() {
			continue
		}
		errs = append(errs, evacuationResultError(evacuation))
	}

	return errors.Join(errs...)
}

// Error はtarget、durable、evacuationの明確な失敗をまとめて返します。
// backend 登録の fail-closed 判定には HasUnregisteredTarget を使ってください。
func (r DeliveryReport) Error() error {
	return errors.Join(
		r.TargetError(),
		r.DurableError(),
		r.EvacuationError(),
	)
}

func deliveryResultError(result DeliveryResult) error {
	if result.Err != nil {
		return fmt.Errorf(
			"%w: backend=%q status=%s: %w",
			ErrTargetDeliveryNotRegistered,
			result.Backend,
			result.Status,
			result.Err,
		)
	}
	return fmt.Errorf(
		"%w: backend=%q status=%s",
		ErrTargetDeliveryNotRegistered,
		result.Backend,
		result.Status,
	)
}

func durableResultError(result DurableResult) error {
	if result.Err != nil {
		return fmt.Errorf(
			"durable store failed: backend=%q kind=%s status=%s: %w",
			result.Backend,
			result.Kind,
			result.Status,
			result.Err,
		)
	}
	return fmt.Errorf(
		"durable store failed: backend=%q kind=%s status=%s",
		result.Backend,
		result.Kind,
		result.Status,
	)
}

func evacuationResultError(result EvacuationResult) error {
	if result.Err != nil {
		return fmt.Errorf(
			"evacuation failed: kind=%s status=%s: %w",
			result.Kind,
			result.Status,
			result.Err,
		)
	}
	return fmt.Errorf(
		"evacuation failed: kind=%s status=%s",
		result.Kind,
		result.Status,
	)
}
