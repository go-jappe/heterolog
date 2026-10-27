// Package sloghandlerは、標準libraryのlog/slogから届くlogの入力窓口です。slogの
// recordをslog.JSONHandlerと同じ規則で構造化fieldにして、application層のLoggerへ
// 渡します。
package sloghandler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Handlerは、標準libraryの構造化loggerを、application層のLoggerにつなぎます。派生した
// handlerは同じ出力先を共有します。出力先のruntimeを閉じるのは呼び出し元の責任です。
// 出力先は同時の呼び出しに対応している必要があります。WithAttrsとWithGroupは、何も
// 変わらない場合はhを、変わる場合は新しい*Handlerを返します。
type Handler struct {
	logger pkglogger.Logger
	opts   slog.HandlerOptions
	groups []string
	bound  []attributeBatch
	err    error
}

type attributeBatch struct {
	groups []string
	attrs  map[string]any
}

var _ slog.Handler = (*Handler)(nil)

// Newは、slog.Newに渡すhandlerを返します。optsがnilの場合はslogの既定値（Info level、
// sourceなし）を使います。loggerがnilの場合はpanicします。
//
// attributeは、slog.JSONHandlerと同じ符号化とReplaceAttrの規則に従います。time・level・
// msgと、指定した場合のsourceを含むslogのrecord全体を、出力先の構造化fieldで
// 参照できます。重複したkeyは最後の値を残します。backendへの振り分けには元のrecordの
// levelを使い、backendの事象の時刻にも元のrecordの時刻を使います。ReplaceAttrは
// 構造化fieldとbackendのmessageを変えますが、振り分けと事象の時刻は変えません。
// 取り除いたmessageは空になり、文字列でないmessageはJSONの符号化を使います。最上位の
// 文字列のrequest_id・trace_id・span_idは、事象のmetadataになります。文字列でない値は、
// contextのIDとは別に、通常の構造化fieldとして残ります。
//
// slog.LoggerはHandler.Handleが返すerrorを捨てます。errorを受け取るにはloggerの
// 記録用のAPIを、出力先ごとの送信結果を受け取るにはDelivery loggerを使います。
func New(logger pkglogger.Logger, opts *slog.HandlerOptions) *Handler {
	if logger == nil || isNilLogger(logger) {
		panic("heterolog: NewSlogHandler requires a non-nil logger")
	}
	h := &Handler{logger: logger}
	if opts != nil {
		h.opts = *opts
	}
	return h
}

func isNilLogger(logger pkglogger.Logger) bool {
	value := reflect.ValueOf(logger)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Enabledは、levelが設定した最低levelに達しているかを返します。optionsのLevelVarは
// 呼び出しのたびに参照するため、handlerを作り直さずにlevelを変えられます。各backendは、
// さらに独自のlevelで絞り込むことがあります。
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	minimum := slog.LevelInfo
	if h.opts.Level != nil {
		minimum = h.opts.Level.Level()
	}
	return level >= minimum
}

// Handleは、recordを出力先へ送り、送信のerrorを返します。標準のhandlerと同じく、
// Handle自体はEnabledを適用しません。contextの値（traceとrequestのIDを含む）は
// 引き継ぎ、contextのcancelでlogの記録を取りやめることはありません。
func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	if h.err != nil {
		return h.err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = applogging.WithSlogRecordTime(context.WithoutCancel(ctx), record.Time)

	builtins := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	fields, message, err := h.encodeBuiltins(builtins)
	if err != nil {
		return err
	}
	groupTargets := []map[string]any{fields}
	for _, batch := range h.bound {
		addAttributes(&groupTargets, batch.groups, batch.attrs)
	}
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})
	values, err := h.encodeAttributes(attrs)
	if err != nil {
		return err
	}
	addAttributes(&groupTargets, h.groups, values)

	// 順序を固定し、fieldのsnapshotと大きさの検証の結果を毎回同じにします。
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	output := make([]pkglogger.Field, 0, len(keys))
	for _, key := range keys {
		field, err := pkglogger.NewSlogField(key, fields[key])
		if err != nil {
			return err
		}
		output = append(output, field)
	}

	switch {
	case record.Level < slog.LevelInfo:
		return h.logger.Debug(ctx, message, output...)
	case record.Level < slog.LevelWarn:
		return h.logger.Info(ctx, message, output...)
	case record.Level < slog.LevelError:
		return h.logger.Warn(ctx, message, output...)
	default:
		return h.logger.Error(ctx, message, output...)
	}
}

// WithAttrsは、今のgroupにattrsのsnapshotを持つ、独立したhandlerを返します。
// LogValuerとReplaceAttrは、ここで一度だけ評価します。
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	clone := *h
	values, err := h.encodeAttributes(attrs)
	if err != nil {
		clone.err = err
		return &clone
	}
	if len(values) == 0 {
		return h
	}
	clone.bound = append(slices.Clone(h.bound), attributeBatch{
		groups: slices.Clone(h.groups),
		attrs:  values,
	})
	return &clone
}

// WithGroupは、以後のattributeがnameに属する、独立したhandlerを返します。空のnameは
// 何も変えません。
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := *h
	clone.groups = append(slices.Clone(h.groups), name)
	return &clone
}

// encodeBuiltinsは、符号化した置き換え後の値から、backendのmessageも取り出します。
// 正規化したJSONを読むことで、元のmessageへ戻したり、JSONの符号化で省いたfieldを
// 漏らし得るStringのmethodを呼んだりすることを避けます。
func (h *Handler) encodeBuiltins(record slog.Record) (map[string]any, string, error) {
	opts := h.opts
	messageKey := slog.MessageKey
	keepMessage := true
	if replace := opts.ReplaceAttr; replace != nil {
		opts.ReplaceAttr = func(groups []string, attr slog.Attr) slog.Attr {
			replacement := replace(groups, attr)
			if len(groups) == 0 && attr.Key == slog.MessageKey {
				messageKey = replacement.Key
				keepMessage = !replacement.Equal(slog.Attr{})
			}
			return replacement
		}
	}
	fields, err := encodeRecord(record, &opts)
	if err != nil {
		return nil, "", err
	}
	if !keepMessage {
		return fields, "", nil
	}
	value, exists := fields[messageKey]
	if !exists {
		return fields, "", nil
	}
	if message, ok := value.(string); ok {
		return fields, message, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("heterolog: encode replaced slog message: %w", err)
	}
	return fields, string(encoded), nil
}

// encodeAttributesは、値の解決とJSONの意味づけを標準のhandlerに任せます。内部用の
// 外側のgroupでattributeを組み込みの項目と分け、ReplaceAttrへgroupのpathを渡す前に
// そのgroupを取り除きます。
func (h *Handler) encodeAttributes(attrs []slog.Attr) (map[string]any, error) {
	if len(attrs) == 0 {
		return nil, nil
	}
	const envelope = "heterolog_attributes"
	opts := h.opts
	opts.ReplaceAttr = func(groups []string, attr slog.Attr) slog.Attr {
		if len(groups) == 0 {
			return slog.Attr{}
		}
		if h.opts.ReplaceAttr == nil {
			return attr
		}
		path := make([]string, 0, len(h.groups)+len(groups)-1)
		path = append(path, h.groups...)
		path = append(path, groups[1:]...)
		return h.opts.ReplaceAttr(path, attr)
	}
	var record slog.Record
	record.AddAttrs(slog.Attr{Key: envelope, Value: slog.GroupValue(attrs...)})
	values, err := encodeRecord(record, &opts)
	if err != nil {
		return nil, err
	}
	result, _ := values[envelope].(map[string]any)
	return result, nil
}

func encodeRecord(record slog.Record, opts *slog.HandlerOptions) (map[string]any, error) {
	var buffer bytes.Buffer
	if err := slog.NewJSONHandler(&buffer, opts).Handle(context.Background(), record); err != nil {
		return nil, fmt.Errorf("heterolog: encode slog record: %w", err)
	}
	decoder := json.NewDecoder(&buffer)
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("heterolog: decode slog record: %w", err)
	}
	return result, nil
}

func addAttributes(targets *[]map[string]any, groups []string, attrs map[string]any) {
	if len(attrs) == 0 {
		return
	}
	// WithGroupのpathは、派生したhandlerの履歴に沿って伸びるだけです。新しいgroupを
	// 開くと、同じ名前の前のattributeを置き換えます。開いているgroupを続ける場合は、
	// 結び付けたattributeを残します。
	for len(*targets) <= len(groups) {
		parent := (*targets)[len(*targets)-1]
		next := make(map[string]any)
		parent[groups[len(*targets)-1]] = next
		*targets = append(*targets, next)
	}
	target := (*targets)[len(groups)]
	for key, value := range attrs {
		target[key] = cloneValue(value)
	}
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		clone := make(map[string]any, len(value))
		for key, item := range value {
			clone[key] = cloneValue(item)
		}
		return clone
	case []any:
		clone := make([]any, len(value))
		for index, item := range value {
			clone[index] = cloneValue(item)
		}
		return clone
	default:
		return value
	}
}
