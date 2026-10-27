// Package adminは、稼働中のloggerの出力先ごとのlog levelとsampling設定を読み取り・
// 変更する、HTTPの管理用APIの受け口です。要求の認証、経路、JSONの変換、errorと
// 状態codeの対応を扱い、設定の検証と反映はapplication層のoutputsettingsに任せます。
package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// maxRequestBytesは、管理用APIの要求本文の上限です。
const maxRequestBytes = 64 << 10

// ErrTokenTooShortは、tokenがMinTokenLengthより短いことを表します。tokenは含めません。
var ErrTokenTooShort = fmt.Errorf("admin token must be at least %d bytes", pkgadmin.MinTokenLength)

// NewHandlerは、管理用APIのhttp.Handlerを返します。
//
//	GET   /settings  全loggerの現在の設定を返します
//	PATCH /settings  JSONの本文で指定した項目を変更し、変更後の設定を返します
//
// すべての要求で「Authorization: Bearer <Token>」が必要です。PATCHは全loggerの全変更を
// 検証してから反映し、一つでも不正なら何も反映しません。変更は次のlogから反映し、
// メモリ上だけに保持します。
func NewHandler(cfg pkgadmin.HandlerConfig) (http.Handler, error) {
	if len(cfg.Token) < pkgadmin.MinTokenLength {
		return nil, ErrTokenTooShort
	}
	service, err := outputsettings.NewService(cfg.Loggers)
	if err != nil {
		return nil, err
	}
	onChange := cfg.OnChange
	if onChange == nil {
		onChange = logChange
	}
	return &handler{tokenDigest: sha256.Sum256([]byte(cfg.Token)), service: service, onChange: onChange}, nil
}

type handler struct {
	tokenDigest [sha256.Size]byte
	service     *outputsettings.Service
	onChange    func(pkgadmin.Change)
}

// 管理用APIのJSONの文書です。
type (
	settingsDocument struct {
		Loggers map[string]loggerDocument `json:"loggers"`
	}
	loggerDocument struct {
		Outputs map[pkglogger.LogOutputDestination]outputDocument `json:"outputs"`
	}
	outputDocument struct {
		Level    string           `json:"level"`
		Sampling samplingDocument `json:"sampling"`
	}
	samplingDocument struct {
		Enabled       bool `json:"enabled"`
		ByRequestID   bool `json:"by_request_id"`
		RequestIDRate int  `json:"request_id_rate"`
		DebugRate     int  `json:"debug_rate"`
		InfoRate      int  `json:"info_rate"`
		WarnRate      int  `json:"warn_rate"`
		ErrorRate     int  `json:"error_rate"`
	}
	patchDocument struct {
		Loggers map[string]loggerPatch `json:"loggers"`
	}
	loggerPatch struct {
		Outputs map[pkglogger.LogOutputDestination]outputPatch `json:"outputs"`
	}
	outputPatch struct {
		Level    *string        `json:"level"`
		Sampling *samplingPatch `json:"sampling"`
	}
	samplingPatch struct {
		Enabled       *bool `json:"enabled"`
		ByRequestID   *bool `json:"by_request_id"`
		RequestIDRate *int  `json:"request_id_rate"`
		DebugRate     *int  `json:"debug_rate"`
		InfoRate      *int  `json:"info_rate"`
		WarnRate      *int  `json:"warn_rate"`
		ErrorRate     *int  `json:"error_rate"`
	}
	errorDocument struct {
		Error string `json:"error"`
	}
)

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 経路より先に認証し、tokenのない要求に経路の有無を知らせません。
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="heterolog"`)
		writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}
	if r.URL.Path != "/settings" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, newSettingsDocument(h.service.Settings()))
	case http.MethodPatch:
		h.patch(w, r)
	default:
		w.Header().Set("Allow", "GET, PATCH")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// authorizedは、tokenのdigestを一定時間で比較します。拒否までの時間から、tokenの
// 一致した長さもtokenの長さも分からないようにするためです。
func (h *handler) authorized(r *http.Request) bool {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return subtle.ConstantTimeCompare(digest[:], h.tokenDigest[:]) == 1
}

func (h *handler) patch(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	var patch patchDocument
	if err := decoder.Decode(&patch); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid JSON: the body must contain a single JSON object")
		return
	}
	changes, err := newChanges(h.service.Settings(), patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	events, err := h.service.Update(changes)
	for _, event := range events {
		h.onChange(pkgadmin.Change{RemoteAddr: r.RemoteAddr, Logger: event.Logger, Output: event.Output, Before: event.Before, After: event.After})
	}
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, newSettingsDocument(h.service.Settings()))
}

// newChangesは、PATCHの文書を変更へ変換します。文書で省略したsamplingの項目は、
// currentの今の値のままにします。loggerや出力先の有無は、Service.Updateが確かめます。
func newChanges(
	current map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings,
	patch patchDocument,
) (map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange, error) {
	changes := make(map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange, len(patch.Loggers))
	for _, name := range slices.Sorted(maps.Keys(patch.Loggers)) {
		outputs := map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{}
		for _, output := range slices.Sorted(maps.Keys(patch.Loggers[name].Outputs)) {
			p := patch.Loggers[name].Outputs[output]
			var change pkglogger.OutputSettingsChange
			if p.Level != nil {
				level, err := pkglogger.ParseLogLevel(*p.Level)
				if err != nil {
					return nil, fmt.Errorf("logger %q output %q: %w", name, output, err)
				}
				change.Level = &level
			}
			if s := p.Sampling; s != nil {
				sampling := current[name][output].Sampling
				setIfPresent(&sampling.IsSamplingEnabled, s.Enabled)
				setIfPresent(&sampling.EnableLoggingByRequestID, s.ByRequestID)
				setIfPresent(&sampling.LoggingByRequestIDRate, s.RequestIDRate)
				setIfPresent(&sampling.SamplingDebugRate, s.DebugRate)
				setIfPresent(&sampling.SamplingInfoRate, s.InfoRate)
				setIfPresent(&sampling.SamplingWarnRate, s.WarnRate)
				setIfPresent(&sampling.SamplingErrorRate, s.ErrorRate)
				change.Sampling = &sampling
			}
			outputs[output] = change
		}
		changes[name] = outputs
	}
	return changes, nil
}

func setIfPresent[T any](target *T, value *T) {
	if value != nil {
		*target = *value
	}
}

// statusOfは、Service.Updateのerrorに対応する状態codeを返します。
func statusOf(err error) int {
	switch {
	case errors.Is(err, outputsettings.ErrUnknownLogger), errors.Is(err, pkglogger.ErrUnknownOutput):
		return http.StatusNotFound
	case errors.Is(err, outputsettings.ErrApplyFailed):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}

func newSettingsDocument(settings map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings) settingsDocument {
	document := settingsDocument{Loggers: make(map[string]loggerDocument, len(settings))}
	for name, outputs := range settings {
		logger := loggerDocument{Outputs: make(map[pkglogger.LogOutputDestination]outputDocument, len(outputs))}
		for output, s := range outputs {
			logger.Outputs[output] = outputDocument{
				Level: s.Level.String(),
				Sampling: samplingDocument{
					Enabled:       s.Sampling.IsSamplingEnabled,
					ByRequestID:   s.Sampling.EnableLoggingByRequestID,
					RequestIDRate: s.Sampling.LoggingByRequestIDRate,
					DebugRate:     s.Sampling.SamplingDebugRate,
					InfoRate:      s.Sampling.SamplingInfoRate,
					WarnRate:      s.Sampling.SamplingWarnRate,
					ErrorRate:     s.Sampling.SamplingErrorRate,
				},
			}
		}
		document.Loggers[name] = logger
	}
	return document
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorDocument{Error: message})
}

// logChangeは、変更を標準のloggerへ一行出力します。
func logChange(change pkgadmin.Change) {
	log.Printf("heterolog: admin API changed %s/%s from %s: level %s -> %s, sampling %+v -> %+v",
		change.Logger, change.Output, change.RemoteAddr,
		change.Before.Level, change.After.Level, change.Before.Sampling, change.After.Sampling)
}
