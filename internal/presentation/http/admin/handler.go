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
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/google/uuid"
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
// すべての要求で「Authorization: Bearer <Token>」が必要です。PATCHは一つずつ順に処理し、
// 全loggerの全変更を検証してから反映して、一つでも不正なら何も反映しません。要求、変更を
// 始める時点の設定、変更後の設定はauditへ監査logとして記録し、拒否した要求も記録します。
// 記録しないのは、成功したGETだけです。変更は次のlogから反映し、メモリ上だけに保持します。
func NewHandler(cfg pkgadmin.HandlerConfig, audit pkglogger.DeliveryLogger) (http.Handler, error) {
	if len(cfg.Token) < pkgadmin.MinTokenLength {
		return nil, ErrTokenTooShort
	}
	service, err := outputsettings.NewService(cfg.Loggers, audit)
	if err != nil {
		return nil, err
	}
	return &handler{tokenDigest: sha256.Sum256([]byte(cfg.Token)), service: service}, nil
}

type handler struct {
	tokenDigest [sha256.Size]byte
	service     *outputsettings.Service
}

// 管理用APIのJSONの文書です。
type (
	settingsDocument struct {
		Loggers map[string]loggerDocument `json:"loggers"`
		// Warningsは、変更は反映したが知らせることがある場合の説明です。
		Warnings []string `json:"warnings,omitempty"`
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
		// Loggersは、変更を反映した後の誤りで、今の設定を返す場合だけ含めます。
		Loggers  map[string]loggerDocument `json:"loggers,omitempty"`
		Warnings []string                  `json:"warnings,omitempty"`
	}
)

// warningAuditUndeliveredは、監査logの一部が記録先のすべてへ届かなかったときの説明です。
const warningAuditUndelivered = "some audit records of this request were not delivered to every audit output"

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 要求IDは、応答のheaderと監査logの両方に含め、利用者が自分の要求の記録を探せる
	// ようにします。
	request := outputsettings.Request{ID: uuid.NewString(), RemoteAddr: r.RemoteAddr}
	w.Header().Set("X-Request-Id", request.ID)
	// 経路より先に認証し、tokenのない要求に経路の有無を知らせません。監査logが届いたか
	// どうかも、tokenのない要求には知らせません。
	if !h.authorized(r) {
		const reason = "missing or invalid bearer token"
		h.service.Reject(r.Context(), request, reason)
		w.Header().Set("WWW-Authenticate", `Bearer realm="heterolog"`)
		writeError(w, http.StatusUnauthorized, reason, false)
		return
	}
	if r.URL.Path != "/settings" {
		const reason = "not found"
		writeError(w, http.StatusNotFound, reason, h.service.Reject(r.Context(), request, reason))
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, newSettingsDocument(h.service.Settings()))
	case http.MethodPatch:
		h.patch(w, r, request)
	default:
		const reason = "method not allowed"
		w.Header().Set("Allow", "GET, PATCH")
		writeError(w, http.StatusMethodNotAllowed, reason, h.service.Reject(r.Context(), request, reason))
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

func (h *handler) patch(w http.ResponseWriter, r *http.Request, request outputsettings.Request) {
	reject := func(status int, reason string) {
		writeError(w, status, reason, h.service.Reject(r.Context(), request, reason))
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	var patch patchDocument
	if err := decoder.Decode(&patch); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reject(http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		reject(http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		reject(http.StatusBadRequest, "invalid JSON: the body must contain a single JSON object")
		return
	}
	changes, err := newChanges(patch)
	if err != nil {
		reject(http.StatusBadRequest, err.Error())
		return
	}
	result, err := h.service.Update(r.Context(), request, changes)
	if err != nil {
		writeError(w, statusOf(err), err.Error(), result.AuditUndelivered)
		return
	}
	var warnings []string
	if result.AuditUndelivered {
		warnings = append(warnings, warningAuditUndelivered)
	}
	settings := newSettingsDocument(h.service.Settings())
	if len(result.Mismatches) > 0 {
		// 変更は反映済みで、戻しません。今の設定を返し、要求どおりでないことを知らせます。
		writeJSON(w, http.StatusInternalServerError, errorDocument{
			Error:    "the settings were changed but do not match the request; they were not reverted",
			Loggers:  settings.Loggers,
			Warnings: warnings,
		})
		return
	}
	settings.Warnings = warnings
	writeJSON(w, http.StatusOK, settings)
}

// newChangesは、PATCHの文書を変更へ変換します。文書で省略したsamplingの項目は変更に
// 含めず、反映する時点の値のままにします。今の値をここで読んで補うと、同時のPATCHが
// 互いの変更を古い値で上書きするためです。loggerや出力先の有無は、Service.Updateが
// 確かめます。
func newChanges(
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
				change.Sampling = &pkglogger.SamplingChange{
					IsSamplingEnabled:        s.Enabled,
					EnableLoggingByRequestID: s.ByRequestID,
					LoggingByRequestIDRate:   s.RequestIDRate,
					SamplingDebugRate:        s.DebugRate,
					SamplingInfoRate:         s.InfoRate,
					SamplingWarnRate:         s.WarnRate,
					SamplingErrorRate:        s.ErrorRate,
				}
			}
			outputs[output] = change
		}
		changes[name] = outputs
	}
	return changes, nil
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

// writeErrorは、errorの文書を返します。auditUndeliveredは、この要求の監査logの一部が
// 記録先のすべてへ届かなかったことを表し、警告として含めます。
func writeError(w http.ResponseWriter, status int, message string, auditUndelivered bool) {
	document := errorDocument{Error: message}
	if auditUndelivered {
		document.Warnings = []string{warningAuditUndelivered}
	}
	writeJSON(w, status, document)
}
