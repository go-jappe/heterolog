package admin

import (
	"embed"
	"net/http"
	"path"
)

// uiFilesは、管理用APIのブラウザ画面です。1枚のHTMLと、そのCSSとJavaScriptで、外部の
// 配信元（CDNなど）は使いません。
//
//go:embed ui/index.html ui/ui.css ui/ui.js
var uiFiles embed.FS

// uiAssetsは、画面のpathと、埋め込んだfileとContent-Typeの対応です。
var uiAssets = map[string]struct{ file, contentType string }{
	"/":       {"ui/index.html", "text/html; charset=utf-8"},
	"/ui.css": {"ui/ui.css", "text/css; charset=utf-8"},
	"/ui.js":  {"ui/ui.js", "text/javascript; charset=utf-8"},
}

// uiContentSecurityPolicyは、画面のscriptとstyleを、同じ配信元のfileだけに限ります。
// inlineのscriptと、ほかの配信元への接続・埋め込みは禁止です。
const uiContentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; " +
	"img-src 'self' data:; form-action 'none'; base-uri 'none'; frame-ancestors 'none'"

// serveUIは、ブラウザ画面のfileを返します。画面はtokenを持たないため認証せず、監査logにも
// 記録しません。tokenは、画面に入力してsessionStorageに置き、画面がAPIへ送ります。
// 画面のpathでなければfalseを返します。
func (h *handler) serveUI(w http.ResponseWriter, r *http.Request) bool {
	asset, ok := uiAssets[path.Clean(r.URL.Path)]
	if !ok {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	content, err := uiFiles.ReadFile(asset.file)
	if err != nil {
		http.Error(w, "the admin page is not available", http.StatusInternalServerError)
		return true
	}
	w.Header().Set("Content-Type", asset.contentType)
	w.Header().Set("Content-Security-Policy", uiContentSecurityPolicy)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(content)
	}
	return true
}
