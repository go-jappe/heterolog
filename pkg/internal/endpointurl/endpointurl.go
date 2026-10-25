// Package endpointurlは、pkg配下のclientが送信先endpointのURLを解析するときに、URLのuser info
// （ユーザー名・パスワード）を解析エラーの文面へ含めないための補助です。
// net/urlのurl.Parseは、解析に失敗した入力をそのまま*url.Errorの文面へ含めるため、
// user infoを含むendpointの設定誤りでパスワードがerrorやログへ出ることを防ぎます。
package endpointurl

import (
	"errors"
	"net/url"
	"strings"
)

// redactedUserInfoは、解析できなかったURLのuser infoを置き換える文字列です。
// net/urlのURL.Redactedがパスワードの代わりに使う文字列と同じです。
const redactedUserInfo = "xxxxx"

// errInvalidUserInfoは、user infoの部分が原因で解析に失敗したことを表します。net/urlの元のerrorは
// 文面にuser infoの一部を含むことがあるため返しません。
var errInvalidUserInfo = errors.New("invalid user info")

// Parseはurl.Parseと同じくrawURLを解析します。解析に失敗した場合も、返す*url.Errorの文面と
// フィールドはrawURLのuser infoを含みません。URLにはuser infoを"xxxxx"へ置き換えたrawURLを、
// Errには置き換えたURLの解析エラー（置き換えたURLは解析できる場合はuser infoが不正であること）を
// 入れます。user infoを含まない入力の解析エラーは、url.Parseのerrorをそのまま返します。
func Parse(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err == nil {
		return parsed, nil
	}
	redacted, ok := redactUserInfo(rawURL)
	if !ok {
		return nil, err
	}
	if _, redactedErr := url.Parse(redacted); redactedErr != nil {
		return nil, redactedErr
	}
	return nil, &url.Error{Op: "parse", URL: redacted, Err: errInvalidUserInfo}
}

// redactUserInfoは、rawURLのauthorityの先頭（"//"の後、それがなければschemeの区切りの":"の後、
// どちらもなければ先頭）から最後の"@"までをuser infoとみなし、redactedUserInfoへ置き換えます。
// url.Parseと異なり"/"・"?"・"#"でauthorityを区切らないため、escapeしていない記号を含む
// パスワードも置き換えます。"@"がなければuser infoはないものとしてfalseを返します。
func redactUserInfo(rawURL string) (string, bool) {
	start := 0
	if index := strings.Index(rawURL, "//"); index >= 0 {
		start = index + len("//")
	} else if index := strings.Index(rawURL, ":"); index >= 0 {
		start = index + 1
	}
	at := strings.LastIndex(rawURL[start:], "@")
	if at < 0 {
		return rawURL, false
	}
	return rawURL[:start] + redactedUserInfo + rawURL[start+at:], true
}
