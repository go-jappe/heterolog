package slack

import (
	"net/http"
	"time"

	slackgo "github.com/slack-go/slack"
)

// newSlackAPIは、Slack API呼び出しが無期限に停止しないHTTP clientを設定します。
// httpClientは設定のHTTPClientで、nilの場合はnet/httpの既定transportを使います。
func newSlackAPI(token string, timeout time.Duration, httpClient *http.Client, debug bool) *slackgo.Client {
	options := []slackgo.Option{
		slackgo.OptionHTTPClient(slackResponseHTTPClient{client: newSlackHTTPClient(timeout, httpClient)}),
	}
	if debug {
		options = append(options, slackgo.OptionDebug(true))
	}
	return slackgo.New(token, options...)
}

// newSlackHTTPClientは、Slack API呼び出し専用のHTTP clientを返します。baseがnilの場合は
// net/httpの既定transportを使います。baseを指定した場合はその設定をcopyして
// Transport、Jar、CheckRedirectを引き継ぎ、base自体は変更しません。
// Timeoutはどちらの場合もtimeoutを使い、baseのTimeoutより優先します。
func newSlackHTTPClient(timeout time.Duration, base *http.Client) *http.Client {
	client := cloneHTTPClient(base)
	if client == nil {
		client = &http.Client{}
	}
	// timeoutはconstructorで正の値に検証済みです。この境界では補正しません。
	client.Timeout = timeout
	return client
}

// cloneHTTPClientは、呼び出し元のhttp.Clientを変更せずに設定を保持するcopyを返します。
// Transport、Jar、CheckRedirectは元のclientと共有し、nilはnilのまま返します。
func cloneHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		return nil
	}
	clone := *client
	return &clone
}
