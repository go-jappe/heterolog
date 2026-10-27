package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

type slackHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// slackResponseHTTPClientは、SDKが消費する前にchat.postMessageのresponse全体を検査します。
// SDKは最初のJSON値だけをdecodeし、確認応答のfieldがなくても受け入れますが、
// それではmessageの配送を確定できません。
type slackResponseHTTPClient struct{ client slackHTTPClient }

func (c slackResponseHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, &slackRequestNotSentError{Err: err}
	}
	response, err := c.client.Do(request)
	if err != nil {
		return response, err
	}
	if evidence, ok := request.Context().Value(slackResponseEvidenceKey{}).(*slackResponseEvidence); ok {
		evidence.statusCode = response.StatusCode
	}
	// HTTPでの拒否はそれだけで確定しています。error responseに読めるJSON本文を求めず、
	// SDKのstatus・rate limit errorをそのまま残します。
	if response.StatusCode != http.StatusOK {
		return response, nil
	}
	if response.Body == nil {
		return nil, errors.New("Slack response has no body")
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("read complete Slack response: %w", err)
	}
	if err := validateSlackPostResponse(body); err != nil {
		return nil, fmt.Errorf("invalid Slack message response: %w", err)
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}

func validateSlackPostResponse(body []byte) error {
	if !utf8.Valid(body) {
		return errors.New("response is not valid UTF-8")
	}
	var response struct {
		OK      *bool  `json:"ok"`
		Error   string `json:"error"`
		Channel string `json:"channel"`
		TS      string `json:"ts"`
	}
	// Unmarshalは、本文の終わりまでを含む一つの完全なJSON値を要求します。
	// Slackとの前方互換性のため、未知のfieldは許可します。
	if err := json.Unmarshal(body, &response); err != nil {
		return err
	}
	if response.OK == nil {
		return errors.New("response has no boolean ok field")
	}
	if !*response.OK {
		if strings.TrimSpace(response.Error) == "" {
			return errors.New("refusal response has no error code")
		}
		return nil
	}
	if response.Error != "" {
		return errors.New("successful response also contains an error")
	}
	// channel名やaliasはIDへ解決されることがあります。requestとの完全一致は求めず、
	// Slackの不透明なchannel・timestamp識別子にも制約を課しません。
	if strings.TrimSpace(response.Channel) == "" || strings.TrimSpace(response.TS) == "" {
		return errors.New("successful response has no channel or timestamp")
	}
	return nil
}
