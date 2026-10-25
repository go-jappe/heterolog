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

// slackResponseHTTPClient checks the entire chat.postMessage response before
// the SDK consumes it. The SDK decodes only the first JSON value and accepts
// missing acknowledgement fields, which cannot establish message delivery.
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
	// HTTP refusal is already conclusive. Let the SDK retain its status/rate-limit
	// error, without requiring a readable JSON body from an error response.
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
	// Unmarshal requires one complete JSON value, including the end of the body.
	// Unknown fields are permitted for forwards compatibility with Slack.
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
	// Channel names/aliases may resolve to IDs. Do not require an exact match to
	// the request, or constrain Slack's opaque channel/timestamp identifiers.
	if strings.TrimSpace(response.Channel) == "" || strings.TrimSpace(response.TS) == "" {
		return errors.New("successful response has no channel or timestamp")
	}
	return nil
}
