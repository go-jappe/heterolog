package slack

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-jappe/heterolog/pkg/logger"
	slackgo "github.com/slack-go/slack"
)

type slackResponseEvidenceKey struct{}
type slackResponseEvidence struct{ statusCode int }

type slackRequestNotSentError struct{ Err error }

func (e *slackRequestNotSentError) Error() string { return e.Err.Error() }
func (e *slackRequestNotSentError) Unwrap() error { return e.Err }

type slackDeliveryError struct {
	status logger.DeliveryStatus
	err    error
}

func (e *slackDeliveryError) Error() string { return e.err.Error() }
func (e *slackDeliveryError) Unwrap() error { return e.err }

// postSlackMessage preserves response evidence separately from SDK errors. For
// example, an HTTP 429 with an invalid Retry-After is a known refusal even when
// the SDK returns an ordinary number-parsing error rather than RateLimitedError.
func postSlackMessage(ctx context.Context, client *slackgo.Client, channel, message string) (string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", "", &slackDeliveryError{status: logger.DeliveryRejected, err: err}
	}
	evidence := &slackResponseEvidence{}
	ctx = context.WithValue(ctx, slackResponseEvidenceKey{}, evidence)
	resultChannel, timestamp, err := client.PostMessageContext(ctx, channel, slackgo.MsgOptionText(message, true))
	if err == nil {
		return resultChannel, timestamp, nil
	}
	status := logger.DeliveryUnconfirmed
	var notSent *slackRequestNotSentError
	var httpFailure slackgo.StatusCodeError
	var apiFailure slackgo.SlackErrorResponse
	var rateLimit *slackgo.RateLimitedError
	switch {
	case errors.As(err, &notSent):
		status = logger.DeliveryRejected
	case evidence.statusCode != 0 && evidence.statusCode != http.StatusOK,
		errors.As(err, &httpFailure), errors.As(err, &apiFailure), errors.As(err, &rateLimit):
		status = logger.DeliveryFailed
	}
	return resultChannel, timestamp, &slackDeliveryError{status: status, err: err}
}

func slackFailureDeliveryStatus(err error) logger.DeliveryStatus {
	var outcome *slackDeliveryError
	if errors.As(err, &outcome) {
		return outcome.status
	}
	return logger.DeliveryUnconfirmed
}
