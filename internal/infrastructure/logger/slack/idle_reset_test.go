package slack

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	slackgo "github.com/slack-go/slack"
)

// Exercise actual requests rather than the transient interval/timer fields.
// A burst begins immediately, grows through the configured intervals, and
// restarts at the first interval after more than MaxRestThreshold without logs.
func TestSlackIdleResetReplacesPendingTimerAndPreservesQueuedLogs(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(slackResetKind(fallback), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &slackResetHTTPClient{}
				fixture := newSlackResetFixture(t, fallback, client)
				start := time.Now()
				fixture.enqueue(t, "initial")
				synctest.Wait()
				client.requireCalls(t, 1, start, "initial")

				fixture.enqueue(t, "first interval queued")
				time.Sleep(time.Hour)
				fixture.enqueue(t, "threshold equality keeps burst")
				synctest.Wait()
				client.requireCalls(t, 1, start, "initial")
				time.Sleep(time.Hour)
				synctest.Wait()
				client.requireCalls(t, 2, start.Add(2*time.Hour), "first interval queued", "threshold equality keeps burst")

				fixture.enqueue(t, "old burst pending") // The next interval is three hours.
				time.Sleep(time.Hour + time.Nanosecond)
				fixture.enqueue(t, "new burst")
				synctest.Wait()
				client.requireCalls(t, 3, time.Now(), "old burst pending", "new burst")
				fixture.enqueue(t, "after reset first interval")

				// The invalidated three-hour timer would expire here. It must not
				// send the entry reserved for the new burst one nanosecond later.
				time.Sleep(2*time.Hour - time.Nanosecond)
				synctest.Wait()
				client.requireCalls(t, 3, start.Add(3*time.Hour+time.Nanosecond), "new burst")
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				client.requireCalls(t, 4, time.Now(), "after reset first interval")
				if err := fixture.close(); err != nil {
					t.Fatal(err)
				}
				time.Sleep(10 * time.Hour)
				synctest.Wait()
				client.requireExactlyOnce(t, "initial", "first interval queued", "threshold equality keeps burst", "old burst pending", "new burst", "after reset first interval")
			})
		})
	}
}

func TestSlackIdleResetDoesNotRescheduleWithinThreshold(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, idle := range []time.Duration{30 * time.Minute, time.Hour} {
			t.Run(slackResetKind(fallback)+"/"+idle.String(), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client := &slackResetHTTPClient{}
					fixture := newSlackResetFixture(t, fallback, client)
					start := time.Now()
					fixture.enqueue(t, "initial")
					synctest.Wait()
					fixture.enqueue(t, "waiting")
					time.Sleep(idle)
					fixture.enqueue(t, "still same burst")
					synctest.Wait()
					client.requireCalls(t, 1, start, "initial")
					time.Sleep(2*time.Hour - idle)
					synctest.Wait()
					client.requireCalls(t, 2, start.Add(2*time.Hour), "waiting", "still same burst")
				})
			})
		}
	}
}

type slackResetFixture struct {
	base     *SlackLogger
	write    func(string) error
	close    func() error
	fallback *FallbackLogger
}

func (f slackResetFixture) enqueue(t *testing.T, message string) {
	t.Helper()
	if err := f.write(message); err != nil {
		t.Fatal(err)
	}
}

func newSlackResetFixture(t *testing.T, fallback bool, client *slackResetHTTPClient) slackResetFixture {
	t.Helper()
	intervals := []time.Duration{2 * time.Hour, 3 * time.Hour}
	var fixture slackResetFixture
	if fallback {
		cfg := fallbackSlackTestConfig(intervals)
		logger := newFallbackLoggerForTest(t, cfg)
		logger.slackAPI = slackgo.New(cfg.Token, slackgo.OptionHTTPClient(client))
		fixture = slackResetFixture{
			base: logger.SlackLogger,
			write: func(message string) error {
				logger.enqueue(fallbackSlackTestEntry(pkglogger.LogLevelInfo, message))
				return nil
			},
			close:    logger.Close,
			fallback: logger,
		}
	} else {
		logger := newNormalSlackLifecycleLogger(t, intervals, client)
		fixture = slackResetFixture{
			base:  logger,
			write: func(message string) error { return logger.testInfo(context.Background(), message) },
			close: logger.Close,
		}
	}
	t.Cleanup(func() {
		if err := fixture.close(); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func slackResetKind(fallback bool) string {
	if fallback {
		return "fallback"
	}
	return "normal"
}

type slackResetRequest struct {
	post fallbackSlackPost
	at   time.Time
}

type slackResetHTTPClient struct {
	mu    sync.Mutex
	calls []slackResetRequest
}

func (c *slackResetHTTPClient) Do(req *http.Request) (*http.Response, error) {
	post, err := fallbackSlackPostFromRequest(req)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.calls = append(c.calls, slackResetRequest{post: post, at: time.Now()})
	c.mu.Unlock()
	return successfulSlackLifecycleResponse(req, post.channel), nil
}

func (c *slackResetHTTPClient) requireCalls(t *testing.T, count int, at time.Time, messages ...string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) != count {
		t.Fatalf("Slack requests = %d, want %d: %+v", len(c.calls), count, c.calls)
	}
	last := c.calls[count-1]
	if !last.at.Equal(at) {
		t.Errorf("last Slack request at %s, want %s", last.at, at)
	}
	for _, message := range messages {
		if !strings.Contains(last.post.text, message) {
			t.Errorf("last Slack request missing %q: %s", message, last.post.text)
		}
	}
}

func (c *slackResetHTTPClient) requireExactlyOnce(t *testing.T, messages ...string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, message := range messages {
		var count int
		for _, request := range c.calls {
			count += strings.Count(request.post.text, message)
		}
		if count != 1 {
			t.Errorf("message %q sent %d times, want once", message, count)
		}
	}
}
