package victoriametrics

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	vm "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

type lifecycleMetricsClient struct {
	send  func(context.Context, []vm.Metric) error
	close func() error
}

func (c lifecycleMetricsClient) Send(ctx context.Context, metrics []vm.Metric) error {
	return c.send(ctx, metrics)
}
func (c lifecycleMetricsClient) SendAsync([]vm.Metric) error       { return nil }
func (c lifecycleMetricsClient) HealthCheck(context.Context) error { return nil }
func (c lifecycleMetricsClient) Close() error                      { return c.close() }

type lifecycleMetricsCollector struct{}

func (lifecycleMetricsCollector) CollectCPUMetrics() ([]string, error)    { return nil, nil }
func (lifecycleMetricsCollector) CollectMemoryMetrics() ([]string, error) { return nil, nil }
func (lifecycleMetricsCollector) CollectRuntimeMetrics() []string         { return nil }

func TestPushManagerStopWaitsForAcceptedSendAndPreservesFinalPush(t *testing.T) {
	for _, blockOn := range []int{1, 2} {
		name := "initial push"
		if blockOn == 2 {
			name = "periodic push"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls, closeCalls := 0, 0
				release := make(chan struct{})
				var acceptedCtx context.Context
				cfg := DefaultConfig()
				cfg.PushInterval = time.Second
				cfg.Timeout = time.Minute
				cfg.OnError = func(err error) { t.Errorf("unexpected send error: %v", err) }
				pm := &PushManager{
					config: cfg, collector: lifecycleMetricsCollector{},
					client: lifecycleMetricsClient{
						send: func(ctx context.Context, _ []vm.Metric) error {
							calls++
							if err := ctx.Err(); err != nil {
								t.Errorf("send %d started with canceled context: %v", calls, err)
							}
							if _, ok := ctx.Deadline(); !ok {
								t.Error("send must have a timeout")
							}
							if calls == blockOn {
								acceptedCtx = ctx
								<-release
								if err := ctx.Err(); err != nil {
									t.Errorf("Stop canceled an accepted send: %v", err)
								}
							}
							return nil
						},
						close: func() error {
							closeCalls++
							if calls != blockOn+1 {
								t.Errorf("client closed before final push: calls=%d", calls)
							}
							return nil
						},
					},
				}
				if err := pm.Start(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if blockOn == 2 {
					time.Sleep(cfg.PushInterval)
					synctest.Wait()
				}
				if calls != blockOn {
					t.Fatalf("send calls = %d, want %d", calls, blockOn)
				}
				if err := pm.Start(); err == nil {
					t.Fatal("Start accepted an already running manager")
				}
				stopped := make(chan error, 1)
				go func() { stopped <- pm.Stop() }()
				synctest.Wait()
				if err := acceptedCtx.Err(); err != nil {
					t.Fatalf("accepted send context after Stop = %v, want active", err)
				}
				select {
				case err := <-stopped:
					t.Fatalf("Stop returned before accepted send completed: %v", err)
				default:
				}
				if closeCalls != 0 {
					t.Fatal("Stop closed the client used by an accepted send")
				}
				// 停止要求後にtickが溜まっても、最後の送信以外は開始しません。
				time.Sleep(3 * cfg.PushInterval)
				close(release)
				if err := <-stopped; err != nil {
					t.Fatal(err)
				}
				if calls != blockOn+1 || closeCalls != 1 {
					t.Fatalf("send calls=%d close calls=%d, want %d and 1", calls, closeCalls, blockOn+1)
				}
				if err := pm.Start(); err == nil {
					t.Fatal("Start reused the client closed by Stop")
				}
			})
		})
	}
}

func TestPushManagerStopWaitsForAcceptedErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("send failed")
		entered, release := make(chan struct{}), make(chan struct{})
		calls, closeCalls := 0, 0
		cfg := DefaultConfig()
		cfg.PushInterval = time.Second
		cfg.OnError = func(err error) {
			if !errors.Is(err, failure) {
				t.Errorf("notification error = %v, want send failure", err)
			}
			close(entered)
			<-release
		}
		pm := &PushManager{
			config: cfg, collector: lifecycleMetricsCollector{},
			client: lifecycleMetricsClient{
				send: func(ctx context.Context, _ []vm.Metric) error {
					calls++
					if err := ctx.Err(); err != nil {
						t.Errorf("send started with canceled context: %v", err)
					}
					if calls == 1 {
						return failure
					}
					return nil
				},
				close: func() error { closeCalls++; return nil },
			},
		}
		if err := pm.Start(); err != nil {
			t.Fatal(err)
		}
		<-entered
		stopped := make(chan error, 1)
		go func() { stopped <- pm.Stop() }()
		synctest.Wait()
		select {
		case err := <-stopped:
			t.Fatalf("Stop returned before OnError completed: %v", err)
		default:
		}
		if closeCalls != 0 {
			t.Fatal("client closed before OnError completed")
		}
		close(release)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		if calls != 2 || closeCalls != 1 {
			t.Fatalf("send calls=%d close calls=%d, want final push and one close", calls, closeCalls)
		}
	})
}
