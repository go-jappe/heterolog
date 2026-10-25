package pressure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/memory/pressure"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

var (
	// ErrInvalidMonitorConfig は、New が受け取った provider・policy・取得間隔のいずれかが
	// 不正なため monitor を生成しなかったことを表します。
	ErrInvalidMonitorConfig = errors.New("invalid memory pressure monitor config")
	// ErrPressureNotRead は、Start 前や Reset 後など、まだ一度もメモリ逼迫状態を
	// 取得していないため Current・IsCritical が値を返せないことを表します。
	ErrPressureNotRead = errors.New("memory pressure has not been read yet")
)

// memoryPressureMonitor は、メモリ逼迫状態をバックグラウンドで定期取得しつつ、
// critical overload 判定を共有状態として管理します。
//
// 特徴:
//   - Start 後は常駐 goroutine が定期取得する
//   - Current / IsCritical / Last は保持済み状態を参照する
//   - critical 候補の連続状態は pkg/memorypressure.CriticalState で一元管理する
type memoryPressureMonitor struct {
	// 内部状態の排他制御
	mu sync.RWMutex

	// メモリ逼迫状態の取得元。OS・実行環境ごとの実装を pkg/memorypressure.Provider として受け取ります。
	reader memorypressure.Provider

	// メモリ取得間隔
	pollInterval time.Duration

	// critical 判定ポリシー。pkg/memorypressure.CriticalState と同じ規則で評価します。
	policy memorypressure.CriticalPolicy

	// 起動は 1 回だけに制限する
	startOnce sync.Once

	// Start の初回結果
	startErr error

	// 直近取得した pressure
	lastPressure memorypressure.MemoryPressure

	// 直近取得時刻。未取得なら zero time
	lastReadAt time.Time

	// 直近の critical 判定結果
	lastCritical bool

	// critical 候補の連続回数
	state memorypressure.CriticalState

	// 現在の critical 候補が始まった時刻。critical 候補でなければ zero time
	criticalSince time.Time

	// 直近取得エラー
	lastReadErr error
}

// New は、policy を pkg/memorypressure.CriticalState と同じ規則で評価する monitor を生成します。
//
// 入力の誤りを既定値で隠さないよう、provider が nil、pollInterval が 0 以下、
// policy が閾値を一つも有効にしない、または不正な値を含む場合は、ErrInvalidMonitorConfig を
// 包んだエラーを返します。ConsecutiveCriticalHits の 0 は、CriticalState と同じく 1 として扱います。
func New(
	provider memorypressure.Provider,
	policy memorypressure.CriticalPolicy,
	pollInterval time.Duration,
) (pressure.MemoryPressureMonitor, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: provider is nil", ErrInvalidMonitorConfig)
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("%w: poll interval must be positive: %s", ErrInvalidMonitorConfig, pollInterval)
	}
	if err := validateCriticalPolicy(policy); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMonitorConfig, err)
	}

	return &memoryPressureMonitor{
		reader:       provider,
		policy:       policy,
		pollInterval: pollInterval,
	}, nil
}

func (m *memoryPressureMonitor) Start(ctx context.Context) error {
	m.startOnce.Do(func() {
		// 起動直後に 1 回取得しておく。
		if err := m.pollOnce(ctx); err != nil {
			m.startErr = err
			return
		}

		go m.loop(ctx)
	})

	return m.startErr
}

func (m *memoryPressureMonitor) loop(ctx context.Context) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = m.pollOnce(ctx)
		}
	}
}

func (m *memoryPressureMonitor) pollOnce(ctx context.Context) error {
	now := time.Now()

	p, err := m.reader.ReadMemoryPressure(ctx)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		m.lastReadErr = err
		//	if m.logger != nil {
		//		m.logger.Log(
		//			ctx,
		//			"warn",
		//			"failed to read memory pressure",
		//			Attr{Key: "error", Value: err.Error()},
		//		)
		//	}
		return err
	}

	m.lastPressure = p
	m.lastReadAt = now
	m.lastReadErr = nil
	m.lastCritical = m.evaluateLocked(now, p)

	return nil
}

// evaluateLocked は今回の観測を pkg/memorypressure.CriticalState で評価し、
// snapshot 用に critical 候補の開始時刻を記録します。呼出し元は m.mu を保持します。
func (m *memoryPressureMonitor) evaluateLocked(now time.Time, p memorypressure.MemoryPressure) bool {
	critical := m.state.Evaluate(p, m.policy)
	switch {
	case m.state.ConsecutiveHits == 0:
		m.criticalSince = time.Time{}
	case m.criticalSince.IsZero():
		m.criticalSince = now
	}
	return critical
}

// IsCritical は、現在保持しているメモリ状態が
// critical overload かどうかを返します。
//
// このメソッド自身は新たなメモリ取得や再判定を行わず、
// バックグラウンド取得で更新済みの状態をそのまま参照します。
//
// 戻り値:
//   - true:  直近評価結果が critical
//   - false: 直近評価結果が non-critical
//
// エラー:
//   - まだ一度も取得できていない場合
//   - 初回取得が失敗したままで有効な値がまだ無い場合
func (m *memoryPressureMonitor) IsCritical(ctx context.Context) (bool, error) {
	_ = ctx

	// 内部状態は読み取りだけなので read lock で保護する。
	m.mu.RLock()
	defer m.mu.RUnlock()

	// まだ一度も取得されていなければ判定不能。
	if m.lastReadAt.IsZero() && m.lastReadErr == nil {
		return false, ErrPressureNotRead
	}

	// 初回取得が失敗しており、有効な取得結果がまだ無ければ error を返す。
	if m.lastReadErr != nil && m.lastReadAt.IsZero() {
		return false, m.lastReadErr
	}

	// 直近評価済みの critical 判定結果を返す。
	return m.lastCritical, nil
}

func (m *memoryPressureMonitor) Last() memorypressure.MonitorSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return memorypressure.MonitorSnapshot{
		Pressure:        m.lastPressure,
		LastReadAt:      m.lastReadAt,
		LastCritical:    m.lastCritical,
		ConsecutiveHits: m.state.ConsecutiveHits,
		CriticalSince:   m.criticalSince,
	}
}

func (m *memoryPressureMonitor) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastPressure = memorypressure.MemoryPressure{}
	m.lastReadAt = time.Time{}
	m.lastCritical = false
	m.lastReadErr = nil
	m.state.Reset()
	m.criticalSince = time.Time{}
}

func (m *memoryPressureMonitor) Current(ctx context.Context) (memorypressure.MemoryPressure, error) {
	_ = ctx

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.lastReadAt.IsZero() && m.lastReadErr == nil {
		return memorypressure.MemoryPressure{}, ErrPressureNotRead
	}
	if m.lastReadErr != nil && m.lastReadAt.IsZero() {
		return memorypressure.MemoryPressure{}, m.lastReadErr
	}

	return m.lastPressure, nil
}
