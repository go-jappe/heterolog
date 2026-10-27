// Package metricsは、system metricsを収集して整形する機能を提供します。
package metrics

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// 一貫したmetric名を付けるためのMetricName定数です。
const (
	CPUUsagePercent    = "system_cpu_usage_percent"
	MemoryUsedBytes    = "system_memory_used_bytes"
	MemoryTotalBytes   = "system_memory_total_bytes"
	MemoryUsagePercent = "system_memory_usage_percent"
	GoGoroutines       = "go_goroutines"
	GoAllocBytes       = "go_alloc_bytes"
)

// SystemMetricsCollectorは、system metricsを収集するためのinterfaceを提供します。
type SystemMetricsCollector interface {
	CollectCPUMetrics() ([]string, error)
	CollectMemoryMetrics() ([]string, error)
	CollectRuntimeMetrics() []string
}

// DefaultSystemMetricsCollectorは、SystemMetricsCollectorの既定の実装です。
type DefaultSystemMetricsCollector struct{}

// NewSystemMetricsCollectorは、既定のsystem metrics collectorの新しいinstanceを作成します。
func NewSystemMetricsCollector() SystemMetricsCollector {
	return &DefaultSystemMetricsCollector{}
}

// CollectCPUMetricsは、CPU関連のmetricsを収集します。
func (c *DefaultSystemMetricsCollector) CollectCPUMetrics() ([]string, error) {
	var metrics []string

	percentages, err := cpu.Percent(0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get CPU usage: %w", err)
	}

	if len(percentages) > 0 {
		metrics = append(metrics, fmt.Sprintf("%s %f", CPUUsagePercent, percentages[0]))
	}

	return metrics, nil
}

// CollectMemoryMetricsは、memory関連のmetricsを収集します。
func (c *DefaultSystemMetricsCollector) CollectMemoryMetrics() ([]string, error) {
	var metrics []string

	v, err := mem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to get memory usage: %w", err)
	}

	metrics = append(metrics, fmt.Sprintf("%s %d", MemoryUsedBytes, v.Used))
	metrics = append(metrics, fmt.Sprintf("%s %d", MemoryTotalBytes, v.Total))
	metrics = append(metrics, fmt.Sprintf("%s %f", MemoryUsagePercent, v.UsedPercent))

	return metrics, nil
}

// CollectRuntimeMetricsは、Go runtimeのmetricsを収集します。
func (c *DefaultSystemMetricsCollector) CollectRuntimeMetrics() []string {
	var metrics []string

	// goroutine数
	metrics = append(metrics, fmt.Sprintf("%s %d", GoGoroutines, runtime.NumGoroutine()))

	// memory割り当ての統計
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	metrics = append(metrics, fmt.Sprintf("%s %d", GoAllocBytes, m.Alloc))

	return metrics
}

// CollectSystemMetricsは、すべてのsystem metricsをPrometheus exposition formatで返します。
func CollectSystemMetrics() (string, error) {
	collector := NewSystemMetricsCollector()
	return CollectSystemMetricsWithCollector(collector)
}

// CollectSystemMetricsWithCollectorは、渡されたcollectorを使ってmetricsを収集します。
// これにより、依存性の注入ができ、testも容易になります。
func CollectSystemMetricsWithCollector(collector SystemMetricsCollector) (string, error) {
	var allMetrics []string

	// CPUのmetricsを収集します。
	cpuMetrics, err := collector.CollectCPUMetrics()
	if err != nil {
		return "", err
	}
	allMetrics = append(allMetrics, cpuMetrics...)

	// memoryのmetricsを収集します。
	memMetrics, err := collector.CollectMemoryMetrics()
	if err != nil {
		return "", err
	}
	allMetrics = append(allMetrics, memMetrics...)

	// runtimeのmetricsを収集します。
	runtimeMetrics := collector.CollectRuntimeMetrics()
	allMetrics = append(allMetrics, runtimeMetrics...)

	// すべてのmetricsを改行で連結し、末尾に改行を付けます。
	return strings.Join(allMetrics, "\n") + "\n", nil
}
