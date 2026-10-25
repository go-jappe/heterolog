// Package metrics provides functionality for collecting and formatting system metrics.
package metrics

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// MetricName constants for consistent metric naming
const (
	CPUUsagePercent    = "system_cpu_usage_percent"
	MemoryUsedBytes    = "system_memory_used_bytes"
	MemoryTotalBytes   = "system_memory_total_bytes"
	MemoryUsagePercent = "system_memory_usage_percent"
	GoGoroutines       = "go_goroutines"
	GoAllocBytes       = "go_alloc_bytes"
)

// SystemMetricsCollector provides an interface for collecting system metrics
type SystemMetricsCollector interface {
	CollectCPUMetrics() ([]string, error)
	CollectMemoryMetrics() ([]string, error)
	CollectRuntimeMetrics() []string
}

// DefaultSystemMetricsCollector is the default implementation of SystemMetricsCollector
type DefaultSystemMetricsCollector struct{}

// NewSystemMetricsCollector creates a new instance of the default system metrics collector
func NewSystemMetricsCollector() SystemMetricsCollector {
	return &DefaultSystemMetricsCollector{}
}

// CollectCPUMetrics collects CPU-related metrics
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

// CollectMemoryMetrics collects memory-related metrics
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

// CollectRuntimeMetrics collects Go runtime metrics
func (c *DefaultSystemMetricsCollector) CollectRuntimeMetrics() []string {
	var metrics []string

	// Goroutine count
	metrics = append(metrics, fmt.Sprintf("%s %d", GoGoroutines, runtime.NumGoroutine()))

	// Memory allocation stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	metrics = append(metrics, fmt.Sprintf("%s %d", GoAllocBytes, m.Alloc))

	return metrics
}

// CollectSystemMetrics returns all system metrics in Prometheus exposition format
func CollectSystemMetrics() (string, error) {
	collector := NewSystemMetricsCollector()
	return CollectSystemMetricsWithCollector(collector)
}

// CollectSystemMetricsWithCollector collects metrics using the provided collector
// This allows for dependency injection and easier testing
func CollectSystemMetricsWithCollector(collector SystemMetricsCollector) (string, error) {
	var allMetrics []string

	// Collect CPU metrics
	cpuMetrics, err := collector.CollectCPUMetrics()
	if err != nil {
		return "", err
	}
	allMetrics = append(allMetrics, cpuMetrics...)

	// Collect memory metrics
	memMetrics, err := collector.CollectMemoryMetrics()
	if err != nil {
		return "", err
	}
	allMetrics = append(allMetrics, memMetrics...)

	// Collect runtime metrics
	runtimeMetrics := collector.CollectRuntimeMetrics()
	allMetrics = append(allMetrics, runtimeMetrics...)

	// Join all metrics with newlines and add a trailing newline
	return strings.Join(allMetrics, "\n") + "\n", nil
}
