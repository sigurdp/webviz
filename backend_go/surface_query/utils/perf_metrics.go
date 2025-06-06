package utils

import "fmt"

type metricEntry struct {
	metricName  string
	duration_ms int64
}

type PerfMetrics struct {
	perfTimer *PerfTimer
	metrics   []metricEntry
}

func NewPerfMetrics() *PerfMetrics {
	return &PerfMetrics{
		perfTimer: NewPerfTimer(),
		metrics:   []metricEntry{},
	}
}

func (pm *PerfMetrics) SetMetric(metricName string, duration_ms int64) {
	pm.metrics = append(pm.metrics, metricEntry{
		metricName:  metricName,
		duration_ms: duration_ms,
	})
}

func (pm *PerfMetrics) RecordLap(metricName string) {
	duration_ms := pm.perfTimer.Lap_ms()
	pm.SetMetric(metricName, duration_ms)
}

func (pm *PerfMetrics) RecordElapsed(metricName string) {
	duration_ms := pm.perfTimer.Elapsed_ms()
	pm.SetMetric(metricName, duration_ms)
}

func (pm *PerfMetrics) ToString(includeTotalElapsed bool) string {
	totalElapsed_ms := pm.perfTimer.Elapsed_ms()

	metricsStr := ""
	for i, entry := range pm.metrics {
		if i > 0 {
			metricsStr += ", "
		}
		metricsStr += fmt.Sprintf("%s=%dms", entry.metricName, entry.duration_ms)
	}

	if includeTotalElapsed {
		return fmt.Sprintf("%dms (%s)", totalElapsed_ms, metricsStr)
	}

	return metricsStr
}
