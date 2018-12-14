package metrics

import (
	"runtime"
	"syscall"

	"github.com/keymetrics/pm2-io-apm-go/structures"
)

// MetricsMemStats is a structure to simplify storage of mem values
type MetricsMemStats struct {
	Initied   bool
	NumGC     *structures.Metric
	LastNumGC float64

	NumMallocs     *structures.Metric
	LastNumMallocs float64

	NumFree     *structures.Metric
	LastNumFree float64

	HeapAlloc *structures.Metric

	Pause     *structures.Metric
	LastPause float64
}

type RuntimeStats struct {
	VolontarySwitchs   *structures.Metric
	InvolontarySwitchs *structures.Metric
	SoftPageFault      *structures.Metric
	HardPageFault      *structures.Metric
}

// GlobalMetricsMemStats store current and last mem stats
var GlobalMetricsMemStats MetricsMemStats

// GlobalRuntimeStats store runtime stats
var GlobalRuntimeStats RuntimeStats

// GoRoutines create a func metric who return number of current GoRoutines
func GoRoutines() *structures.Metric {
	metric := structures.CreateFuncMetric("GoRoutines", "metric", "routines", func() float64 {
		return float64(runtime.NumGoroutine())
	})
	return &metric
}

// CgoCalls create a func metric who return number of current C calls of last second
func CgoCalls() *structures.Metric {
	last := runtime.NumCgoCall()
	metric := structures.CreateFuncMetric("CgoCalls/sec", "metric", "calls/sec", func() float64 {
		calls := runtime.NumCgoCall()
		v := calls - last
		last = calls
		return float64(v)
	})
	return &metric
}

// InitInternalMetrics create metrics
func InitInternalMetrics() {
	numGC := structures.CreateMetric("GCRuns/sec", "metric", "runs")
	numMalloc := structures.CreateMetric("mallocs/sec", "metric", "mallocs")
	numFree := structures.CreateMetric("free/sec", "metric", "frees")
	heapAlloc := structures.CreateMetric("heapAlloc", "metric", "bytes")
	pause := structures.CreateMetric("Pause/sec", "metric", "ns/sec")

	GlobalMetricsMemStats = MetricsMemStats{
		Initied:    true,
		NumGC:      &numGC,
		NumMallocs: &numMalloc,
		NumFree:    &numFree,
		HeapAlloc:  &heapAlloc,
		Pause:      &pause,
	}

	volontarySwitchs := structures.CreateMetric("VolontarySwitchs", "metric", "switches")
	involontarySwitchs := structures.CreateMetric("InvolontarySwitchs", "metric", "switches")
	softPageFault := structures.CreateMetric("SoftPageFaults", "metric", "faults")
	hardPageFault := structures.CreateMetric("HardPageFaults", "metric", "faults")

	GlobalRuntimeStats = RuntimeStats{
		VolontarySwitchs:   &volontarySwitchs,
		InvolontarySwitchs: &involontarySwitchs,
		SoftPageFault:      &softPageFault,
		HardPageFault:      &hardPageFault,
	}
}

// Handler write values in MemStats metrics
func Handler() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	GlobalMetricsMemStats.NumGC.Set(float64(stats.NumGC) - GlobalMetricsMemStats.LastNumGC)
	GlobalMetricsMemStats.LastNumGC = float64(stats.NumGC)

	GlobalMetricsMemStats.NumMallocs.Set(float64(stats.Mallocs) - GlobalMetricsMemStats.LastNumMallocs)
	GlobalMetricsMemStats.LastNumMallocs = float64(stats.Mallocs)

	GlobalMetricsMemStats.NumFree.Set(float64(stats.Frees) - GlobalMetricsMemStats.LastNumFree)
	GlobalMetricsMemStats.LastNumFree = float64(stats.Frees)

	GlobalMetricsMemStats.HeapAlloc.Set(float64(stats.HeapAlloc))

	GlobalMetricsMemStats.Pause.Set(float64(stats.PauseTotalNs) - GlobalMetricsMemStats.LastPause)
	GlobalMetricsMemStats.LastPause = float64(stats.PauseTotalNs)

	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	GlobalRuntimeStats.VolontarySwitchs.Set(float64(ru.Nvcsw))
	GlobalRuntimeStats.InvolontarySwitchs.Set(float64(ru.Nivcsw))
	GlobalRuntimeStats.SoftPageFault.Set(float64(ru.Minflt))
	GlobalRuntimeStats.HardPageFault.Set(float64(ru.Majflt))
}
