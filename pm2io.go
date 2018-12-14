package pm2io

import (
	"os"
	"runtime"
	"time"

	"github.com/keymetrics/pm2-io-apm-go/features"
	"github.com/keymetrics/pm2-io-apm-go/features/metrics"
	"github.com/keymetrics/pm2-io-apm-go/services"
	"github.com/keymetrics/pm2-io-apm-go/structures"
	"github.com/shirou/gopsutil/process"
)

var version = "0.0.1"

// Pm2Io to config and access all services
type Pm2Io struct {
	Config *structures.Config

	Notifier    *features.Notifier
	transporter *services.Transporter

	StatusOverrider func() *structures.Status

	hostname  string
	startTime time.Time
}

// Start and prepare services + profiling
func (pm2io *Pm2Io) Start() {
	if pm2io.Config.ServerName == nil {
		pm2io.Config.GenerateServerName()
	}

	node := pm2io.Config.Node
	defaultNode := "api.cloud.pm2.io"
	if node == nil {
		node = &defaultNode
	}

	pm2io.transporter = services.NewTransporter(pm2io.Config, version, pm2io.hostname, *pm2io.Config.ServerName, *node)
	pm2io.Notifier = &features.Notifier{
		Transporter: pm2io.transporter,
	}
	metrics.InitInternalMetrics()
	services.AttachHandler(metrics.Handler)

	services.AddMetric(metrics.GoRoutines())
	services.AddMetric(metrics.CgoCalls())
	services.AddMetric(metrics.GlobalMetricsMemStats.NumGC)
	services.AddMetric(metrics.GlobalMetricsMemStats.NumMallocs)
	services.AddMetric(metrics.GlobalMetricsMemStats.NumFree)
	services.AddMetric(metrics.GlobalMetricsMemStats.HeapAlloc)
	services.AddMetric(metrics.GlobalMetricsMemStats.Pause)

	services.AddMetric(metrics.GlobalRuntimeStats.VolontarySwitchs)
	services.AddMetric(metrics.GlobalRuntimeStats.InvolontarySwitchs)
	services.AddMetric(metrics.GlobalRuntimeStats.SoftPageFault)
	services.AddMetric(metrics.GlobalRuntimeStats.HardPageFault)

	services.AddAction(&structures.Action{
		ActionName: "km:heapdump",
		ActionType: "internal",
		Callback: func(payload map[string]interface{}) string {
			data, err := features.HeapDump()
			if err != nil {
				pm2io.Notifier.Error(err)
			}
			pm2io.transporter.Send("profilings", structures.NewProfilingResponse(data, "heapdump"))
			return ""
		},
	})
	services.AddAction(&structures.Action{
		ActionName: "km:cpu:profiling:start",
		ActionType: "internal",
		Callback: func(payload map[string]interface{}) string {
			err := features.StartCPUProfile()
			if err != nil {
				pm2io.Notifier.Error(err)
			}

			if payload["opts"] != nil {
				go func() {
					timeout := payload["opts"].(map[string]interface{})["timeout"]
					time.Sleep(time.Duration(timeout.(float64)) * time.Millisecond)
					r, err := features.StopCPUProfile()
					if err != nil {
						pm2io.Notifier.Error(err)
					}
					pm2io.transporter.Send("profilings", structures.NewProfilingResponse(r, "cpuprofile"))
				}()
			}
			return ""
		},
	})
	services.AddAction(&structures.Action{
		ActionName: "km:cpu:profiling:stop",
		ActionType: "internal",
		Callback: func(payload map[string]interface{}) string {
			r, err := features.StopCPUProfile()
			if err != nil {
				pm2io.Notifier.Error(err)
			}
			pm2io.transporter.Send("profilings", structures.NewProfilingResponse(r, "cpuprofile"))
			return ""
		},
	})

	pm2io.startTime = time.Now()
	pm2io.transporter.Connect()

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			pm2io.SendStatus()
		}
	}()
}

// RestartTransporter including webSocket connection
func (pm2io *Pm2Io) RestartTransporter() {
	pm2io.transporter.CloseAndReconnect()
}

// SendStatus of current state
func (pm2io *Pm2Io) SendStatus() {
	if pm2io.StatusOverrider != nil {
		pm2io.transporter.Send("status", pm2io.StatusOverrider())
		return
	}
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		pm2io.Notifier.Error(err)
	}
	cp, err := metrics.CPUPercent()
	if err != nil {
		pm2io.Notifier.Error(err)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	kmProc := []structures.Process{}

	kmProc = append(kmProc, structures.Process{
		Pid:         p.Pid,
		Name:        pm2io.Config.Name,
		Interpreter: "golang",
		RestartTime: 0,
		CreatedAt:   pm2io.startTime.UnixNano() / int64(time.Millisecond),
		ExecMode:    "fork_mode",
		Watching:    false,
		PmUptime:    pm2io.startTime.UnixNano() / int64(time.Millisecond),
		Status:      "online",
		PmID:        0,
		CPU:         cp,
		Memory:      m.Alloc,
		NodeEnv:     "production",
		AxmActions:  services.Actions,
		AxmMonitor:  services.GetMetricsAsMap(),
		AxmOptions: structures.Options{
			HeapDump:     true,
			Profiling:    true,
			CustomProbes: true,
			Apm: structures.Apm{
				Type:    "golang",
				Version: version,
			},
		},
	})

	pm2io.transporter.Send("status", structures.Status{
		Process: kmProc,
		Server: structures.Server{
			Loadavg:     metrics.CPULoad(),
			TotalMem:    metrics.TotalMem(),
			Hostname:    pm2io.hostname,
			Uptime:      (time.Now().UnixNano()-pm2io.startTime.UnixNano())/int64(time.Millisecond) + 600000,
			Pm2Version:  version,
			Type:        runtime.GOOS,
			Interaction: true,
			CPU: structures.CPU{
				Number: runtime.NumCPU(),
				Info:   metrics.CPUName(),
			},
			NodeVersion: runtime.Version(),
		},
	})
}

// Panic notify KM then panic
func (pm2io *Pm2Io) Panic(err error) {
	pm2io.Notifier.Error(err)
	panic(err)
}
