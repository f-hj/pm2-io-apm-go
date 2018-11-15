![](assets/gopher-pm2.png)

# PM2/io APM Golang
This PM2 module is standalone and must include a public/private key to working properly with PM2 Plus.

## Init

```golang
package main

import (
  "github.com/keymetrics/pm2-io-apm-go/services"
  "github.com/keymetrics/pm2-io-apm-go/structures"
)

func main() {
  // Create PM2 connector
  pm2 := pm2io.Pm2Io{
    Config: &structures.Config{
      PublicKey:  "myPublic",
      PrivateKey: "myPrivate",
      Name:       "Golang app",
    },
  }
  
  // Add an action who can be triggered from PM2 Plus
  services.AddAction(&structures.Action{
    ActionName: "Get env",
    Callback: func(_ map[string]interface{}) string {
      return strings.Join(os.Environ(), "\n")
    },
  })
  
  // Add a function metric who will be aggregated
  nbd := structures.CreateFuncMetric("Function metric", "metric", "stable/integer", func() float64 {
    // For a FuncMetric, this will be called every ~1sec
    return float64(10)
  })
  services.AddMetric(&nbd)
  
  // Add a normal metric
  nbreq := structures.CreateMetric("Incrementable", "metric", "increments")
  services.AddMetric(&nbreq)

  // Goroutine who increment the value each 4 seconds
  go func() {
    ticker := time.NewTicker(4 * time.Second)
    for {
      <-ticker.C
      nbreq.Value++

      // Log to PM2 Plus
      pm2io.Notifier.Log("Value incremented")
    }
  }()

  // Start the connection to PM2 Plus servers
  pm2.Start()

  // Log that we started the program (optional, just for example)
  pm2io.Notifier.Log("Started")
  
  // Wait infinitely (for example)
  <-time.After(time.Duration(math.MaxInt64))
}
```

## Send error then panic
You can send an error to PM2 Plus before panic your program
```golang
name, err := os.Hostname()
if err != nil {
  pm2io.Panic(err)
  // The program will crash with panic just after the message is sent
}
```

## Use a proxy for APM requests and WebSocket
```golang
  pm2io.Pm2Io{
    Config: &structures.Config{
      PublicKey:  "myPublic",
      PrivateKey: "myPrivate",
      Name:       "Golang app",
      Proxy:      "socks5://localhost:1080/",
    },
  }
```

## Connect logrus to PM2 Plus
If you are using logrus, this is an example to send logs and create exceptions on PM2 Plus when you log an error

```golang
package main

import (
  pm2io "github.com/keymetrics/pm2-io-apm-go"
  "github.com/sirupsen/logrus"
)

// HookLog will send logs to PM2 Plus
type HookLog struct {
  Pm2 *pm2io.Pm2Io
}

// HookErr will send all errors to PM2 Plus
type HookErr struct {
  Pm2 *pm2io.Pm2Io
}

// Fire event
func (hook *HookLog) Fire(e *logrus.Entry) error {
  str, err := e.String()
  if err == nil {
    hook.Pm2.Notifier.Log(str)
  }
  return err
}

// Levels for all possible logs
func (*HookLog) Levels() []logrus.Level {
  return logrus.AllLevels
}

// Fire an error and notify it as exception
func (hook *HookErr) Fire(e *logrus.Entry) error {
  if err, ok := e.Data["error"].(error); ok {
    hook.Pm2.Notifier.Error(err)
  }
  return nil
}

// Levels only for errors
func (*HookErr) Levels() []logrus.Level {
  return []logrus.Level{logrus.ErrorLevel}
}

func main() {
  pm2 := pm2io.Pm2Io{
    Config: &structures.Config{
      PublicKey:  "myPublic",
      PrivateKey: "myPrivate",
      Name:       "Golang app",
    },
  }

  logrus.AddHook(&HookLog{
    Pm2: pm2,
  })
  logrus.AddHook(&HookErr{
    Pm2: pm2,
  })
}
```

## Known problems
### x509: unknown authority
You must have the **Let's Encrypt Authority X3** certificate on your machine/container to connect to our backend

#### Alpine based container
```sh
apk add -U --no-cache ca-certificates
```

#### Debian based container
```sh
apt-get install -y ca-certificates
```