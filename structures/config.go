package structures

import (
	"crypto/rand"
	"encoding/hex"
	"os"
)

// Config from user code
type Config struct {
	PublicKey  string
	PrivateKey string
	Name       string
	ServerName *string
	Node       *string
	Proxy      string
}

// GenerateServerName with random values
func (config *Config) GenerateServerName() {
	realHostname, err := os.Hostname()
	random := randomHex(5)
	if err != nil {
		config.ServerName = &random
		return
	}
	serverName := realHostname + "_" + random
	config.ServerName = &serverName
}

func randomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes)
}
