package main

import (
	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/core"
	_ "github.com/bjang03/gmq/web"
)

func main() {
	core.GmqRegister("nats", &components.NatsConn{
		URL:            "nats://localhost:4222",
		Timeout:        10,
		ReconnectWait:  5,
		MaxReconnects:  -1,
		MessageTimeout: 30,
	})
}
