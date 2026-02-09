package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/web"
	"github.com/bjang03/gmq/web/controller"
)

func main() {
	core.GmqRegister("nats", &components.NatsConn{
		Url:            "nats://localhost:4222",
		Timeout:        10,
		ReconnectWait:  5,
		MaxReconnects:  -1,
		MessageTimeout: 30,
	})
	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := web.HttpServer.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}
	if err := core.Shutdown(ctx); err != nil {
		log.Printf("Error during MQ shutdown: %v", err)
	}
	controller.StopMetricsBroadcast()
}
