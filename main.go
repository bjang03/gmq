package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/bjang03/gmq/api"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/mq"
)

func main() {
	api.InitGmq()
	core.GmqRegister("redis", &mq.RedisConn{
		Url:  "localhost",
		Port: "6379",
	})
	core.GmqRegister("nats", &mq.NatsConn{
		Url:  "localhost",
		Port: "4222",
	})
	core.GmqRegister("rabbit", &mq.RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	})
	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	api.Shutdown()
}
