package main

import (
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

	// 阻塞等待关闭信号，触发优雅关闭
	api.StartGmqWithGracefulShutdown(10)
}
