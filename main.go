package main

import (
	"context"

	"github.com/bjang03/gmq/api"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/utils"
)

func main() {
	core.GmqRegister("redis", &mq.RedisConn{
		Url: "localhost:6379",
	})
	core.GmqRegister("nats", &mq.NatsConn{
		Url: "localhost:4222",
	})
	core.GmqRegister("rabbit", &mq.RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	})

	// 设置路由 - 使用封装的ServeMux
	mux := utils.NewServeMux()
	api.SetupRouter(mux)

	// 打印注册的路由
	api.PrintRoutes(mux)

	// 创建服务器管理器
	manager := utils.NewServerManager(func(ctx context.Context) error {
		if err := core.Shutdown(ctx); err != nil {
			return err
		}
		api.StopMetricsBroadcast()
		return nil
	})

	// 添加服务器
	manager.AddServer(utils.NewServer(":1688", mux))

	// 启动所有服务器并支持优雅关闭
	manager.StartWithGracefulShutdown(10)
}
