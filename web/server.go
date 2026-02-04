package web

import (
	"github.com/bjang03/gmq/web/controller"
)

func init() {
	// 注册路由
	HttpServer.Post("/publish", controller.Publish)
	HttpServer.Get("/subscribe", controller.Subscribe)
	HttpServer.Get("/metrics", controller.GetMetrics)
	HttpServer.Get("/metrics/all", controller.GetAllMetrics)

	go func() {
		_ = HttpServer.Run(":1688")
	}()
}
