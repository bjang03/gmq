package main

import (
	"github.com/bjang03/gmq/web"
	"github.com/bjang03/gmq/web/controller"
)

func main() {
	// 注册业务路由
	web.HttpServer.Post("/publish", controller.Publish)
	web.HttpServer.Get("/subscribe", controller.Subscribe)

	// 注册静态文件路由
	web.RegisterStaticRoutes(web.HttpServer.GetEngine())

	// WebSocket指标推送路由（需要直接注册，绕过ControllerAdapter）
	web.HttpServer.GetEngine().GET("/ws/metrics", controller.WSMetricsHandler)

	web.HttpServer.SetPrintRoutes(true)
	web.HttpServer.Run(":1688")
	select {}
}
