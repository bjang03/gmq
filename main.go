package main

import (
	"log"

	"github.com/bjang03/gmq/web"
	"github.com/bjang03/gmq/web/controller"
)

func main() {
	log.Println("GMQ Server starting...")

	// 注册业务路由
	web.HttpServer.Post("/publish", controller.Publish)
	web.HttpServer.Get("/subscribe", controller.Subscribe)
	web.HttpServer.Get("/metrics", controller.GetMetrics)
	web.HttpServer.Get("/metrics/all", controller.GetAllMetrics)

	// 注册静态文件路由
	web.RegisterStaticRoutes(web.HttpServer.GetEngine())

	// 配置并启动服务器
	config := &web.ServerConfig{
		PrintRoutes: true, // 开启/关闭路由打印
		Addr:        ":1688",
	}
	web.HttpServer.SetPrintRoutes(config.PrintRoutes)
	web.HttpServer.Run(config.Addr)
	select {}
}
