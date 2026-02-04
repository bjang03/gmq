package web

import (
	"github.com/bjang03/gmq/web/controller"
)

func init() {
	// 注册路由
	HttpServer.Post("/publish", controller.Publish)
	HttpServer.Get("/subscribe", controller.Subscribe)

	go func() {
		_ = HttpServer.Run(":1688")
	}()
}
