package web

import (
	"github.com/bjang03/gmq/web/controller"
	"github.com/bjang03/gmq/web/middleware"
	"github.com/gin-gonic/gin"
)

var ginServer *gin.Engine

func init() {
	gin.SetMode(gin.ReleaseMode)
	ginServer = gin.Default()
	Use(middleware.ResponseMiddleware())

	// 注册路由
	Post("/publish", controller.Publish)
	Post("/subscribe", controller.Subscribe)
	Get("/subscribe", controller.Subscribe)

	go func() {
		_ = Run(":1688")
	}()
}
