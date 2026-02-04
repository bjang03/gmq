package web

import (
	"github.com/bjang03/gmq/web/controller"
	"github.com/bjang03/gmq/web/middleware"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.Use(middleware.ResponseMiddleware())
	Get("/subscribe", controller.Publish)
	r.GET("/subscribe", middleware.Wrap(controller.Subscribe))
	go func() {
		_ = r.Run(":1688")
	}()
}
