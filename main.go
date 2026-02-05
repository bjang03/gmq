package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bjang03/gmq/config"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/web"
	"github.com/bjang03/gmq/web/controller"
	"github.com/gin-gonic/gin"
)

func main() {
	// 加载配置文件
	if err := config.LoadConfig("config.yml"); err != nil {
		log.Printf("Warning: failed to load config: %v, using defaults", err)
	}

	// 启动WebSocket广播协程
	controller.StartMetricsBroadcast()

	// 注册业务路由
	web.HttpServer.Post("/publish", controller.Publish)
	web.HttpServer.Get("/subscribe", controller.Subscribe)

	// 注册静态文件路由
	web.RegisterStaticRoutes(web.HttpServer.GetEngine())

	// WebSocket指标推送路由（需要直接注册，绕过ControllerAdapter）
	web.HttpServer.GetEngine().GET("/ws/metrics", controller.WSMetricsHandler)

	// 健康检查端点
	web.HttpServer.GetEngine().GET("/health", func(c *gin.Context) {
		c.JSON(200, map[string]string{"status": "ok"})
	})

	web.HttpServer.SetPrintRoutes(true)

	// 使用配置的地址启动服务器
	addr := config.GetServerAddress()
	log.Printf("Starting server on %s", addr)

	// 在协程中启动服务器
	go func() {
		if err := web.HttpServer.Run(addr); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// 优雅关闭上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 关闭所有消息队列连接
	if err := core.Shutdown(ctx); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("Server gracefully stopped")
}
