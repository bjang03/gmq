package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bjang03/gmq/api"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/mq"
	"github.com/gin-gonic/gin"
)

func main() {
	core.GmqRegister("redis", &mq.RedisConn{
		Url: "localhost:6379",
	})
	core.GmqRegister("nats", &mq.NatsConn{
		Url: "nats://localhost:4222",
	})
	core.GmqRegister("rabbit", &mq.RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	})
	// 设置 Gin 路由
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	api.SetupRouter(router)

	// 打印注册的路由
	api.PrintRoutes(router)

	// 创建 HTTP 服务器
	srv := &http.Server{
		Addr:    ":1688",
		Handler: router,
	}

	// 启动 HTTP 服务器
	go func() {
		log.Println("HTTP server starting on :1688")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}
	if err := core.Shutdown(ctx); err != nil {
		log.Printf("Error during MQ shutdown: %v", err)
	}
	api.StopMetricsBroadcast()
	log.Println("Server stopped")
}
