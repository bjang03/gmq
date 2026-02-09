package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// SetupRouter 设置路由
func SetupRouter(engine *gin.Engine) {
	// 启动WebSocket广播协程
	StartMetricsBroadcast()

	// 静态文件服务
	engine.Static("/statics", "./statics")
	engine.Static("/ui", "./statics")
	engine.StaticFile("/", "./statics/html/index.html")

	// 注册业务路由 - 使用gin默认注册方式
	engine.POST("/publish", PublishHandler)
	engine.POST("/subscribe", SubscribeHandler)

	// WebSocket 订阅路由
	engine.GET("/ws/subscribe", WSSubscribeHandler)

	// WebSocket 指标推送路由
	engine.GET("/ws/metrics", WSMetricsHandler)

	// 健康检查端点
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, map[string]string{"status": "ok"})
	})

	// 输出已注册的路由
	fmt.Println("\n========== 已注册路由 ==========")
	routes := engine.Routes()
	for _, route := range routes {
		fmt.Printf("%-8s %s\n", route.Method, route.Path)
	}
	fmt.Println("================================\n")
}

// PublishHandler 发布消息处理器
func PublishHandler(c *gin.Context) {
	var req PublishReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 400,
			"msg":  "Invalid request: " + err.Error(),
			"data": nil,
		})
		return
	}

	res, err := Publish(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 500,
			"msg":  err.Error(),
			"data": nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": res,
	})
}

// SubscribeHandler 订阅消息处理器
func SubscribeHandler(c *gin.Context) {
	var req SubscribeReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 400,
			"msg":  "Invalid request: " + err.Error(),
			"data": nil,
		})
		return
	}

	res, err := Subscribe(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 500,
			"msg":  err.Error(),
			"data": nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": res,
	})
}
