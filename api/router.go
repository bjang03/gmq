package api

import (
	"log"
	stdhttp "net/http"

	"github.com/bjang03/gmq/utils"
)

// SetupRouter 设置路由
func SetupRouter(mux *utils.ServeMux) {
	// 启动WebSocket广播协程
	StartMetricsBroadcast()

	// 静态文件服务
	fileServer := stdhttp.FileServer(stdhttp.Dir("./statics"))
	mux.HandleFunc("/statics/", "", stdhttp.StripPrefix("/statics/", fileServer))
	mux.HandleFunc("/ui/", "", stdhttp.StripPrefix("/ui/", fileServer))
	mux.Get("/", func(ctx *utils.Context) {
		stdhttp.ServeFile(ctx.W, ctx.R, "./statics/html/index.html")
	})

	// 健康检查端点
	mux.Get("/health", func(ctx *utils.Context) {
		utils.WriteJSONResponse(ctx.W, stdhttp.StatusOK, utils.Response{
			Code: 200,
			Msg:  "ok",
			Data: nil,
		})
	})

	// 发布消息端点
	mux.Post("/publish", Publish)

	// 发布延迟消息端点
	mux.Post("/publishDelay", PublishDelay)

	// 订阅消息端点
	mux.Post("/subscribe", Subscribe)

	// WebSocket 订阅端点
	mux.Get("/ws/subscribe", WSSubscribeHandler)

	// WebSocket 指标端点
	mux.Get("/ws/metrics", WSMetricsHandler)
}

// PrintRoutes 打印所有注册的路由
func PrintRoutes(mux *utils.ServeMux) {
	log.Println("========== 注册的路由 ==========")
	routes := mux.GetRoutes()
	for _, r := range routes {
		if r.Method == "" {
			log.Printf("ALL  %s", r.Pattern)
		} else {
			log.Printf("%s %s", r.Method, r.Pattern)
		}
	}
	log.Println("================================")
}
