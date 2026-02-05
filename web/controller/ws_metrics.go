package controller

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WebSocketMetricsHandler WebSocket指标推送处理器
func WebSocketMetricsHandler(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	// 设置ping/pong处理
	ws.SetPingHandler(func(appData string) error {
		err := ws.WriteMessage(websocket.PongMessage, []byte(appData))
		if err != nil {
			return err
		}
		return nil
	})

	// 心跳 ticker
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()

	// 指标推送 ticker
	pushTicker := time.NewTicker(2 * time.Second)
	defer pushTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			// 发送心跳
			if err := ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-pushTicker.C:
			// 获取所有指标数据
			result := make(map[string]*core.Metrics)
			for name, pipeline := range core.GetAllGmq() {
				metrics := pipeline.GetMetrics(c.Request.Context())
				result[name] = metrics
			}

			// 构造推送消息
			message := map[string]interface{}{
				"type":      "metrics",
				"payload":   result,
				"timestamp": time.Now().Unix(),
			}

			// 发送JSON消息
			messageBytes, err := json.Marshal(message)
			if err != nil {
				continue
			}

			if err := ws.WriteMessage(websocket.TextMessage, messageBytes); err != nil {
				return
			}
		}
	}
}
