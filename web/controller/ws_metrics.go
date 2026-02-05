package controller

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	// 管理所有活跃的WebSocket连接
	wsClients    = make(map[*websocket.Conn]bool)
	wsClientsMux sync.RWMutex
)

// MetricsMessage WebSocket消息结构
type MetricsMessage struct {
	Type    string                   `json:"type"`
	Payload map[string]*core.Metrics `json:"payload"`
}

// WSMetricsHandler WebSocket指标处理器
func WSMetricsHandler(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// 注册客户端
	wsClientsMux.Lock()
	wsClients[conn] = true
	wsClientsMux.Unlock()

	defer func() {
		wsClientsMux.Lock()
		delete(wsClients, conn)
		wsClientsMux.Unlock()
	}()

	// 发送初始数据
	if err := sendMetrics(conn); err != nil {
		return
	}

	// 设置读取超时和心跳检测
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// 启动定时发送指标数据
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// 用于接收客户端消息的通道
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := sendMetrics(conn); err != nil {
				return
			}
		}
	}
}

// sendMetrics 发送指标数据给指定连接
func sendMetrics(conn *websocket.Conn) error {
	metrics := getAllMetrics()
	msg := MetricsMessage{
		Type:    "metrics",
		Payload: metrics,
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteJSON(msg)
}

// getAllMetrics 获取所有消息队列的监控指标
func getAllMetrics() map[string]*core.Metrics {
	ctx := context.Background()
	result := make(map[string]*core.Metrics)

	for name, pipeline := range core.GetAllGmq() {
		metrics := pipeline.GetMetrics(ctx)
		result[name] = metrics
	}

	return result
}
