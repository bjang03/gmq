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

	broadcastOnce sync.Once
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

	// 设置写入超时
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// 注册客户端
	wsClientsMux.Lock()
	wsClients[conn] = true
	wsClientsMux.Unlock()

	// 发送初始数据
	if err := sendMetrics(conn); err != nil {
		conn.Close()
		wsClientsMux.Lock()
		delete(wsClients, conn)
		wsClientsMux.Unlock()
		return
	}

	// 设置读取超时和心跳检测
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// 保持连接，等待客户端关闭或错误
	for {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}

	// 清理连接
	conn.Close()
	wsClientsMux.Lock()
	delete(wsClients, conn)
	wsClientsMux.Unlock()
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(map[string]*core.Metrics)
	for name, pipeline := range core.GetAllGmq() {
		metrics := pipeline.GetMetrics(ctx)
		result[name] = metrics
	}

	return result
}

// StartMetricsBroadcast 启动指标广播协程
func StartMetricsBroadcast() {
	broadcastOnce.Do(func() {
		go metricsBroadcastLoop()
	})
}

// metricsBroadcastLoop 指标广播循环
func metricsBroadcastLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		metrics := getAllMetrics()
		broadcastMetrics(metrics)
	}
}

// broadcastMetrics 向所有客户端广播指标
func broadcastMetrics(metrics map[string]*core.Metrics) {
	wsClientsMux.RLock()
	clients := make([]*websocket.Conn, 0, len(wsClients))
	for client := range wsClients {
		clients = append(clients, client)
	}
	wsClientsMux.RUnlock()

	if len(clients) == 0 {
		return
	}

	msg := MetricsMessage{
		Type:    "metrics",
		Payload: metrics,
	}

	for _, client := range clients {
		client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := client.WriteJSON(msg); err != nil {
			// 写入失败，关闭连接
			client.Close()
			wsClientsMux.Lock()
			delete(wsClients, client)
			wsClientsMux.Unlock()
		}
	}
}
