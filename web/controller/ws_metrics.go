package controller

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bjang03/gmq/config"
	"github.com/bjang03/gmq/core"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	// 管理所有活跃的WebSocket连接
	wsClients    = make(map[*websocket.Conn]bool)
	wsClientsMux sync.RWMutex

	broadcastOnce   sync.Once
	broadcastStopCh chan struct{} // 广播停止信号
)

// MetricsMessage WebSocket消息结构
type MetricsMessage struct {
	Type    string                   `json:"type"`
	Payload map[string]*core.Metrics `json:"payload"`
}

// initWebSocketUpgrader 初始化 WebSocket upgrader（使用配置）
func initWebSocketUpgrader() websocket.Upgrader {
	wsCfg := config.GetWebSocketConfig()
	return websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		ReadBufferSize:  wsCfg.ReadBufferSize,
		WriteBufferSize: wsCfg.WriteBufferSize,
	}
}

// WSMetricsHandler WebSocket指标处理器
func WSMetricsHandler(c *gin.Context) {
	upgrader := initWebSocketUpgrader()
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	wsCfg := config.GetWebSocketConfig()
	pingInterval := time.Duration(wsCfg.PingInterval) * time.Second
	readTimeout := time.Duration(wsCfg.ReadTimeout) * time.Second

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
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	// 启动心跳发送协程
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-broadcastStopCh:
				return
			}
		}
	}()

	// 保持连接，等待客户端关闭或错误
	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
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
		broadcastStopCh = make(chan struct{})
		go metricsBroadcastLoop()
	})
}

// StopMetricsBroadcast 停止指标广播协程
func StopMetricsBroadcast() {
	if broadcastStopCh != nil {
		close(broadcastStopCh)
		broadcastStopCh = nil
	}
}

// metricsBroadcastLoop 指标广播循环
func metricsBroadcastLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := getAllMetrics()
			broadcastMetrics(metrics)
		case <-broadcastStopCh:
			return
		}
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
		// 检查连接是否仍然有效（避免竞态条件）
		wsClientsMux.RLock()
		_, exists := wsClients[client]
		wsClientsMux.RUnlock()

		if !exists {
			continue
		}

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
