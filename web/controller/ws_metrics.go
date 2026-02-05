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

	// 全局复用的 WebSocket Upgrader（问题15修复）
	wsUpgrader     *websocket.Upgrader
	wsUpgraderOnce sync.Once
)

// MetricsMessage WebSocket消息结构
type MetricsMessage struct {
	Type    string                   `json:"type"`
	Payload map[string]*core.Metrics `json:"payload"`
}

// initWebSocketUpgrader 初始化 WebSocket upgrader（使用配置，单例模式）
func initWebSocketUpgrader() *websocket.Upgrader {
	wsUpgraderOnce.Do(func() {
		wsCfg := config.GetWebSocketConfig()
		wsUpgrader = &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  wsCfg.ReadBufferSize,
			WriteBufferSize: wsCfg.WriteBufferSize,
		}
	})
	return wsUpgrader
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

	// 为每个连接创建独立的停止信号（问题1修复：使用 context 控制生命周期）
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel() // 确保连接关闭时取消 context

	// 监听广播停止信号，转发到连接级别的停止信号
	go func() {
		select {
		case <-broadcastStopCh:
			connCancel() // 广播停止时取消连接 context
		case <-connCtx.Done():
			// 连接已关闭，正常退出
		}
	}()

	// 启动心跳发送协程（使用 context 控制生命周期）
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-connCtx.Done():
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

// broadcastMetrics 向所有客户端广播指标（问题2修复：先收集失败连接，再统一删除）
func broadcastMetrics(metrics map[string]*core.Metrics) {
	wsClientsMux.Lock()
	defer wsClientsMux.Unlock()

	if len(wsClients) == 0 {
		return
	}

	msg := MetricsMessage{
		Type:    "metrics",
		Payload: metrics,
	}

	// 先收集需要移除的失败连接
	var failedClients []*websocket.Conn
	for client := range wsClients {
		client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := client.WriteJSON(msg); err != nil {
			failedClients = append(failedClients, client)
		}
	}

	// 统一关闭并删除失败连接
	for _, client := range failedClients {
		client.Close()
		delete(wsClients, client)
	}
}
