package utils

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/bjang03/gmq/config"
	"github.com/gorilla/websocket"
)

// WebSocketManager WebSocket连接管理器
type WebSocketManager struct {
	connections map[*websocket.Conn]bool
	mu          sync.RWMutex

	upgrader     *websocket.Upgrader
	upgraderOnce sync.Once

	broadcastStopCh chan struct{}
	broadcastStopMu sync.Mutex
}

// BroadcastStopCh 获取广播停止通道(外部监听用)
func (m *WebSocketManager) BroadcastStopCh() <-chan struct{} {
	return m.broadcastStopCh
}

// MessageType WebSocket消息类型
type MessageType string

const (
	MessageTypeMetrics MessageType = "metrics"
)

// WebSocketMessage WebSocket统一消息结构
type WebSocketMessage struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

// MessageHandler 消息处理函数类型
type MessageHandler func(conn *websocket.Conn, messageType int, data []byte) error

// NewWebSocketManager 创建WebSocket管理器
func NewWebSocketManager() *WebSocketManager {
	return &WebSocketManager{
		connections: make(map[*websocket.Conn]bool),
	}
}

// GetUpgrader 获取WebSocket upgrader(单例模式)
func (m *WebSocketManager) GetUpgrader() *websocket.Upgrader {
	m.upgraderOnce.Do(func() {
		wsCfg := config.GetWebSocketConfig()
		m.upgrader = &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  wsCfg.ReadBufferSize,
			WriteBufferSize: wsCfg.WriteBufferSize,
		}
	})
	return m.upgrader
}

// RegisterConnection 注册连接
func (m *WebSocketManager) RegisterConnection(conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections[conn] = true
}

// UnregisterConnection 取消注册连接
func (m *WebSocketManager) UnregisterConnection(conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.connections, conn)
}

// Broadcast 广播消息到所有连接
func (m *WebSocketManager) Broadcast(message *WebSocketMessage) error {
	m.mu.RLock()
	if len(m.connections) == 0 {
		m.mu.RUnlock()
		return nil
	}

	// 复制连接列表,避免在锁内进行网络操作
	connections := make([]*websocket.Conn, 0, len(m.connections))
	for conn := range m.connections {
		connections = append(connections, conn)
	}
	m.mu.RUnlock()

	// 收集失败的连接
	var failedConns []*websocket.Conn
	for _, conn := range connections {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteJSON(message); err != nil {
			log.Printf("[WebSocket] Failed to send message to connection: %v", err)
			failedConns = append(failedConns, conn)
		}
	}

	// 移除失败的连接
	if len(failedConns) > 0 {
		m.mu.Lock()
		for _, conn := range failedConns {
			conn.Close()
			delete(m.connections, conn)
		}
		m.mu.Unlock()
	}

	return nil
}

// Send 发送消息到指定连接
func (m *WebSocketManager) Send(conn *websocket.Conn, message *WebSocketMessage) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteJSON(message)
}

// GetConnectionCount 获取连接数量
func (m *WebSocketManager) GetConnectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.connections)
}

// CloseAll 关闭所有连接
func (m *WebSocketManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for conn := range m.connections {
		conn.Close()
	}
	m.connections = make(map[*websocket.Conn]bool)
}

// StartBroadcastLoop 启动广播循环
func (m *WebSocketManager) StartBroadcastLoop(interval time.Duration, broadcastFunc func() *WebSocketMessage) {
	m.broadcastStopMu.Lock()
	if m.broadcastStopCh != nil {
		m.broadcastStopMu.Unlock()
		return
	}
	m.broadcastStopCh = make(chan struct{})
	m.broadcastStopMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if msg := broadcastFunc(); msg != nil {
					m.Broadcast(msg)
				}
			case <-m.broadcastStopCh:
				return
			}
		}
	}()
}

// StopBroadcastLoop 停止广播循环
func (m *WebSocketManager) StopBroadcastLoop() {
	m.broadcastStopMu.Lock()
	defer m.broadcastStopMu.Unlock()

	if m.broadcastStopCh != nil {
		close(m.broadcastStopCh)
		m.broadcastStopCh = nil
	}
}

// HandleConnection 处理WebSocket连接(统一管理心跳、停止监听、生命周期)
func (m *WebSocketManager) HandleConnection(
	w http.ResponseWriter,
	r *http.Request,
	handler MessageHandler,
	initialData *WebSocketMessage,
) (*websocket.Conn, error) {
	wsCfg := config.GetWebSocketConfig()
	pingInterval := time.Duration(wsCfg.PingInterval) * time.Second
	readTimeout := time.Duration(wsCfg.ReadTimeout) * time.Second

	conn, err := m.Upgrade(w, r)
	if err != nil {
		return nil, err
	}

	// 注册连接
	m.RegisterConnection(conn)

	// 发送初始数据
	if initialData != nil {
		if err := m.Send(conn, initialData); err != nil {
			m.UnregisterConnection(conn)
			SafeClose(conn)
			return nil, err
		}
	}

	// 创建连接上下文
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	// 监听全局停止信号
	go func() {
		select {
		case <-m.broadcastStopCh:
			connCancel()
		case <-connCtx.Done():
		}
	}()

	// 设置心跳检测
	if err := m.SetupPingPong(conn, pingInterval, readTimeout, connCtx); err != nil {
		m.UnregisterConnection(conn)
		SafeClose(conn)
		return nil, err
	}

	// 启动连接处理协程，异步处理消息和生命周期管理
	go func() {
		defer func() {
			m.UnregisterConnection(conn)
			SafeClose(conn)
		}()

		// 处理连接
		for {
			messageType, data, err := ReadMessage(conn, readTimeout)
			if err != nil {
				log.Printf("[WebSocket] Read error: %v", err)
				break
			}

			if handler != nil {
				if err := handler(conn, messageType, data); err != nil {
					log.Printf("[WebSocket] Handler error: %v", err)
					break
				}
			}
		}
	}()

	return conn, nil
}

// SetupPingPong 设置连接的心跳检测
func (m *WebSocketManager) SetupPingPong(conn *websocket.Conn, pingInterval, readTimeout time.Duration, connCtx context.Context) error {
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
			case <-connCtx.Done():
				return
			}
		}
	}()

	// 设置读取超时和Pong处理器
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	return nil
}

// Upgrade 升级HTTP连接到WebSocket
func (m *WebSocketManager) Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	return m.GetUpgrader().Upgrade(w, r, nil)
}

// WriteTextMessage 写入文本消息
func WriteTextMessage(conn *websocket.Conn, message []byte) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteMessage(websocket.TextMessage, message)
}

// ReadMessage 读取消息(带超时)
func ReadMessage(conn *websocket.Conn, timeout time.Duration) (int, []byte, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	return conn.ReadMessage()
}

// SafeClose 安全关闭连接
func SafeClose(conn *websocket.Conn) {
	if conn != nil {
		conn.Close()
	}
}

// SafeCloseWrite 安全写入消息(处理Ping/Pong等控制消息)
func SafeCloseWrite(conn *websocket.Conn, messageType int, data []byte) error {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return conn.WriteMessage(messageType, data)
}
