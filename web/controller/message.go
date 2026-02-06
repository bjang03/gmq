package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/config"
	"github.com/bjang03/gmq/core"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	mqClients      = make(map[*websocket.Conn]*mqClientInfo)
	mqClientsMux   sync.RWMutex
	mqUpgrader     *websocket.Upgrader
	mqUpgraderOnce sync.Once
)

// PublishMessage 发布消息结构
type PublishMessage struct {
	Data string `json:"data"` // 消息内容
}

// mqClientInfo 客户端连接信息
type mqClientInfo struct {
	serverName   string
	queueName    string
	consumerName string
	subscribed   bool
	subObj       interface{}
}

// WSConnectHandler WebSocket MQ连接处理器
// 连接时通过 URL 参数指定要订阅的队列: ?server=nats&queue=test.queue
// 客户端发送的所有消息都作为发布消息
// 服务端会推送订阅队列收到的消息
func WSConnectHandler(c *gin.Context) {
	// 获取连接参数
	serverName := c.Query("serverName")
	queueName := c.Query("queueName")

	if serverName == "" || queueName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "server and queue parameters are required"})
		return
	}

	// 升级为 WebSocket 连接
	upgrader := initMQUpgrader()
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[MQ] WebSocket upgrade failed: %v", err)
		return
	}

	wsCfg := config.GetWebSocketConfig()
	readTimeout := time.Duration(wsCfg.ReadTimeout) * time.Second

	// 创建连接上下文
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	// 创建客户端信息
	clientInfo := &mqClientInfo{
		serverName:   serverName,
		queueName:    queueName,
		consumerName: fmt.Sprintf("ws-client-%d", time.Now().UnixNano()),
	}

	// 注册客户端
	mqClientsMux.Lock()
	mqClients[conn] = clientInfo
	mqClientsMux.Unlock()

	// 发送连接成功消息
	conn.WriteJSON(map[string]interface{}{
		"type":   "connected",
		"server": serverName,
		"queue":  queueName,
	})

	log.Printf("[MQ] Client connected - Server: %s, Queue: %s, Consumer: %s", serverName, queueName, clientInfo.consumerName)

	// 创建消息通道（用于接收订阅的消息）
	msgChan := make(chan []byte, 100)
	cancelSub := make(chan struct{})

	// 订阅队列
	if err := subscribeQueue(conn, clientInfo, msgChan, connCtx); err != nil {
		log.Printf("[MQ] Failed to subscribe: %v", err)
		conn.WriteJSON(map[string]interface{}{
			"type":  "error",
			"error": err.Error(),
		})
		cleanupClient(conn, clientInfo, connCancel)
		return
	}

	// 启动消息转发协程
	go forwardMessages(conn, msgChan, cancelSub, connCtx, queueName)

	// 处理客户端发送的消息（都是发布消息）
	handleClientPublish(conn, clientInfo, connCtx, readTimeout)

	// 清理连接
	cleanupClient(conn, clientInfo, connCancel)
}

// subscribeQueue 订阅队列
func subscribeQueue(conn *websocket.Conn, clientInfo *mqClientInfo, msgChan chan []byte, connCtx context.Context) error {
	pipeline := core.GetGmq(clientInfo.serverName)
	if pipeline == nil {
		return fmt.Errorf("[%s] pipeline not found", clientInfo.serverName)
	}

	subMsg := &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName:    clientInfo.queueName,
			ConsumerName: clientInfo.consumerName,
			HandleFunc: func(ctx context.Context, message any) error {
				var data []byte
				switch v := message.(type) {
				case []byte:
					data = v
				case string:
					data = []byte(v)
				default:
					var err error
					data, err = json.Marshal(v)
					if err != nil {
						return err
					}
				}

				select {
				case msgChan <- data:
				case <-connCtx.Done():
				}
				return nil
			},
		},
	}

	subObj, err := pipeline.GmqSubscribe(connCtx, subMsg)
	if err != nil {
		return err
	}

	mqClientsMux.Lock()
	clientInfo.subscribed = true
	clientInfo.subObj = subObj
	mqClientsMux.Unlock()

	return nil
}

// handleClientPublish 处理客户端发布的消息
func handleClientPublish(conn *websocket.Conn, clientInfo *mqClientInfo, connCtx context.Context, readTimeout time.Duration) {
	for {
		select {
		case <-connCtx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(readTimeout))
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[MQ] Read message error: %v", err)
			return
		}

		// 忽略 Ping/Pong 消息
		if messageType == websocket.PingMessage || messageType == websocket.PongMessage {
			continue
		}

		// 解析客户端消息
		var msg PublishMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[MQ] Failed to unmarshal message: %v", err)
			conn.WriteJSON(map[string]interface{}{
				"type":  "error",
				"error": "Invalid message format",
			})
			continue
		}

		// 发布消息到 MQ
		if err := publishToMQ(clientInfo, msg.Data, connCtx); err != nil {
			log.Printf("[MQ] Failed to publish message: %v", err)
			conn.WriteJSON(map[string]interface{}{
				"type":  "error",
				"error": err.Error(),
			})
			continue
		}
	}
}

// publishToMQ 发布消息到 MQ
func publishToMQ(clientInfo *mqClientInfo, data string, connCtx context.Context) error {
	pipeline := core.GetGmq(clientInfo.serverName)
	if pipeline == nil {
		return fmt.Errorf("[%s] pipeline not found", clientInfo.serverName)
	}

	return pipeline.GmqPublish(connCtx, &components.NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: clientInfo.queueName,
			Data:      data,
		},
	})
}

// forwardMessages 转发订阅的消息到客户端
func forwardMessages(conn *websocket.Conn, msgChan chan []byte, cancelSub <-chan struct{}, connCtx context.Context, queueName string) {
	for {
		select {
		case msg := <-msgChan:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("[MQ] Failed to forward message: %v", err)
				return
			}
		case <-cancelSub:
			log.Printf("[MQ] Message forwarder stopped")
			return
		case <-connCtx.Done():
			log.Printf("[MQ] Message forwarder stopped due to connection close")
			return
		}
	}
}

// cleanupClient 清理客户端连接
func cleanupClient(conn *websocket.Conn, clientInfo *mqClientInfo, connCancel context.CancelFunc) {
	log.Printf("[MQ] Cleaning up client %s", clientInfo.consumerName)

	// 取消订阅
	if clientInfo.subscribed && clientInfo.subObj != nil {
		if unsubber, ok := clientInfo.subObj.(interface{ Unsubscribe() error }); ok {
			_ = unsubber.Unsubscribe()
		}
	}

	// 关闭连接
	conn.Close()

	// 从客户端列表中移除
	mqClientsMux.Lock()
	delete(mqClients, conn)
	mqClientsMux.Unlock()

	log.Printf("[MQ] Client %s disconnected", clientInfo.consumerName)
}

// initMQUpgrader 初始化 WebSocket upgrader
func initMQUpgrader() *websocket.Upgrader {
	mqUpgraderOnce.Do(func() {
		wsCfg := config.GetWebSocketConfig()
		mqUpgrader = &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  wsCfg.ReadBufferSize,
			WriteBufferSize: wsCfg.WriteBufferSize,
		}
	})
	return mqUpgrader
}
