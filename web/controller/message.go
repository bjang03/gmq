package controller

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/config"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/web/dto"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	subscribeClients      = make(map[*websocket.Conn]*subscribeClientInfo)
	subscribeUpgrader     *websocket.Upgrader
	subscribeUpgraderOnce sync.Once
)

type subscribeClientInfo struct {
	serverName string
	queueName  string
	subObj     interface{}
}

// Publish 发布消息
func Publish(ctx context.Context, req *dto.PublishReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.ServerName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.ServerName)
	}
	err = pipeline.GmqPublish(ctx, &components.NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: req.QueueName,
			Data:      req.Message,
		},
	})
	return
}

// WSSubscribeHandler WebSocket订阅处理器
func WSSubscribeHandler(c *gin.Context) {
	// 获取请求参数
	serverName := c.Query("serverName")
	queueName := c.Query("queueName")

	if serverName == "" || queueName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "serverName and queueName are required"})
		return
	}

	// 升级为 WebSocket 连接
	upgrader := initSubscribeUpgrader()
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[Subscribe] WebSocket upgrade failed: %v", err)
		return
	}

	wsCfg := config.GetWebSocketConfig()
	pingInterval := time.Duration(wsCfg.PingInterval) * time.Second

	// 创建连接上下文
	connCtx, connCancel := context.WithCancel(c.Request.Context())
	defer connCancel()

	// 创建消费者名称
	consumerName := fmt.Sprintf("ws-client-%d", time.Now().UnixNano())

	// 初始化管道
	pipeline := core.GetGmq(serverName)
	if pipeline == nil {
		conn.WriteJSON(map[string]string{"error": fmt.Sprintf("[%s] pipeline not found", serverName)})
		conn.Close()
		return
	}

	// 创建订阅通道和取消通道
	cancelSub := make(chan struct{})

	// 注册客户端
	clientInfo := &subscribeClientInfo{
		serverName: serverName,
		queueName:  queueName,
	}
	subscribeClients[conn] = clientInfo

	// 取消订阅函数
	unsubscribe := func() {
		close(cancelSub)
		if clientInfo.subObj != nil {
			if unsubber, ok := clientInfo.subObj.(interface{ Unsubscribe() error }); ok {
				_ = unsubber.Unsubscribe()
			}
		}
	}

	// 启动订阅协程
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Subscribe] panic: %v", r)
			}
		}()

		log.Printf("[Subscribe] Starting subscription for queue: %s, consumer: %s", queueName, consumerName)

		subMsg := &components.NatsSubMessage{
			SubMessage: core.SubMessage[any]{
				QueueName:    queueName,
				ConsumerName: consumerName,
				HandleFunc: func(ctx context.Context, msg any) error {
					if err := conn.WriteMessage(websocket.TextMessage, msg.([]byte)); err != nil {
						log.Printf("[Subscribe] Failed to write message to client: %v", err)
					}
					return nil
				},
			},
		}

		subObj, err := pipeline.GmqSubscribe(connCtx, subMsg)
		if err != nil {
			log.Printf("[Subscribe] failed to subscribe: %v", err)
			conn.WriteJSON(map[string]string{"error": err.Error()})
			return
		}

		clientInfo.subObj = subObj
		log.Printf("[Subscribe] Subscription established successfully, waiting for messages...")

		// 保持订阅直到取消
		<-cancelSub
		log.Printf("[Subscribe] Subscription cancelled")
	}()

	// 启动心跳协程
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					connCancel()
					return
				}
			case <-connCtx.Done():
				return
			}
		}
	}()

	// 等待连接关闭
	<-connCtx.Done()

	// 清理
	unsubscribe()
	conn.Close()
	delete(subscribeClients, conn)

	log.Printf("[Subscribe] client disconnected from %s:%s", serverName, queueName)
}

// initSubscribeUpgrader 初始化订阅 WebSocket upgrader
func initSubscribeUpgrader() *websocket.Upgrader {
	subscribeUpgraderOnce.Do(func() {
		wsCfg := config.GetWebSocketConfig()
		subscribeUpgrader = &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  wsCfg.ReadBufferSize,
			WriteBufferSize: wsCfg.WriteBufferSize,
		}
	})
	return subscribeUpgrader
}
