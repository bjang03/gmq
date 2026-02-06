package controller

import (
	"bytes"
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
	"github.com/bjang03/gmq/web/dto"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	// HTTP 订阅者管理 (queueName -> key -> subscription)
	httpSubscribers = make(map[string]map[string]interface{})
	httpSubsMux     sync.RWMutex

	subscribeUpgrader     *websocket.Upgrader
	subscribeUpgraderOnce sync.Once
)

// Publish 发布消息
func Publish(ctx context.Context, req *dto.PublishReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.MqName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.MqName)
	}

	err = pipeline.GmqPublish(ctx, &components.NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: req.QueueName,
			Data:      req.Message,
		},
	})
	return
}

// Subscribe 订阅消息（HTTP接口）
// 创建 MQ 订阅，收到消息后通过 WebHook 回调通知
func Subscribe(ctx context.Context, req *dto.SubscribeReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.MqName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.MqName)
	}

	key := req.ServerName + ":" + req.WebHook

	// 创建 MQ 订阅
	subMsg := &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName:    req.QueueName,
			ConsumerName: fmt.Sprintf("http-sub-%s-%d", req.QueueName, time.Now().UnixNano()),
			HandleFunc: func(ctx context.Context, message any) error {
				callbackURL := req.ServerName + req.WebHook
				return handleHttpCallback(ctx, callbackURL, message)
			},
		},
	}

	subObj, err := pipeline.GmqSubscribe(ctx, subMsg)
	if err != nil {
		return nil, err
	}

	// 保存订阅
	httpSubsMux.Lock()
	if httpSubscribers[req.QueueName] == nil {
		httpSubscribers[req.QueueName] = make(map[string]interface{})
	}
	httpSubscribers[req.QueueName][key] = subObj
	httpSubsMux.Unlock()

	log.Printf("[HTTP-Subscribe] Success - MqName: %s, Queue: %s, WebHook: %s",
		req.MqName, req.QueueName, req.WebHook)

	return map[string]interface{}{
		"status":  "subscribed",
		"mqName":  req.MqName,
		"queue":   req.QueueName,
		"webHook": req.WebHook,
	}, nil
}

// Unsubscribe 取消订阅（HTTP接口）
func Unsubscribe(c *gin.Context) {
	var req dto.SubscribeReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	httpSubsMux.Lock()
	defer httpSubsMux.Unlock()

	key := req.ServerName + ":" + req.WebHook

	if queueSubs, ok := httpSubscribers[req.QueueName]; ok {
		if subObj, exists := queueSubs[key]; exists {
			// 取消 MQ 订阅
			if unsubber, ok := subObj.(interface{ Unsubscribe() error }); ok {
				_ = unsubber.Unsubscribe()
			}

			delete(queueSubs, key)

			log.Printf("[HTTP-Unsubscribe] Success - Server: %s, Queue: %s, WebHook: %s",
				req.ServerName, req.QueueName, req.WebHook)

			c.JSON(http.StatusOK, gin.H{
				"status": "unsubscribed",
				"server": req.ServerName,
				"queue":  req.QueueName,
			})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "subscription not found"})
}

// WSSubscribeHandler WebSocket订阅处理器
// 仅用于订阅接收消息，不用于发布
func WSSubscribeHandler(c *gin.Context) {
	mqName := c.Query("mqName")
	queueName := c.Query("queueName")

	if mqName == "" || queueName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "mqName and queueName are required"})
		return
	}

	upgrader := initSubscribeUpgrader()
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[WS-Subscribe] WebSocket upgrade failed: %v", err)
		return
	}

	wsCfg := config.GetWebSocketConfig()
	readTimeout := time.Duration(wsCfg.ReadTimeout) * time.Second

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	msgChan := make(chan []byte, 100)
	cancelSub := make(chan struct{})

	// 创建 MQ 订阅
	pipeline := core.GetGmq(mqName)
	if pipeline == nil {
		conn.WriteJSON(map[string]interface{}{"type": "error", "error": fmt.Sprintf("[%s] pipeline not found", mqName)})
		conn.Close()
		return
	}

	subMsg := &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName:    queueName,
			ConsumerName: fmt.Sprintf("ws-sub-%s-%d", queueName, time.Now().UnixNano()),
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
						log.Printf("[WS-Subscribe] Failed to marshal message: %v", err)
						return err
					}
				}
				select {
				case msgChan <- data:
				case <-cancelSub:
				case <-connCtx.Done():
				}
				return nil
			},
		},
	}

	subObj, err := pipeline.GmqSubscribe(connCtx, subMsg)
	if err != nil {
		log.Printf("[WS-Subscribe] Failed to subscribe: %v", err)
		conn.WriteJSON(map[string]interface{}{"type": "error", "error": err.Error()})
		conn.Close()
		return
	}

	if err := conn.WriteJSON(map[string]interface{}{
		"type":   "connected",
		"mqName": mqName,
		"queue":  queueName,
	}); err != nil {
		log.Printf("[WS-Subscribe] Failed to write connected message: %v", err)
		conn.Close()
		return
	}

	log.Printf("[WS-Subscribe] Success - MqName: %s, Queue: %s", mqName, queueName)

	go forwardWSMessages(conn, msgChan, cancelSub, connCtx, queueName)

	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		messageType, _, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[WS-Subscribe] Read error: %v", err)
			break
		}
		if messageType == websocket.PingMessage {
			if err := conn.WriteMessage(websocket.PongMessage, nil); err != nil {
				log.Printf("[WS-Subscribe] Failed to write pong: %v", err)
				break
			}
		}
	}

	close(cancelSub)
	if unsubber, ok := subObj.(interface{ Unsubscribe() error }); ok {
		_ = unsubber.Unsubscribe()
	}
	conn.Close()
}

// forwardWSMessages 转发消息到 WebSocket 客户端
func forwardWSMessages(conn *websocket.Conn, msgChan chan []byte, cancelSub chan struct{}, connCtx context.Context, queueName string) {
	for {
		select {
		case msg := <-msgChan:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("[WS-Subscribe] Failed to write message: %v", err)
				return
			}
		case <-cancelSub:
			return
		case <-connCtx.Done():
			return
		}
	}
}

var httpClient = &http.Client{}

// handleHttpCallback 处理 HTTP 订阅回调
func handleHttpCallback(ctx context.Context, callbackURL string, message any) (err error) {
	var payload []byte
	switch v := message.(type) {
	case []byte:
		payload = v
	case string:
		payload = []byte(v)
	default:
		payload, err = json.Marshal(v)
		if err != nil {
			return fmt.Errorf("[HTTP-Callback] Failed to marshal message: %v", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", callbackURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("[HTTP-Callback] Failed to create request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("[HTTP-Callback] Failed to send callback to %s: %v", callbackURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Printf("[HTTP-Callback] Callback failed with status: %d", resp.StatusCode)
	} else {
		log.Printf("[HTTP-Callback] Callback sent successfully to %s", callbackURL)
	}
	return
}

// initSubscribeUpgrader 初始化 WebSocket 订阅 upgrader
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
