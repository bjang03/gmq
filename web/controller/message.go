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

// marshalMessage 将消息转换为 []byte
func marshalMessage(message any) ([]byte, error) {
	switch v := message.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

// createMQSubscription 创建 MQ 订阅
func createMQSubscription(ctx context.Context, mqName, queueName, subType string, handler func(context.Context, []byte) error) (interface{}, error) {
	pipeline := core.GetGmq(mqName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", mqName)
	}

	subMsg := &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName:    queueName,
			ConsumerName: fmt.Sprintf("%s-sub-%s-%d", subType, queueName, time.Now().UnixNano()),
			HandleFunc: func(ctx context.Context, message any) error {
				data, err := marshalMessage(message)
				if err != nil {
					return err
				}
				return handler(ctx, data)
			},
		},
	}

	return pipeline.GmqSubscribe(ctx, subMsg)
}

// Subscribe 订阅消息（HTTP接口）
// 创建 MQ 订阅，收到消息后通过 WebHook 回调通知
func Subscribe(ctx context.Context, req *dto.SubscribeReq) (res interface{}, err error) {
	key := req.ServerName + ":" + req.WebHook
	callbackURL := req.ServerName + req.WebHook

	subObj, err := createMQSubscription(ctx, req.MqName, req.QueueName, "http", func(ctx context.Context, data []byte) error {
		return sendHttpCallback(ctx, callbackURL, data)
	})
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

	return
}

// WSSubscribeHandler WebSocket订阅处理器
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

	subObj, err := createMQSubscription(connCtx, mqName, queueName, "ws", func(ctx context.Context, data []byte) error {
		select {
		case msgChan <- data:
		case <-cancelSub:
		case <-connCtx.Done():
		}
		return nil
	})
	if err != nil {
		conn.Close()
		return
	}

	log.Printf("[WS-Subscribe] Success - MqName: %s, Queue: %s", mqName, queueName)

	go forwardWSMessages(conn, msgChan, cancelSub, connCtx)

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
func forwardWSMessages(conn *websocket.Conn, msgChan chan []byte, cancelSub chan struct{}, connCtx context.Context) {
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

var httpClient = &http.Client{Timeout: 30 * time.Second}

// sendHttpCallback 发送 HTTP 回调
func sendHttpCallback(ctx context.Context, callbackURL string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", callbackURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("[HTTP-Callback] Failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
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
	return nil
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
