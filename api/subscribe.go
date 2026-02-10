package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cast"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/utils"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	// HTTP 订阅者管理 (queueName -> key -> subscription)
	httpSubscribers = make(map[string]map[string]interface{})
	httpSubsMux     sync.RWMutex

	// WebSocket订阅管理器
	subscribeWSManager = utils.NewWebSocketManager()

	httpClient = &http.Client{Timeout: 30 * time.Second}
)

// SubscribeReq 订阅请求
type SubscribeReq struct {
	ServerName   string `json:"serverName" validate:"required"` // 回调服务名
	MqName       string `json:"mqName" validate:"required"`     // 消息队列名
	QueueName    string `json:"queueName" validate:"required"`
	ConsumerName string `json:"consumerName" validate:"required"`
	AutoAck      bool   `json:"autoAck"`
	FetchCount   int    `json:"fetchCount"`
	Durable      bool   `json:"durable"`
	IsDelayMsg   bool   `json:"isDelayMsg"`
	WebHook      string `json:"webHook"` // 回调路径，与ServerName拼接构成完整回调地址
}

// Subscribe 订阅消息（HTTP接口）
// 创建 MQ 订阅，收到消息后通过 WebHook 回调通知
func Subscribe(ctx context.Context, req *SubscribeReq) (res interface{}, err error) {
	//key := req.ServerName + ":" + req.WebHook
	callbackURL := req.ServerName + req.WebHook

	err = createMQSubscription(ctx, req.MqName, req.QueueName, req.ConsumerName, req.AutoAck, req.FetchCount, req.Durable, req.IsDelayMsg, func(ctx context.Context, data []byte) error {
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
	//httpSubscribers[req.QueueName][key] = subObj
	httpSubsMux.Unlock()

	log.Printf("[HTTP-Subscribe] Success - MqName: %s, Queue: %s, WebHook: %s",
		req.MqName, req.QueueName, req.WebHook)

	return
}

// WSSubscribeHandler WebSocket订阅处理器
func WSSubscribeHandler(c *gin.Context) {
	mqName := c.Query("mqName")
	queueName := c.Query("queueName")
	consumerName := c.Query("consumerName")
	autoAck := cast.ToBool(c.Query("autoAck"))
	fetchCount := cast.ToInt(c.Query("fetchCount"))
	durable := cast.ToBool(c.Query("durable"))
	isDelayMsg := cast.ToBool(c.Query("isDelayMsg"))

	if mqName == "" || queueName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "mqName and queueName are required"})
		return
	}

	// 创建消息通道
	msgChan := make(chan []byte, 100)

	// 创建MQ订阅
	err := createMQSubscription(context.Background(), mqName, queueName, consumerName, autoAck, fetchCount, durable, isDelayMsg, func(ctx context.Context, data []byte) error {
		select {
		case msgChan <- data:
		case <-ctx.Done():
		}
		return nil
	})
	if err != nil {
		return
	}

	log.Printf("[WS-Subscribe] Success - MqName: %s, Queue: %s", mqName, queueName)

	// 使用WebSocketManager统一管理连接(心跳、停止监听、生命周期)
	handler := func(conn *websocket.Conn, messageType int, data []byte) error {
		// 处理Ping/Pong消息
		if messageType == websocket.PingMessage {
			return utils.SafeCloseWrite(conn, websocket.PongMessage, nil)
		}
		// 忽略其他控制消息
		return nil
	}

	conn, err := subscribeWSManager.HandleConnection(c.Writer, c.Request, handler, nil)
	if err != nil {
		log.Printf("[WS-Subscribe] WebSocket connection failed: %v", err)
		return
	}

	// 订阅消息转发
	go func() {
		//defer func() {
		//	if unsubber, ok := subObj.(interface{ Unsubscribe() error }); ok {
		//		_ = unsubber.Unsubscribe()
		//	}
		//}()
		for {
			select {
			case msg := <-msgChan:
				if err := utils.WriteTextMessage(conn, msg); err != nil {
					log.Printf("[WS-Subscribe] Failed to write message: %v", err)
					return
				}
			case <-subscribeWSManager.BroadcastStopCh():
				return
			}
		}
	}()
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
func createMQSubscription(ctx context.Context, mqName, queueName, consumerName string, autoAck bool, fetchCount int, durable bool, delaySeconds bool, handler func(context.Context, []byte) error) error {
	pipeline := core.GetGmq(mqName)
	if pipeline == nil {
		return fmt.Errorf("[%s] pipeline not found", mqName)
	}

	baseMsg := core.SubMessage[any]{
		QueueName:    queueName,
		ConsumerName: consumerName,
		AutoAck:      autoAck,
		FetchCount:   fetchCount,
		HandleFunc: func(ctx context.Context, message any) error {
			data, err := marshalMessage(message)
			if err != nil {
				return err
			}
			return handler(ctx, data)
		},
	}

	switch mqName {
	case "nats":
		return pipeline.GmqSubscribe(ctx, &mq.NatsSubMessage{SubMessage: baseMsg, Durable: durable, IsDelayMsg: delaySeconds})
	case "rabbitmq":
		return pipeline.GmqSubscribe(ctx, &mq.RabbitMQSubMessage{SubMessage: baseMsg})
	case "redis":
		return pipeline.GmqSubscribe(ctx, &mq.RedisSubMessage{SubMessage: baseMsg})
	default:
		return fmt.Errorf("unsupported mq type: %s", mqName)
	}
}

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
