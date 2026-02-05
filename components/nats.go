package components

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/bjang03/gmq/config"
	"github.com/bjang03/gmq/core"
	"github.com/nats-io/nats.go"
)

func init() {
	core.GmqRegister("nats", &natsMsg{})
}

// NatsPubMessage NATS发布消息结构，支持延迟消息
type NatsPubMessage struct {
	core.PubMessage
	DelaySeconds int // 延迟时间(秒)
}

// GetGmqPublishMsgType 标记为NATS发布消息类型
func (n NatsPubMessage) GetGmqPublishMsgType() {}

// NatsSubMessage NATS订阅消息结构，支持持久化订阅和延迟消费
type NatsSubMessage struct {
	core.SubMessage[any]
	Durable      bool   // 是否持久化订阅
	ConsumerName string // 消费者名称
}

// natsMsg NATS消息队列实现
type natsMsg struct {
	conn    *nats.Conn
	connURL string
	mu      sync.RWMutex // 保护conn和connURL
}

// GmqPing 检测NATS连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return false
	}

	return c.conn.IsConnected()
}

// GmqConnect 连接NATS服务器
func (c *natsMsg) GmqConnect(ctx context.Context) (err error) {
	connURL := config.GetNATSURL()
	log.Printf("[NATS] Connecting to %s", connURL)

	// 设置连接选项
	opts := []nats.Option{
		nats.Timeout(10 * time.Second),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[NATS] Disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Reconnected to %s", nc.ConnectedUrl())
		}),
	}

	conn, err := nats.Connect(connURL, opts...)
	if err != nil {
		log.Printf("[NATS] Failed to connect: %v", err)
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.connURL = connURL
	c.mu.Unlock()

	log.Printf("[NATS] Successfully connected to %s", connURL)
	return nil
}

// GmqClose 关闭NATS连接
func (c *natsMsg) GmqClose(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		log.Printf("[NATS] Closing connection to %s", c.connURL)
		c.conn.Close()
		c.conn = nil
	}
	return nil
}

// GmqPublish 发布NATS消息
func (c *natsMsg) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("nats connection not established")
	}

	natsMsg, ok := msg.(*NatsPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}

	log.Printf("[NATS] Publishing to topic: %s", natsMsg.QueueName)

	// 使用JSON序列化消息数据
	var data []byte
	if natsMsg.Data == nil {
		data = []byte("null")
	} else {
		switch v := natsMsg.Data.(type) {
		case []byte:
			data = v
		case string:
			data = []byte(v)
		default:
			data, err = json.Marshal(natsMsg.Data)
			if err != nil {
				return fmt.Errorf("failed to marshal message data: %w", err)
			}
		}
	}

	if err = conn.Publish(natsMsg.QueueName, data); err != nil {
		log.Printf("[NATS] Failed to publish: %v", err)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("[NATS] Successfully published to topic: %s", natsMsg.QueueName)
	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (err error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("nats connection not established")
	}

	natsMsg, ok := msg.(*NatsSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}

	log.Printf("[NATS] Subscribing to topic: %s", natsMsg.QueueName)

	var sub *nats.Subscription
	if natsMsg.Durable && natsMsg.ConsumerName != "" {
		sub, err = conn.QueueSubscribe(natsMsg.QueueName, natsMsg.ConsumerName, func(m *nats.Msg) {
			c.handleMessage(ctx, natsMsg, m)
		})
	} else {
		sub, err = conn.Subscribe(natsMsg.QueueName, func(m *nats.Msg) {
			c.handleMessage(ctx, natsMsg, m)
		})
	}

	if err != nil {
		log.Printf("[NATS] Failed to subscribe: %v", err)
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// 检查订阅是否有效
	if !sub.IsValid() {
		return fmt.Errorf("subscription is not valid")
	}

	log.Printf("[NATS] Successfully subscribed to topic: %s", natsMsg.QueueName)
	return nil
}

// 消息处理超时
const messageHandleTimeout = 30 * time.Second

// handleMessage 处理消息
func (c *natsMsg) handleMessage(ctx context.Context, natsMsg *NatsSubMessage, m *nats.Msg) {
	if natsMsg.HandleFunc == nil {
		// 如果没有处理函数，直接确认消息避免阻塞
		_ = m.Ack()
		return
	}

	// 创建独立的上下文，避免使用可能已过期的外部上下文
	msgCtx, cancel := context.WithTimeout(context.Background(), messageHandleTimeout)
	defer cancel()

	if err := natsMsg.HandleFunc(msgCtx, m.Data); err != nil {
		// 处理失败，如果不是AutoAck模式，不确认消息让服务器重发
		if !natsMsg.AutoAck {
			log.Printf("[NATS] Message handler error: %v", err)
			return
		}
	}

	// 处理成功或AutoAck模式下，手动确认消息
	if err := m.Ack(); err != nil {
		log.Printf("[NATS] Failed to ack message: %v", err)
	}
}

// GetMetrics 获取基础监控指标
func (c *natsMsg) GetMetrics(ctx context.Context) *core.Metrics {
	c.mu.RLock()
	conn := c.conn
	connURL := c.connURL
	c.mu.RUnlock()

	m := &core.Metrics{
		Type:       "nats",
		ServerAddr: connURL,
	}

	// 检查连接状态，避免在 GetMetrics 中调用 GmqPing 防止重复计算延迟
	if conn != nil && conn.IsConnected() {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
		return m
	}

	// 从 NATS 连接获取服务端统计信息
	stats := conn.Stats()
	// NATS 提供的统计信息 - 直接使用 uint64 避免溢出
	m.MsgsIn = int64(stats.InMsgs)
	m.MsgsOut = int64(stats.OutMsgs)
	m.BytesIn = stats.InBytes
	m.BytesOut = stats.OutBytes
	m.ReconnectCount = int64(conn.Reconnects)

	// 服务端详细信息
	m.ServerMetrics = map[string]interface{}{
		"serverId":          conn.ConnectedServerId(),
		"serverVersion":     conn.ConnectedServerVersion(),
		"totalConnections":  0, // NATS 客户端库不提供这个信息
		"activeConnections": 1, // 当前客户端连接
	}

	return m
}
