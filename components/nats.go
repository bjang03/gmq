package components

import (
	"context"
	"encoding/json"
	"fmt"
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
}

// GmqPing 检测NATS连接状态（并发控制由管道层统一管理）
func (c *natsMsg) GmqPing(_ context.Context) bool {
	if c.conn == nil {
		return false
	}
	return c.conn.IsConnected()
}

// GmqConnect 连接NATS服务器（并发控制由管道层统一管理，日志由管道层统一输出）
func (c *natsMsg) GmqConnect(_ context.Context) error {
	connURL := config.GetNATSURL()
	natsCfg := config.GetNATSConfig()

	// 设置连接选项
	opts := []nats.Option{
		nats.Timeout(time.Duration(natsCfg.Timeout) * time.Second),
		nats.ReconnectWait(time.Duration(natsCfg.ReconnectWait) * time.Second),
		nats.MaxReconnects(natsCfg.MaxReconnects),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			// 连接断开事件，由管道层统一处理日志
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			// 重连成功事件，由管道层统一处理日志
		}),
	}

	conn, err := nats.Connect(connURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.conn = conn
	c.connURL = connURL
	return nil
}

// GmqClose 关闭NATS连接（并发控制由管道层统一管理，日志由管道层统一输出）
func (c *natsMsg) GmqClose(_ context.Context) error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	return nil
}

// GmqPublish 发布NATS消息（并发控制由管道层统一管理，日志由管道层统一输出）
func (c *natsMsg) GmqPublish(_ context.Context, msg core.Publish) error {
	if c.conn == nil {
		return fmt.Errorf("nats connection not established")
	}

	natsMsg, ok := msg.(*NatsPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}

	// 使用JSON序列化消息数据
	var data []byte
	var err error
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

	if err = c.conn.Publish(natsMsg.QueueName, data); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// GmqSubscribe 订阅NATS消息（并发控制由管道层统一管理，日志由管道层统一输出）
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (interface{}, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("nats connection not established")
	}

	natsMsg, ok := msg.(*NatsSubMessage)
	if !ok {
		return nil, fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}

	var sub *nats.Subscription
	var err error
	if natsMsg.Durable && natsMsg.ConsumerName != "" {
		sub, err = c.conn.QueueSubscribe(natsMsg.QueueName, natsMsg.ConsumerName, func(m *nats.Msg) {
			c.handleMessage(ctx, natsMsg, m)
		})
	} else {
		sub, err = c.conn.Subscribe(natsMsg.QueueName, func(m *nats.Msg) {
			c.handleMessage(ctx, natsMsg, m)
		})
	}

	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	// 检查订阅是否有效
	if !sub.IsValid() {
		return nil, fmt.Errorf("subscription is not valid")
	}

	return sub, nil
}

// GmqUnsubscribe 取消NATS订阅（并发控制由管道层统一管理，日志由管道层统一输出）
func (c *natsMsg) GmqUnsubscribe(_ context.Context, topic, consumerName string, subObj interface{}) error {
	if c.conn == nil {
		return fmt.Errorf("nats connection not established")
	}

	// 从管道层获取订阅对象
	sub, ok := subObj.(*nats.Subscription)
	if !ok {
		return fmt.Errorf("invalid subscription object type for topic: %s", topic)
	}

	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	return nil
}

// handleMessage 处理消息（日志由管道层统一输出）
func (c *natsMsg) handleMessage(ctx context.Context, natsMsg *NatsSubMessage, m *nats.Msg) {
	if natsMsg.HandleFunc == nil {
		_ = m.Ack()
		return
	}

	// 创建独立的上下文，避免使用可能已过期的外部上下文
	natsCfg := config.GetNATSConfig()
	msgCtx, cancel := context.WithTimeout(context.Background(), time.Duration(natsCfg.MessageTimeout)*time.Second)
	defer cancel()

	if err := natsMsg.HandleFunc(msgCtx, m.Data); err != nil {
		// 处理失败，如果不是AutoAck模式，不确认消息让服务器重发
		if !natsMsg.AutoAck {
			return
		}
	}

	// 处理成功或AutoAck模式下，手动确认消息
	_ = m.Ack()
}

// GetMetrics 获取基础监控指标（并发控制由管道层统一管理）
func (c *natsMsg) GetMetrics(_ context.Context) *core.Metrics {
	m := &core.Metrics{
		Type:       "nats",
		ServerAddr: c.connURL,
	}

	// 检查连接状态
	if c.conn != nil && c.conn.IsConnected() {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
		return m
	}

	// 从 NATS 连接获取服务端统计信息
	stats := c.conn.Stats()
	// NATS 提供的统计信息 - 直接使用 uint64 避免溢出
	m.MsgsIn = int64(stats.InMsgs)
	m.MsgsOut = int64(stats.OutMsgs)
	m.BytesIn = stats.InBytes
	m.BytesOut = stats.OutBytes
	m.ReconnectCount = int64(c.conn.Reconnects)

	// 服务端详细信息
	m.ServerMetrics = map[string]interface{}{
		"serverId":          c.conn.ConnectedServerId(),
		"serverVersion":     c.conn.ConnectedServerVersion(),
		"totalConnections":  0, // NATS 客户端库不提供这个信息
		"activeConnections": 1, // 当前客户端连接
	}

	return m
}
