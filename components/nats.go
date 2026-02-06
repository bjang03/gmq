package components

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bjang03/gmq/config"
	"github.com/bjang03/gmq/core"
	"github.com/nats-io/nats.go"
)

func init() {
	_ = core.GmqRegister("nats", &natsMsg{})
}

// NatsPubMessage NATS发布消息结构，支持延迟消息
type NatsPubMessage struct {
	core.PubMessage
	DelaySeconds int // 延迟时间(秒)
}

// GetGmqPublishMsgType 标记为NATS发布消息类型
func (n NatsPubMessage) GetGmqPublishMsgType() {}

// NatsSubMessage NATS订阅消息结构，支持持久化订阅和延迟消费
// 问题16修复：移除重复的 ConsumerName 字段，使用 SubMessage 中的字段
type NatsSubMessage struct {
	core.SubMessage[any]
	Durable bool // 是否持久化订阅（注：需要 NATS JetStream 支持）
}

// natsMsg NATS消息队列实现
type natsMsg struct {
	Conn    *nats.Conn
	connURL string
}

// GmqPing 检测NATS连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	return c.Conn != nil && c.Conn.IsConnected()
}

// GmqConnect 连接NATS服务器
func (c *natsMsg) GmqConnect(_ context.Context) error {
	connURL := config.GetNATSURL()
	natsCfg := config.GetNATSConfig()

	// 设置连接选项（问题4修复：添加连接事件日志）
	opts := []nats.Option{
		nats.Timeout(time.Duration(natsCfg.Timeout) * time.Second),
		nats.ReconnectWait(time.Duration(natsCfg.ReconnectWait) * time.Second),
		nats.MaxReconnects(natsCfg.MaxReconnects),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[NATS] Connection disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Connection reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ConnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Connection established to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Connection closed")
		}),
	}

	conn, err := nats.Connect(connURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.Conn = conn
	c.connURL = connURL
	return nil
}

// GmqClose 关闭NATS连接
func (c *natsMsg) GmqClose(_ context.Context) error {
	if c.Conn != nil {
		return nil
	}
	c.Conn.Close()
	return nil
}

// GmqPublish 发布NATS消息
func (c *natsMsg) GmqPublish(_ context.Context, msg core.Publish) error {
	natsMsg, ok := msg.(*NatsPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type: expected *NatsPubMessage")
	}

	// 自动转换数据为 []byte
	var data []byte
	switch v := natsMsg.Data.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		// 其他类型使用 JSON 序列化
		var err error
		data, err = json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal message data: %w", err)
		}
	}

	return c.Conn.Publish(natsMsg.QueueName, data)
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (interface{}, error) {
	// 问题4修复：检查连接状态
	if c.Conn == nil || !c.Conn.IsConnected() {
		return nil, fmt.Errorf("nats not connected")
	}

	natsMsg, ok := msg.(*NatsSubMessage)
	if !ok {
		return nil, fmt.Errorf("invalid message type: expected *NatsSubMessage")
	}

	var sub *nats.Subscription
	var err error
	// 问题16修复：使用 SubMessage 中的 ConsumerName 字段
	if natsMsg.Durable && natsMsg.SubMessage.ConsumerName != "" {
		sub, err = c.Conn.QueueSubscribe(natsMsg.QueueName, natsMsg.SubMessage.ConsumerName, func(m *nats.Msg) {
			c.handleMessage(ctx, natsMsg, m)
		})
	} else {
		sub, err = c.Conn.Subscribe(natsMsg.QueueName, func(m *nats.Msg) {
			c.handleMessage(ctx, natsMsg, m)
		})
	}

	return sub, err
}

// handleMessage 处理消息
func (c *natsMsg) handleMessage(ctx context.Context, natsMsg *NatsSubMessage, m *nats.Msg) {
	if natsMsg.HandleFunc == nil {
		_ = m.Ack()
		return
	}

	natsCfg := config.GetNATSConfig()
	// 使用传入的 ctx 创建带超时的子 context
	msgCtx, cancel := context.WithTimeout(ctx, time.Duration(natsCfg.MessageTimeout)*time.Second)
	defer cancel()

	err := natsMsg.HandleFunc(msgCtx, m.Data)
	if err != nil {
		// 处理失败时，根据 AutoAck 决定是否 ACK
		if !natsMsg.AutoAck {
			// 不自动确认，消息会重新投递
			return
		}
		// AutoAck=true 且处理失败，也 ACK（避免无限重试）
	}
	_ = m.Ack()
}

// GetMetrics 获取基础监控指标
func (c *natsMsg) GetMetrics(_ context.Context) *core.Metrics {
	m := &core.Metrics{
		Type:       "nats",
		ServerAddr: c.connURL,
	}

	// P1修复：检查连接是否为 nil
	if c.Conn == nil {
		m.Status = "disconnected"
		return m
	}

	// 从 NATS 连接获取服务端统计信息
	stats := c.Conn.Stats()
	// NATS 提供的统计信息
	m.MsgsIn = int64(stats.InMsgs)
	m.MsgsOut = int64(stats.OutMsgs)
	m.BytesIn = int64(stats.InBytes)
	m.BytesOut = int64(stats.OutBytes)
	m.ReconnectCount = int64(c.Conn.Reconnects)

	// 问题15修复：只提供客户端可获取的真实指标，移除硬编码的虚假数据
	m.ServerMetrics = map[string]interface{}{
		"serverId":      c.Conn.ConnectedServerId(),
		"serverVersion": c.Conn.ConnectedServerVersion(),
		// 注意：totalConnections 和 activeConnections 是服务端视角的指标
		// NATS 客户端库不提供这些信息，不返回虚假数据
	}

	return m
}
