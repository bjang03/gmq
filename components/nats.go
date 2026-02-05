package components

import (
	"context"
	"fmt"
	"time"

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

func (n NatsPubMessage) GetGmqPublishMsgType() {
	panic("implement me")
}

// NatsSubMessage NATS订阅消息结构，支持持久化订阅和延迟消费
type NatsSubMessage struct {
	core.SubMessage[any]
	Durable      bool   // 是否持久化订阅
	ConsumerName string // 消费者名称
}

// natsMsg NATS消息队列实现
type natsMsg struct {
	conn            *nats.Conn
	connURL         string
	connectedAt     time.Time
	lastPingLatency float64
}

// GmqPing 检测NATS连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	if c.conn == nil {
		return false
	}

	if !c.conn.IsConnected() {
		return false
	}

	start := time.Now()
	_ = c.conn.Flush()
	c.lastPingLatency = float64(time.Since(start).Milliseconds())

	return true
}

// GmqConnect 连接NATS服务器
func (c *natsMsg) GmqConnect(ctx context.Context) (err error) {
	c.connURL = "nats://localhost:4222"

	c.conn, err = nats.Connect(c.connURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.connectedAt = time.Now()
	return nil
}

// GmqClose 关闭NATS连接
func (c *natsMsg) GmqClose(ctx context.Context) (err error) {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	return nil
}

// GmqPublish 发布NATS消息
func (c *natsMsg) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	if c.conn == nil {
		return fmt.Errorf("nats connection not established")
	}

	natsMsg, ok := msg.(*NatsPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}

	err = c.conn.Publish(natsMsg.QueueName, []byte(fmt.Sprintf("%v", natsMsg.Data)))
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (err error) {
	if c.conn == nil {
		return fmt.Errorf("nats connection not established")
	}

	natsMsg, ok := msg.(*NatsSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}

	if natsMsg.Durable && natsMsg.ConsumerName != "" {
		_, err = c.conn.QueueSubscribe(natsMsg.QueueName, natsMsg.ConsumerName, func(m *nats.Msg) {
			if natsMsg.HandleFunc != nil {
				_ = natsMsg.HandleFunc(ctx, m.Data)
			}
		})
	} else {
		_, err = c.conn.Subscribe(natsMsg.QueueName, func(m *nats.Msg) {
			if natsMsg.HandleFunc != nil {
				_ = natsMsg.HandleFunc(ctx, m.Data)
			}
		})
	}

	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	return nil
}

// GetMetrics 获取基础监控指标
func (c *natsMsg) GetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:            "nats",
		Type:            "nats",
		ServerAddr:      c.connURL,
		ConnectedAt:     c.connectedAt.Format("2006-01-02 15:04:05"),
		LastPingLatency: c.lastPingLatency,
	}

	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	// 计算运行时间
	if !c.connectedAt.IsZero() {
		m.UptimeSeconds = int64(time.Since(c.connectedAt).Seconds())
	}

	// 从 NATS 连接获取服务端统计信息
	if c.conn != nil && c.conn.IsConnected() {
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
	}

	return m
}
