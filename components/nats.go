package components

import (
	"context"
	"fmt"
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

// GmqPing 检测NATS连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	return c.conn.IsConnected()
}

// GmqConnect 连接NATS服务器
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

// GmqClose 关闭NATS连接
func (c *natsMsg) GmqClose(_ context.Context) error {
	c.conn.Close()
	c.conn = nil
	return nil
}

// GmqPublish 发布NATS消息
func (c *natsMsg) GmqPublish(_ context.Context, msg core.Publish) error {
	natsMsg := msg.(*NatsPubMessage)
	// 数据已由管道层序列化为 []byte
	data, _ := natsMsg.Data.([]byte)
	return c.conn.Publish(natsMsg.QueueName, data)
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (interface{}, error) {
	natsMsg := msg.(*NatsSubMessage)

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

	return sub, err
}

// handleMessage 处理消息
func (c *natsMsg) handleMessage(ctx context.Context, natsMsg *NatsSubMessage, m *nats.Msg) {
	if natsMsg.HandleFunc == nil {
		_ = m.Ack()
		return
	}

	natsCfg := config.GetNATSConfig()
	msgCtx, cancel := context.WithTimeout(context.TODO(), time.Duration(natsCfg.MessageTimeout)*time.Second)
	defer cancel()

	if err := natsMsg.HandleFunc(msgCtx, m.Data); err != nil {
		if !natsMsg.AutoAck {
			return
		}
	}
	_ = m.Ack()
}

// GetMetrics 获取基础监控指标
func (c *natsMsg) GetMetrics(_ context.Context) *core.Metrics {
	m := &core.Metrics{
		Type:       "nats",
		ServerAddr: c.connURL,
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
