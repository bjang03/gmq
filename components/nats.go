package components

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	metrics         natsMetrics
}

// natsMetrics NATS监控指标
type natsMetrics struct {
	publishCount    int64
	subscribeCount  int64
	publishFailed   int64
	subscribeFailed int64
	messageCount    int64
	totalLatency    int64
	latencyCount    int64
	mu              sync.RWMutex
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
		atomic.AddInt64(&c.metrics.publishFailed, 1)
		return fmt.Errorf("nats connection not established")
	}

	start := time.Now()
	defer func() {
		latency := int64(time.Since(start).Milliseconds())
		atomic.AddInt64(&c.metrics.totalLatency, latency)
		atomic.AddInt64(&c.metrics.latencyCount, 1)
	}()

	natsMsg, ok := msg.(*NatsPubMessage)
	if !ok {
		atomic.AddInt64(&c.metrics.publishFailed, 1)
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}

	err = c.conn.Publish(natsMsg.QueueName, []byte(fmt.Sprintf("%v", natsMsg.Data)))
	if err != nil {
		atomic.AddInt64(&c.metrics.publishFailed, 1)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	atomic.AddInt64(&c.metrics.publishCount, 1)
	atomic.AddInt64(&c.metrics.messageCount, 1)
	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (err error) {
	if c.conn == nil {
		atomic.AddInt64(&c.metrics.subscribeFailed, 1)
		return fmt.Errorf("nats connection not established")
	}

	start := time.Now()
	defer func() {
		latency := int64(time.Since(start).Milliseconds())
		atomic.AddInt64(&c.metrics.totalLatency, latency)
		atomic.AddInt64(&c.metrics.latencyCount, 1)
	}()

	natsMsg, ok := msg.(*NatsSubMessage)
	if !ok {
		atomic.AddInt64(&c.metrics.subscribeFailed, 1)
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
		atomic.AddInt64(&c.metrics.subscribeFailed, 1)
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	atomic.AddInt64(&c.metrics.subscribeCount, 1)
	return nil
}

// GetMetrics 获取监控指标
func (c *natsMsg) GetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:            "nats",
		ConnectedAt:     c.connectedAt.Format("2006-01-02 15:04:05"),
		LastPingLatency: c.lastPingLatency,
	}

	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	latencyCount := atomic.LoadInt64(&c.metrics.latencyCount)
	totalLatency := atomic.LoadInt64(&c.metrics.totalLatency)
	if latencyCount > 0 {
		m.AverageLatency = float64(totalLatency) / float64(latencyCount)
	}

	m.PublishCount = atomic.LoadInt64(&c.metrics.publishCount)
	m.SubscribeCount = atomic.LoadInt64(&c.metrics.subscribeCount)
	m.PublishFailed = atomic.LoadInt64(&c.metrics.publishFailed)
	m.SubscribeFailed = atomic.LoadInt64(&c.metrics.subscribeFailed)
	m.MessageCount = atomic.LoadInt64(&c.metrics.messageCount)

	// 计算吞吐量（简单实现，基于运行时间）
	if !c.connectedAt.IsZero() {
		duration := time.Since(c.connectedAt).Seconds()
		if duration > 0 {
			m.ThroughputPerSec = float64(m.MessageCount) / duration
		}
	}

	return m
}
