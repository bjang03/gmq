// Package gmq NATS消息中间件实现
package gmq

import (
	"context"

	"github.com/nats-io/nats.go"
)

func init() {
	GmqRegister("nats", &natsMsg{})
}

// NatsPubMessage NATS发布消息结构，支持延迟消息
type NatsPubMessage struct {
	PubMessage
	DelayTime int // 延迟时间(秒)
}

// NatsSubMessage NATS订阅消息结构，支持持久化订阅和延迟消费
type NatsSubMessage struct {
	SubMessage
	Durable      bool   // 是否持久化订阅
	DelayTime    int    // 延迟时间(秒)
	ConsumerName string // 消费者名称
}

// natsMsg NATS消息队列实现
type natsMsg struct {
	conn *nats.Conn
}

// GmqPing 检测NATS连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	if c.conn == nil {
		return false
	}
	return c.conn.IsConnected()
}

// GmqConnect 连接NATS服务器
func (c *natsMsg) GmqConnect(ctx context.Context) (err error) {

	return
}

// GmqClose 关闭NATS连接
func (c *natsMsg) GmqClose(ctx context.Context) (err error) {
	return
}

// GmqPublish 发布NATS消息
func (c *natsMsg) GmqPublish(ctx context.Context, msg Publish) (err error) {
	return
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg Subscribe) (err error) {
	return
}
