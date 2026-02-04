package gmq

import (
	"context"

	"github.com/nats-io/nats.go"
)

func init() {
	GmqRegister("nats", &natsMsg{})
}

type NatsPubMessage struct {
	PubMessage
	DelayTime int
}
type NatsSubMessage struct {
	SubMessage
	Durable      bool
	DelayTime    int
	ConsumerName string
}

type natsMsg struct {
	conn *nats.Conn
}

// GmqPing 检测连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	if c.conn == nil {
		return false
	}
	return c.conn.IsConnected()
}

// GmqConnect 连接
func (c *natsMsg) GmqConnect(ctx context.Context) (err error) {

	return
}

// GmqClose 关闭连接
func (c *natsMsg) GmqClose(ctx context.Context) (err error) {
	return
}

// GmqPublish 发布消息
func (c *natsMsg) GmqPublish(ctx context.Context, msg Publish) (err error) {
	return
}

// GmqSubscribe 订阅消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg Subscribe) (err error) {
	return
}
